// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import org.apache.hadoop.fs.Path

import java.nio.channels.{FileChannel, FileLock}
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.concurrent.locks.ReentrantLock
import scala.annotation.tailrec
import scala.collection.mutable

/**
  * Local Delta LogStores serialize rename-based commits within one JVM only. The kernel lock
  * serializes this feature's control transactions across local drivers, including table creation.
  * It is never an ownership lease: the durable owner survives both lock release and driver death.
  */
private[ai] object AIPromptLocalTransaction {
  val LockFile: String = ".aiprompt-control.lock"
  private val LockPollMillis = 25L

  private case class LocalLock(lock: ReentrantLock, var users: Int)
  private val Locks = mutable.Map.empty[java.nio.file.Path, LocalLock]

  @tailrec
  private def acquire(channel: FileChannel): FileLock = {
    // Blocking kernel acquisition can also block Thread.interrupt on Windows.
    val lock = channel.tryLock()
    if (lock != null) lock else {
      Thread.sleep(LockPollMillis)
      acquire(channel)
    }
  }

  def apply[T](path: String)(transaction: => T): T = try {
    val uri = new Path(path).toUri
    if (uri.getScheme == "file") {
      val root = Paths.get(uri)
      Files.createDirectories(root)
      val canonical = root.toRealPath()
      val local = Locks.synchronized {
        val entry = Locks.getOrElseUpdate(canonical, LocalLock(new ReentrantLock(), 0))
        entry.users += 1
        entry
      }
      try {
        local.lock.lockInterruptibly()
        try {
          val channel = FileChannel.open(canonical.resolve(LockFile),
            StandardOpenOption.CREATE, StandardOpenOption.WRITE)
          try {
            val lock = acquire(channel)
            try {
              if (Thread.currentThread().isInterrupted) {
                throw new InterruptedException("AIPrompt control transaction interrupted before execution.")
              }
              transaction
            } finally lock.release()
          } finally channel.close()
        } finally local.lock.unlock()
      } finally Locks.synchronized {
        local.users -= 1
        if (local.users == 0) Locks.remove(canonical)
      }
    } else {
      transaction
    }
  } catch {
    case error: InterruptedException =>
      Thread.currentThread().interrupt()
      throw error
  }
}
