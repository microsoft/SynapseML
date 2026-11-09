// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import org.apache.hadoop.fs.{FSDataInputStream, FSInputStream, Path, RawLocalFileSystem}
import org.apache.http.conn.ConnectTimeoutException
import org.apache.spark.TaskContext

import java.io.{ByteArrayInputStream, InterruptedIOException, IOException}
import java.net.{SocketTimeoutException, URI}
import java.nio.channels.ClosedByInterruptException
import java.util.concurrent.{CancellationException, ConcurrentHashMap, ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

private[openai] object AttachmentReadFailures {
  val Location: URI = URI.create("attachmentfailure:///")

  class Gate(val content: Option[Array[Byte]] = None) {
    val entered = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val completed = new CountDownLatch(1)
    val context = new AtomicReference[TaskContext]()
    val taskFailure = new AtomicReference[Throwable]()
    val completionInterrupted = new AtomicBoolean()
  }

  val Active = new ConcurrentHashMap[String, Gate]()

  val TimeoutNames = Seq("socket-timeout.png", "connect-timeout.png",
    "close-socket-timeout.png", "close-connect-timeout.png", "wrapped-socket-timeout.png",
    "wrapped-connect-timeout.png", "relocated-connect-timeout.png", "relocated-pool-timeout.png",
    "nested-connect-timeout.png", "nested-interrupted-timeout.png", "httpclient5-timeout.png")

  private object RelocatedHttpClient {
    class ConnectTimeoutException extends InterruptedIOException("Synthetic private connect timeout")
    class ConnectionPoolTimeoutException extends ConnectTimeoutException
  }

  private def timeoutCausedBy(cause: Throwable): SocketTimeoutException = {
    val error = new SocketTimeoutException("Synthetic private storage timeout")
    error.initCause(new IOException("Synthetic private storage wrapper", cause))
    error
  }

  def cyclicExceptionGraph(includeCauses: Boolean): (IOException, IOException) = {
    val root = new IOException("Synthetic private path")
    val last = (1 to 2048).foldLeft(root) { (previous, index) =>
      val next = new IOException("Synthetic private cleanup failure")
      if (includeCauses && index % 2 == 0) previous.initCause(next) else previous.addSuppressed(next)
      next
    }
    last.addSuppressed(root)
    root -> last
  }

  private def throwWithClearedInterrupt(error: IOException): Nothing = {
    Thread.currentThread().interrupt()
    Thread.interrupted()
    throw error
  }

  private def cleanupFailure(id: String): Option[IOException] = id match {
    case "close-interrupted-io.png" => Some(new InterruptedIOException("Synthetic private close failure"))
    case "close-socket-timeout.png" => Some(new SocketTimeoutException("Synthetic private close failure"))
    case "close-connect-timeout.png" => Some(new ConnectTimeoutException("Synthetic private close failure"))
    case _ => None
  }

  private def storageTimeout(id: String): Option[IOException] = id match {
    case "relocated-connect-timeout.png" => Some(new RelocatedHttpClient.ConnectTimeoutException)
    case "relocated-pool-timeout.png" => Some(new RelocatedHttpClient.ConnectionPoolTimeoutException {})
    case "nested-connect-timeout.png" =>
      Some(timeoutCausedBy(new RelocatedHttpClient.ConnectTimeoutException))
    case "nested-interrupted-timeout.png" =>
      Some(timeoutCausedBy(new InterruptedIOException("Synthetic private timeout cause")))
    case "httpclient5-timeout.png" =>
      Some(new org.apache.hc.client5.http.ConnectTimeoutException("Synthetic private connect timeout"))
    case "timeout-close-interrupted.png" =>
      val error = timeoutCausedBy(new RelocatedHttpClient.ConnectTimeoutException)
      error.addSuppressed(new InterruptedIOException("Synthetic private cleanup interruption"))
      Some(error)
    case _ => None
  }

  private def wrappedFailure(id: String): Option[IOException] = {
    val cause = id match {
      case "wrapped-interrupted.png" => Some(new InterruptedException("Synthetic private path"))
      case "wrapped-interrupted-io.png" => Some(new InterruptedIOException("Synthetic private path"))
      case "wrapped-closed-interrupt.png" => Some(new ClosedByInterruptException())
      case "wrapped-socket-timeout.png" => Some(new SocketTimeoutException("Synthetic private path"))
      case "wrapped-connect-timeout.png" => Some(new ConnectTimeoutException("Synthetic private path"))
      case _ => None
    }
    cause.orElse(storageTimeout(id)).map(new IOException("Synthetic private wrapped failure", _))
  }

  def open(id: String): FSDataInputStream = {
    val active = Option(Active.get(id))
    active.foreach { gate =>
      val context = TaskContext.get()
      gate.context.set(context)
      context.addTaskFailureListener((_: TaskContext, error: Throwable) => gate.taskFailure.set(error))
      context.addTaskCompletionListener[Unit] { _ =>
        gate.completionInterrupted.set(Thread.currentThread().isInterrupted)
        gate.completed.countDown()
      }
      gate.entered.countDown()
      require(gate.release.await(30, TimeUnit.SECONDS), "Attachment cancellation test gate timed out")
    }
    wrappedFailure(id).foreach(throwWithClearedInterrupt)
    if (id == "interrupted.png") throw new ClosedByInterruptException()
    if (id == "plain-interrupted.png") throw new InterruptedException("Synthetic private path")
    if (id == "interrupted-io.png") {
      throwWithClearedInterrupt(new InterruptedIOException("Synthetic private path"))
    }
    if (id == "socket-timeout.png") throw new SocketTimeoutException("Synthetic private path")
    if (id == "connect-timeout.png") throw new ConnectTimeoutException("Synthetic private path")
    if (id == "flagged.png") Thread.currentThread().interrupt()
    if (id == "cancelled.png") throw new CancellationException("Synthetic private path")
    val cleanupError = cleanupFailure(id)
    val streamContent = active.flatMap(_.content).orElse(cleanupError.map(_ => Array.emptyByteArray))
    if (id == "read-close.png" || streamContent.isDefined) {
      val content = streamContent.getOrElse(Array.emptyByteArray)
      val bytes = new ByteArrayInputStream(content)
      new FSDataInputStream(new FSInputStream {
        override def getPos: Long = content.length - bytes.available()
        override def seek(position: Long): Unit = throw new UnsupportedOperationException("Test stream is not seekable")
        override def seekToNewSource(position: Long): Boolean = false
        override def read(): Int = {
          if (id == "read-close.png") throw new InterruptedException("Synthetic private path")
          if (cleanupError.isDefined) throw new IOException("Synthetic private read failure")
          bytes.read()
        }
        override def close(): Unit = {
          bytes.close()
          if (id == "read-close.png") throw new IOException("Synthetic private close failure")
          cleanupError.foreach(throwWithClearedInterrupt)
        }
      })
    } else {
      throw new IOException("Synthetic attachment read failure")
    }
  }
}

class AttachmentReadFailureFileSystem extends RawLocalFileSystem {
  val opened = new ConcurrentLinkedQueue[String]()

  override def getUri: URI = AttachmentReadFailures.Location

  override def open(path: Path, bufferSize: Int): FSDataInputStream = {
    opened.add(path.getName)
    AttachmentReadFailures.open(path.getName)
  }
}
