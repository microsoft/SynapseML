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

    def awaitRelease(): Unit = {
      val task = TaskContext.get()
      context.set(task)
      task.addTaskFailureListener((_: TaskContext, error: Throwable) => taskFailure.set(error))
      task.addTaskCompletionListener[Unit] { _ =>
        completionInterrupted.set(Thread.currentThread().isInterrupted)
        completed.countDown()
      }
      entered.countDown()
      require(release.await(30, TimeUnit.SECONDS), "Attachment cancellation test gate timed out")
    }
  }

  val Active = new ConcurrentHashMap[String, Gate]()

  def pathAfterCancellation(id: String): Iterator[(String, String)] = {
    Active.get(id).awaitRelease()
    Iterator.single("Describe this image." -> Location.resolve(id).toString)
  }

  val TimeoutNames = Seq("socket-timeout.png", "connect-timeout.png",
    "close-socket-timeout.png", "close-connect-timeout.png", "wrapped-socket-timeout.png",
    "wrapped-connect-timeout.png", "relocated-connect-timeout.png", "relocated-pool-timeout.png",
    "nested-connect-timeout.png", "nested-interrupted-timeout.png", "httpclient5-timeout.png",
    "translated-pool-timeout.png", "timeout-cause-cycle.png", "concurrent-timeout.png",
    "http-timeout.png", "translated-concurrent-timeout.png")

  val ClearedInterruptNames = Seq("interrupted-io.png", "close-interrupted-io.png", "wrapped-interrupted.png",
    "wrapped-interrupted-io.png", "wrapped-closed-interrupt.png", "timeout-close-interrupted.png",
    "translated-interrupted.png", "timeout-nested-close-interrupted.png", "timeout-shared-close-interrupted.png",
    "concurrent-timeout-close-interrupted.png", "http-timeout-close-interrupted.png")

  val InterruptedNames = Seq("interrupted.png", "plain-interrupted.png", "flagged.png", "read-close.png") ++
    ClearedInterruptNames

  private object RelocatedHttpClient {
    class ConnectTimeoutException extends InterruptedIOException("Synthetic private connect timeout")
    class ConnectionPoolTimeoutException extends ConnectTimeoutException
    class HttpTimeoutException extends IOException("Synthetic private HTTP timeout")
  }

  private def timeoutCausedBy(cause: Throwable): SocketTimeoutException = {
    val error = new SocketTimeoutException("Synthetic private storage timeout")
    error.initCause(new IOException("Synthetic private storage wrapper", cause))
    error
  }

  def cyclicInterruption(): InterruptedIOException = {
    val error = new InterruptedIOException("Synthetic private interruption") {
      private var causeReads = 0
      override def getCause: Throwable = {
        causeReads += 1
        // Bound traversal so a broken cycle guard fails instead of hanging the suite.
        assert(causeReads <= 2, "Cyclic interruption cause traversal did not terminate")
        super.getCause
      }
    }
    error.initCause(new IOException("Synthetic private cyclic cause", error))
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

  private def timeoutWithCleanup(id: String): IOException = {
    val interruption = new InterruptedIOException("Synthetic private cleanup interruption")
    val cause = if (id == "timeout-shared-close-interrupted.png") interruption
      else new RelocatedHttpClient.ConnectTimeoutException
    val error = timeoutCausedBy(cause)
    val cleanupOwner = if (id == "timeout-nested-close-interrupted.png") error.getCause else error
    cleanupOwner.addSuppressed(interruption)
    error
  }

  private def storageTimeout(id: String): Option[IOException] = id match {
    case "relocated-connect-timeout.png" => Some(new RelocatedHttpClient.ConnectTimeoutException)
    case "relocated-pool-timeout.png" | "translated-pool-timeout.png" =>
      Some(new RelocatedHttpClient.ConnectionPoolTimeoutException {})
    case "nested-connect-timeout.png" =>
      Some(timeoutCausedBy(new RelocatedHttpClient.ConnectTimeoutException))
    case "nested-interrupted-timeout.png" =>
      Some(timeoutCausedBy(new InterruptedIOException("Synthetic private timeout cause")))
    case "httpclient5-timeout.png" =>
      Some(new org.apache.hc.client5.http.ConnectTimeoutException("Synthetic private connect timeout"))
    case "timeout-cause-cycle.png" =>
      val error = timeoutCausedBy(new IOException("Synthetic private cyclic timeout cause"))
      error.getCause.getCause.initCause(error)
      Some(error)
    case "timeout-close-interrupted.png" | "timeout-nested-close-interrupted.png" |
         "timeout-shared-close-interrupted.png" =>
      Some(timeoutWithCleanup(id))
    case _ => None
  }

  private def otherTimeout(id: String): Option[Throwable] = {
    val timeout = id match {
      case "concurrent-timeout.png" | "translated-concurrent-timeout.png" |
           "concurrent-timeout-close-interrupted.png" =>
        Some(new java.util.concurrent.TimeoutException("Synthetic private concurrent timeout"))
      case "http-timeout.png" | "http-timeout-close-interrupted.png" =>
        Some(new RelocatedHttpClient.HttpTimeoutException)
      case _ => None
    }
    timeout.map { error =>
      error.initCause(new InterruptedException("Synthetic private timeout cause"))
      if (id.endsWith("-close-interrupted.png")) {
        error.getCause.addSuppressed(new InterruptedIOException("Synthetic private cleanup interruption"))
      }
      error
    }
  }

  private def wrappedFailure(id: String): Option[IOException] = {
    val cause = id match {
      case "wrapped-interrupted.png" | "translated-interrupted.png" =>
        Some(new InterruptedException("Synthetic private path"))
      case "wrapped-interrupted-io.png" => Some(new InterruptedIOException("Synthetic private path"))
      case "wrapped-closed-interrupt.png" => Some(new ClosedByInterruptException())
      case "wrapped-socket-timeout.png" => Some(new SocketTimeoutException("Synthetic private path"))
      case "wrapped-connect-timeout.png" => Some(new ConnectTimeoutException("Synthetic private path"))
      case _ => None
    }
    cause.orElse(storageTimeout(id)).orElse(otherTimeout(id)).map { underlying =>
      val wrapper = new IOException("Synthetic private wrapped failure", underlying)
      if (id.startsWith("translated-")) {
        val translated = new InterruptedIOException("Synthetic private translated failure")
        translated.initCause(wrapper)
        translated
      } else wrapper
    }
  }

  private def ordinaryFailure(id: String, active: Boolean, unexpected: ConcurrentLinkedQueue[String]): Nothing = {
    if (!active && !Set("ordinary.png", "flagged.png").contains(id)) unexpected.add(id)
    throw new IOException("Synthetic attachment read failure")
  }

  def open(id: String, unexpected: ConcurrentLinkedQueue[String]): FSDataInputStream = {
    val active = Option(Active.get(id))
    active.foreach(_.awaitRelease())
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
      ordinaryFailure(id, active.isDefined, unexpected)
    }
  }
}

class AttachmentReadFailureFileSystem extends RawLocalFileSystem {
  val opened = new ConcurrentLinkedQueue[String]()
  val unexpected = new ConcurrentLinkedQueue[String]()

  override def getUri: URI = AttachmentReadFailures.Location

  override def open(path: Path, bufferSize: Int): FSDataInputStream = {
    opened.add(path.getName)
    AttachmentReadFailures.open(path.getName, unexpected)
  }
}
