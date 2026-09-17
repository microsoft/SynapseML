// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm

import org.slf4j.Logger

import java.io.IOException
import java.net.{BindException, InetSocketAddress, Socket, SocketTimeoutException}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}
import java.util.concurrent.{ExecutionException, Future, ScheduledThreadPoolExecutor, ThreadFactory, TimeUnit}
import scala.annotation.tailrec

private[lightgbm] object NetworkManagerSocketSupport {
  private val MaxSocketCloseAttempts = 2
  private val WriteTimeoutPollDivisor = 4
  private val WatchdogThreadCounter = new AtomicInteger()
  private val WatchdogThreadFactory = new ThreadFactory {
    override def newThread(runnable: Runnable): Thread = {
      val thread = new Thread(
        runnable,
        s"synapseml-socket-write-watchdog-${WatchdogThreadCounter.incrementAndGet()}")
      thread.setDaemon(true)
      thread
    }
  }
  private val WriteWatchdog = {
    val executor = new ScheduledThreadPoolExecutor(1, WatchdogThreadFactory)
    executor.setRemoveOnCancelPolicy(true)
    executor
  }

  private sealed trait WriteState
  private case object WriteActive extends WriteState
  private case object WriteComplete extends WriteState
  private case class WriteTimedOut(failure: SocketTimeoutException) extends WriteState

  def addSuppressed(primaryFailure: Throwable, secondaryFailure: Throwable): Unit = {
    if (primaryFailure ne secondaryFailure) primaryFailure.addSuppressed(secondaryFailure)
  }

  def recordFailure(reference: AtomicReference[Throwable], failure: Throwable): Unit = {
    val existing = reference.get()
    if (existing != null) { // scalastyle:ignore null
      addSuppressed(existing, failure)
    } else if (!reference.compareAndSet(null, failure)) { // scalastyle:ignore null
      addSuppressed(reference.get(), failure)
    }
  }

  def awaitFuture(future: Future[_]): Unit = {
    try {
      future.get()
    } catch {
      case interrupted: InterruptedException =>
        Thread.currentThread().interrupt()
        throw interrupted
      case failed: ExecutionException =>
        throw Option(failed.getCause).getOrElse(failed)
    }
  }

  def withSocketWriteTimeout[T](socket: Socket,
                                timeoutMillis: Int)
                               (operation: (() => Unit) => T): T = {
    require(timeoutMillis > 0, "Socket write timeout must be positive")
    val timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis.toLong)
    val pollMillis = Math.max(1L, timeoutMillis.toLong / WriteTimeoutPollDivisor)
    val lastProgress = new AtomicLong(System.nanoTime())
    val state = new AtomicReference[WriteState](WriteActive)
    val watchdog = WriteWatchdog.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = {
          if (System.nanoTime() - lastProgress.get() >= timeoutNanos) {
            val timeout = new SocketTimeoutException(
              s"Timed out after $timeoutMillis ms without validation socket write progress")
            if (state.compareAndSet(WriteActive, WriteTimedOut(timeout))) {
              try closeSocketWithRetry(socket)
              catch {
                case closeFailure: IOException => addSuppressed(timeout, closeFailure)
              }
            }
          }
        }
      },
      pollMillis,
      pollMillis,
      TimeUnit.MILLISECONDS)

    try {
      val result = operation(() => lastProgress.set(System.nanoTime()))
      if (state.compareAndSet(WriteActive, WriteComplete)) result
      else throw writeTimeoutFailure(state.get(), None)
    } catch {
      case failure: Throwable =>
        if (state.compareAndSet(WriteActive, WriteComplete)) throw failure
        else throw writeTimeoutFailure(state.get(), Option(failure))
    } finally {
      watchdog.cancel(false)
    }
  }

  private def writeTimeoutFailure(state: WriteState,
                                  operationFailure: Option[Throwable]): Throwable = {
    state match {
      case WriteTimedOut(timeout) =>
        operationFailure.foreach(failure => addSuppressed(timeout, failure))
        timeout
      case _ => operationFailure.getOrElse(
        new IllegalStateException("Validation socket write completed without an outcome"))
    }
  }

  /** Close a socket with one immediate retry, retaining every observed cleanup failure. */
  def closeSocketWithRetry(socket: Socket): Unit = {
    @tailrec
    def attemptClose(attemptsRemaining: Int,
                     firstFailure: Option[IOException]): Option[IOException] = {
      if (socket.isClosed || attemptsRemaining == 0) {
        firstFailure
      } else {
        val updatedFailure = try {
          socket.close()
          firstFailure
        } catch {
          case failure: IOException =>
            firstFailure.foreach(existing => addSuppressed(existing, failure))
            firstFailure.orElse(Option(failure))
        }
        attemptClose(attemptsRemaining - 1, updatedFailure)
      }
    }

    val closeFailure = attemptClose(MaxSocketCloseAttempts, None).orElse {
      if (socket.isClosed) None else Option(new IOException("Socket remained open after cleanup attempts"))
    }
    closeFailure.foreach(throw _)
  }

  /** Run cleanup without allowing it to replace a failure from the protected operation.
    *
    * The Throwable catch is deliberately limited to recording and immediately rethrowing the original
    * failure; it is not a fallback or recovery boundary.
    */
  def withCleanupPreservingPrimary[T](cleanup: => Unit)(operation: => T): T = {
    var primaryFailure: Option[Throwable] = None
    try {
      operation
    } catch {
      case failure: Throwable =>
        primaryFailure = Option(failure)
        throw failure
    } finally {
      try {
        cleanup
      } catch {
        case cleanupFailure: Throwable if primaryFailure.isDefined =>
          addSuppressed(primaryFailure.get, cleanupFailure)
      }
    }
  }

  /** Run cleanup only when the protected operation fails, preserving that primary failure. */
  def withCleanupOnFailurePreservingPrimary[T](cleanup: => Unit)(operation: => T): T = {
    var completed = false
    withCleanupPreservingPrimary(if (!completed) cleanup) {
      val result = operation
      completed = true
      result
    }
  }

  /** Reserve the first available port at or above basePort.
    *
    * Only address-in-use failures advance to another port. Other failures propagate after the candidate
    * socket is closed, rather than silently falling back to a different port.
    */
  def reserveOpenPort(basePort: Int, log: Logger): Socket = {
    reserveOpenPort(basePort, log, () => new Socket())
  }

  def reserveOpenPort(basePort: Int,
                      log: Logger,
                      createSocket: () => Socket): Socket = {
    validatePort(basePort)

    @tailrec
    def reservePort(localListenPort: Int): Socket = {
      val bindResult: Either[BindException, Socket] = try {
        Right(reserveExactPort(localListenPort, log, createSocket))
      } catch {
        // A suppressed exception means candidate cleanup failed, so proceeding would leak a socket.
        case contention: BindException if contention.getSuppressed.isEmpty => Left(contention)
      }

      bindResult match {
        case Right(reservation) => reservation
        case Left(_) =>
          log.warn(s"Could not bind to port $localListenPort...")
          val nextPort = localListenPort + 1
          if (nextPort > LightGBMConstants.MaxPort) {
            throw new Exception(s"Error: port $basePort out of range, " +
              "possibly due to networking or firewall issues")
          }
          if (nextPort - basePort > 1000) {
            throw new Exception("Error: Could not find open port after 1k tries")
          }
          reservePort(nextPort)
      }
    }

    reservePort(basePort)
  }

  /** Reserve one exact port. Native-init retries cannot change the previously advertised port. */
  def reserveExactPort(localListenPort: Int, log: Logger): Socket = {
    reserveExactPort(localListenPort, log, () => new Socket())
  }

  private def reserveExactPort(localListenPort: Int,
                               log: Logger,
                               createSocket: () => Socket): Socket = {
    validatePort(localListenPort)
    val candidate = createSocket()
    withCleanupOnFailurePreservingPrimary(closeSocketWithRetry(candidate)) {
      candidate.bind(new InetSocketAddress(localListenPort))
      log.info(s"Successfully bound to port $localListenPort")
      candidate
    }
  }

  private def validatePort(port: Int): Unit = {
    if (port < 0 || port > LightGBMConstants.MaxPort) {
      throw new Exception(s"Error: port $port out of range, possibly due to too many executors or unknown error")
    }
  }

  /** Keep a training task's port reserved, while releasing helper and failed-task reservations immediately. */
  def withPortReservation(reservation: Socket,
                          shouldExecuteTraining: Boolean)
                         (getTopology: Int => NetworkTopologyInfo): NetworkTopologyInfo = {
    var retained = false
    withCleanupPreservingPrimary(if (!retained) closeSocketWithRetry(reservation)) {
      val topology = getTopology(reservation.getLocalPort)
      if (shouldExecuteTraining) {
        topology.retainPortReservation(reservation)
        retained = true
      }
      topology
    }
  }
}
