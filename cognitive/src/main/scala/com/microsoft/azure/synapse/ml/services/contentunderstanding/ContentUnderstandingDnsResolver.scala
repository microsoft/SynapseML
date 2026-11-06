// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.contentunderstanding

import org.apache.http.conn.DnsResolver

import java.io.InterruptedIOException
import java.net.{InetAddress, UnknownHostException}
import java.util.ArrayDeque
import java.util.concurrent.{ArrayBlockingQueue, Callable, ExecutionException, Future, RejectedExecutionException,
  Semaphore, ThreadFactory, ThreadPoolExecutor, TimeUnit, TimeoutException}
import scala.annotation.tailrec

private[contentunderstanding] class OperationDeadlineExceeded
  extends UnknownHostException(ContentUnderstandingProtocol.timeoutError)

private[contentunderstanding] class DnsResolutionFailed(cause: Throwable)
  extends UnknownHostException(ContentUnderstandingProtocol.diagnostic("DnsResolutionFailed",
    "DNS resolution failed before the HTTP request was sent.")) {
  initCause(cause)
}

private[contentunderstanding] class DnsCapacityExceeded(val deadlineExpired: Boolean = false)
  extends UnknownHostException(ContentUnderstandingProtocol.diagnostic("DnsCapacityExceeded",
    "DNS worker capacity was unavailable within the operation budget; no HTTP request was sent."))

/** Bounds blocking JVM DNS resolution by the current Content Understanding operation budget. */
private[contentunderstanding] class BudgetedDnsResolver(remainingMs: () => Long,
                                                        delegate: DnsResolver) extends DnsResolver {
  override def resolve(host: String): Array[InetAddress] = {
    val remaining = remainingMs()
    if (remaining == 0) {
      throw new OperationDeadlineExceeded
    }
    try {
      BudgetedDnsResolver.acquire(remaining)
    } catch {
      case error: InterruptedException => throw interrupted(error)
    }
    var submitted = false
    val resolution = try {
      ContentUnderstandingProtocol.checkInterrupted()
      if (remainingMs() == 0) throw new OperationDeadlineExceeded
      val future = BudgetedDnsResolver.Executor.submit(new Callable[Array[InetAddress]] {
        override def call(): Array[InetAddress] =
          try {
            delegate.resolve(host)
          } finally {
            BudgetedDnsResolver.release()
          }
      })
      submitted = true
      future
    } catch {
      case error: InterruptedException => throw interrupted(error)
      case _: RejectedExecutionException =>
        throw new DnsCapacityExceeded()
    } finally {
      if (!submitted) BudgetedDnsResolver.release()
    }
    awaitResolution(resolution)
  }

  private def awaitResolution(resolution: Future[Array[InetAddress]]): Array[InetAddress] = {
    val timeout = remainingMs()
    val started = System.nanoTime()
    @tailrec
    def poll(): Array[InetAddress] = {
      ContentUnderstandingProtocol.checkInterrupted()
      val wait = BudgetedDnsResolver.waitRemaining(started, timeout)
      if (wait == 0) throw new OperationDeadlineExceeded
      val addresses = try {
        Some(resolution.get(math.min(wait, ContentUnderstandingBudget.CancellationPollMs), TimeUnit.MILLISECONDS))
      } catch {
        case _: TimeoutException => None
      }
      addresses match {
        case Some(value) => value
        case None => poll()
      }
    }
    try {
      val addresses = poll()
      if (remainingMs() == 0) {
        throw new OperationDeadlineExceeded
      }
      addresses
    } catch {
      case error: ExecutionException =>
        throw new DnsResolutionFailed(error.getCause)
      case error: InterruptedException =>
        throw interrupted(error)
    }
  }

  private def interrupted(error: InterruptedException): InterruptedIOException = {
    Thread.currentThread().interrupt()
    val failure = new InterruptedIOException("DNS resolution was interrupted.")
    failure.initCause(error)
    failure
  }
}

private[contentunderstanding] object BudgetedDnsResolver {
  def expiredBeforeDispatch(error: UnknownHostException): Boolean = error match {
    case _: OperationDeadlineExceeded => true
    case capacity: DnsCapacityExceeded => capacity.deadlineExpired
    case _ => false
  }

  def diagnostic(error: UnknownHostException, timedOut: Boolean): String =
    Option(error.getMessage).filter(_ =>
      error.isInstanceOf[DnsCapacityExceeded] || error.isInstanceOf[DnsResolutionFailed] || !timedOut)
      .getOrElse(ContentUnderstandingProtocol.timeoutError)

  private[contentunderstanding] val MaxConcurrentLookups = 32
  private val IdleThreadKeepAliveSeconds = 60L

  private val Capacity = new Semaphore(MaxConcurrentLookups, true)
  private val Waiters = new ArrayDeque[Object]()

  private def waitRemaining(started: Long, timeout: Long): Long =
    math.max(0L, timeout - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started))

  private def acquire(timeout: Long): Unit = {
    val started = System.nanoTime()
    val waiter = new Object()
    Waiters.synchronized { Waiters.addLast(waiter) }
    @tailrec
    def poll(): Unit = {
      ContentUnderstandingProtocol.checkInterrupted()
      val acquired = Waiters.synchronized {
        val remaining = waitRemaining(started, timeout)
        if (remaining == 0) throw new DnsCapacityExceeded(deadlineExpired = true)
        if ((Waiters.peekFirst() eq waiter) && Capacity.tryAcquire(0L, TimeUnit.MILLISECONDS)) {
          true
        } else {
          Waiters.wait(math.min(remaining, ContentUnderstandingBudget.CancellationPollMs))
          false
        }
      }
      if (!acquired) poll()
    }
    // Keep the queue position across cancellation checks, removing it on every exit path.
    try {
      poll()
    } finally {
      Waiters.synchronized {
        Waiters.remove(waiter)
        Waiters.notifyAll()
      }
    }
  }

  private def release(): Unit = Waiters.synchronized {
    Capacity.release()
    Waiters.notifyAll()
  }

  private lazy val Executor = {
    val executor = new ThreadPoolExecutor(MaxConcurrentLookups, MaxConcurrentLookups, IdleThreadKeepAliveSeconds,
    TimeUnit.SECONDS,
    new ArrayBlockingQueue[Runnable](MaxConcurrentLookups), new ThreadFactory {
      override def newThread(task: Runnable): Thread = {
        val thread = new Thread(task, "synapseml-cu-dns")
        thread.setDaemon(true)
        thread
      }
    })
    executor.allowCoreThreadTimeOut(true)
    executor
  }
}
