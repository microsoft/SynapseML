// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.contentunderstanding

import org.apache.http.conn.DnsResolver

import java.io.InterruptedIOException
import java.net.{InetAddress, UnknownHostException}
import java.util.concurrent.{ArrayBlockingQueue, Callable, ExecutionException, RejectedExecutionException, Semaphore,
  ThreadFactory, ThreadPoolExecutor, TimeUnit, TimeoutException}

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
      if (!BudgetedDnsResolver.Capacity.tryAcquire(remaining, TimeUnit.MILLISECONDS)) {
        throw new DnsCapacityExceeded(deadlineExpired = true)
      }
    } catch {
      case error: InterruptedException => throw interrupted(error)
    }
    if (remainingMs() == 0) {
      BudgetedDnsResolver.Capacity.release()
      throw new OperationDeadlineExceeded
    }
    val resolution = try {
      BudgetedDnsResolver.Executor.submit(new Callable[Array[InetAddress]] {
        override def call(): Array[InetAddress] =
          try {
            delegate.resolve(host)
          } finally {
            BudgetedDnsResolver.Capacity.release()
          }
      })
    } catch {
      case _: RejectedExecutionException =>
        BudgetedDnsResolver.Capacity.release()
        throw new DnsCapacityExceeded()
    }
    try {
      val addresses = resolution.get(remainingMs(), TimeUnit.MILLISECONDS)
      if (remainingMs() == 0) {
        throw new OperationDeadlineExceeded
      }
      addresses
    } catch {
      case _: TimeoutException =>
        throw new OperationDeadlineExceeded
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
