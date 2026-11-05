// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.http

import org.apache.http.client.methods.HttpRequestBase
import org.apache.spark.{TaskContext, TaskKilledException}
import org.apache.spark.internal.Logging

import java.util.concurrent.{Callable, CancellationException, ConcurrentHashMap, ExecutionException, Executors, Future,
  FutureTask, ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit, TimeoutException}
import java.util.concurrent.atomic.AtomicBoolean
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

private[http] object CancellableRequests extends Logging {
  private val PollMillis = 100L
  private val PollNanos = TimeUnit.MILLISECONDS.toNanos(PollMillis)
  private val DrainSeconds = 5L
  private val DrainNanos = TimeUnit.SECONDS.toNanos(DrainSeconds)
  private val Active = new ThreadLocal[Scope]()
  private lazy val CancellationMonitor = {
    val monitor = new ScheduledThreadPoolExecutor(1, (runnable: Runnable) => {
      val thread = Executors.defaultThreadFactory().newThread(runnable)
      thread.setName("synapseml-http-cancellation-monitor")
      thread.setDaemon(true)
      thread
    })
    monitor.setRemoveOnCancelPolicy(true)
    monitor
  }

  private def preserveInterruption[T](action: => T): T = try action catch {
    case error: InterruptedException =>
      Thread.currentThread().interrupt()
      throw error
  }

  def check(): Unit = {
    if (Thread.currentThread().isInterrupted) {
      throw new InterruptedException("HTTP request canceled")
    }
    if (Option(Active.get()).exists(_.isCancelled)) {
      throw new CancellationException("HTTP request canceled")
    }
  }

  def withRequest[T](request: HttpRequestBase)(action: => T): T = {
    val scope = Option(Active.get())
    scope.foreach(_.requests.add(request))
    try preserveInterruption {
      check()
      action
    } finally scope.foreach(_.requests.remove(request))
  }

  private class Scope(concurrency: Int) extends AutoCloseable {
    val cancelled = new AtomicBoolean(false)
    val requests = ConcurrentHashMap.newKeySet[HttpRequestBase]()
    val context: Option[TaskContext] = Option(TaskContext.get())
    private val pool = Executors.newFixedThreadPool(concurrency, (runnable: Runnable) => {
      val thread = Executors.defaultThreadFactory().newThread(runnable)
      thread.setName("synapseml-bounded-http-" + thread.getId)
      thread.setDaemon(true)
      thread
    })
    private var closed = false
    private val cleanupLock = new Object()
    private var cleanupAttempted = false
    private var abortWork: Option[FutureTask[Unit]] = None
    private var cancellationWatch: Option[ScheduledFuture[_]] = None

    def isCancelled: Boolean = cancelled.get() || context.exists(_.isInterrupted())

    def observeCancellation(): Unit = synchronized {
      if (!closed) {
        cancellationWatch = context.map { task =>
          CancellationMonitor.scheduleWithFixedDelay(new Runnable {
            override def run(): Unit = if (task.isInterrupted()) cancel()
          }, PollMillis, PollMillis, TimeUnit.MILLISECONDS)
        }
      }
    }

    def checkTask(): Unit = {
      check()
      if (context.exists(_.isInterrupted())) {
        throw new TaskKilledException("HTTP request task canceled")
      }
      if (cancelled.get()) {
        throw new CancellationException("HTTP request canceled")
      }
    }

    def submit[T](action: => T): Future[T] = {
      checkTask()
      pool.submit(new Callable[T] {
        override def call(): T = {
          Active.set(Scope.this)
          try {
            check()
            action
          } finally Active.remove()
        }
      })
    }

    @tailrec
    final def await[T](future: Future[T], started: Long, timeout: Duration): T = {
      checkTask()
      val remaining = if (timeout.isFinite) timeout.toNanos - (System.nanoTime() - started) else Long.MaxValue
      val waitNanos = math.max(0L, math.min(remaining, PollNanos))
      val result = try Some(future.get(waitNanos, TimeUnit.NANOSECONDS)) catch {
        case _: TimeoutException if remaining <= 0 => throw new TimeoutException("HTTP concurrent request timed out")
        case _: TimeoutException => None
        case failure: ExecutionException =>
          checkTask()
          throw failure.getCause
      }
      result match {
        case Some(value) => value
        case None => await(future, started, timeout)
      }
    }

    private def cancel(): Option[FutureTask[Unit]] = synchronized {
      if (!closed) {
        closed = true
        cancelled.set(true)
        cancellationWatch.foreach(_.cancel(false))
        pool.shutdownNow()
        val activeRequests = requests.asScala.toVector
        if (activeRequests.nonEmpty) {
          val work = new FutureTask[Unit](new Callable[Unit] {
            override def call(): Unit = activeRequests.foreach { request =>
              try request.abort() catch {
                case NonFatal(error) =>
                  logWarning(s"HTTP request abort failed (${error.getClass.getSimpleName})")
              }
            }
          })
          abortWork = Some(work)
          val thread = new Thread(work)
          thread.setName("synapseml-http-abort-" + thread.getId)
          thread.setDaemon(true)
          thread.start()
        }
      }
      abortWork
    }

    override def close(): Unit = cleanupLock.synchronized {
      if (!cleanupAttempted) {
        cleanupAttempted = true
        val work = cancel()
        var interrupted = Thread.interrupted()
        val started = System.nanoTime()
        def remaining: Long = math.max(0L, DrainNanos - (System.nanoTime() - started))
        var failure: Option[Exception] = None
        def attempt(action: => Unit): Unit = try action catch {
          case error: Exception =>
            interrupted = interrupted || error.isInstanceOf[InterruptedException]
            failure match {
              case Some(original) => original.addSuppressed(error)
              case None => failure = Some(error)
            }
        }
        try {
          attempt {
            work.foreach { abort =>
              try abort.get(remaining, TimeUnit.NANOSECONDS) catch {
                case error: TimeoutException =>
                  abort.cancel(true)
                  throw new IllegalStateException(
                    "HTTP request abort did not stop; request quiescence is not confirmed", error)
                case error: Exception =>
                  abort.cancel(true)
                  throw error
              }
            }
          }
          attempt {
            require(pool.awaitTermination(remaining, TimeUnit.NANOSECONDS),
              "HTTP request workers did not stop; request quiescence is not confirmed")
          }
          failure.foreach(error => throw error)
        } finally {
          if (interrupted) Thread.currentThread().interrupt()
        }
      }
    }
  }

  def map[A, B](input: Iterator[A], concurrency: Int, timeout: Duration)
               (send: A => B): Iterator[B] with AutoCloseable = {
    val scope = new Scope(concurrency)
    scope.context.foreach(_.addTaskCompletionListener[Unit](_ => scope.close()))
    scope.observeCancellation()

    def guarded[T](action: => T): T = try preserveInterruption(action) catch {
      case error: Exception =>
        try scope.close() catch {
          case cleanup: Exception => error.addSuppressed(cleanup)
        }
        throw error
    }

    new Iterator[B] with AutoCloseable {
      private val pending = mutable.Queue.empty[Future[B]]
      private var finished = false

      override def close(): Unit = {
        finished = true
        pending.clear()
        scope.close()
      }

      @tailrec
      private def fill(): Unit = {
        scope.checkTask()
        if (pending.size < concurrency && input.hasNext) {
          val request = input.next()
          pending.enqueue(scope.submit(send(request)))
          fill()
        }
      }

      override def hasNext: Boolean = if (finished) false else guarded {
        fill()
        if (pending.isEmpty) close()
        !finished
      }

      override def next(): B = guarded {
        if (!hasNext) throw new NoSuchElementException("No more HTTP responses")
        scope.await(pending.dequeue(), System.nanoTime(), timeout)
      }
    }
  }
}
