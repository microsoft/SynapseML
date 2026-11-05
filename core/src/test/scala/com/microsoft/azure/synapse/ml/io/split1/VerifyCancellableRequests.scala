// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.split1

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.http.{HTTPRequestData, HTTPSchema, HandlingUtils, SingleThreadedHTTPClient}
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.spark.{TaskContextTestSupport, TaskKilledException}
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.{Seconds, Span}

import java.util.concurrent.{CancellationException, ConcurrentHashMap, CountDownLatch, ExecutionException, Semaphore,
  TimeUnit, TimeoutException}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Using

class VerifyCancellableRequests extends TestBase {
  test("driver response iterators can close before submitting requests") {
    val sent = new AtomicInteger()
    val handler: HandlingUtils.HandlerFunc = (_, _) => {
      sent.incrementAndGet()
      HTTPSchema.stringToResponse("unused", 200, "OK")
    }
    Using.resource(new SingleThreadedHTTPClient(handler, 1000)) { production =>
      val data = new HTTPRequestData(new HttpPost("http://example.invalid/test"))
      val results = production.sendCancellableRequestsWithContext(
        Iterator(production.RequestWithContext(Some(data), None)), 1, 30.seconds)
      try {
        results.close()
        results.close()
        assert(!results.hasNext && sent.get() == 0)
        intercept[NoSuchElementException](results.next())
      } finally results.toVector
    }
  }

  test("driver response iterators close abandoned work and drain every owned worker") {
    val started = new CountDownLatch(1)
    val release = new Semaphore(0)
    val workers = ConcurrentHashMap.newKeySet[Thread]()
    val abortWorker = new AtomicReference[Thread]()
    val aborts = new AtomicInteger()
    val post = new HttpPost("http://example.invalid/blocked") {
      override def abort(): Unit = {
        abortWorker.set(Thread.currentThread())
        aborts.incrementAndGet()
        release.release()
        super.abort()
      }
    }
    Using.resource(VerifyBoundedRequests.client(() => {
      started.countDown()
      release.acquireUninterruptibly()
      VerifyBoundedRequests.response(200, new StringEntity("released"), () => ())
    })) { http =>
      val handler: HandlingUtils.HandlerFunc = (_, data) => {
        workers.add(Thread.currentThread())
        if (data.requestLine.uri.endsWith("/first")) HTTPSchema.stringToResponse("first", 200, "OK")
        else HandlingUtils.bounded()(http, data)
      }
      Using.resource(new SingleThreadedHTTPClient(handler, 1000)) { production =>
        val first = new HTTPRequestData(new HttpPost("http://example.invalid/first"))
        val blocked = new HTTPRequestData(post) {
          override def toHTTPCore: HttpPost = post
        }
        val results = production.sendCancellableRequestsWithContext(
          Iterator(first, blocked).map(data => production.RequestWithContext(Some(data), None)), 2, 30.seconds)
        try {
          assert(results.next().response.nonEmpty && started.await(5, TimeUnit.SECONDS))
          results.close()
          results.close()
          assert(!results.hasNext && aborts.get() == 1)
          assert(workers.size() == 2 && workers.asScala.forall(!_.isAlive))
          abortWorker.get().join(5000)
          assert(!abortWorker.get().isAlive)
        } finally {
          release.release()
          results.toVector
          workers.asScala.foreach { worker =>
            worker.join(5000)
            assert(!worker.isAlive)
          }
        }
      }
    }
  }

  Seq(false, true).foreach { cooperative =>
    test(s"a logged abort failure only fails task cleanup while HTTP workers remain active: $cooperative") {
      val context = new TaskContextTestSupport.ControlledContext(() => ())
      val started = new CountDownLatch(1)
      val release = new Semaphore(0)
      val worker = new AtomicReference[Thread]()
      val abortWorker = new AtomicReference[Thread]()
      val aborts = new AtomicInteger()
      val post = new HttpPost("http://example.invalid/test") {
        override def abort(): Unit = {
          abortWorker.set(Thread.currentThread())
          aborts.incrementAndGet()
          if (cooperative) release.release()
          throw new IllegalStateException("Synthetic abort failure")
        }
      }
      Using.resource(VerifyBoundedRequests.client(() => {
        worker.set(Thread.currentThread())
        started.countDown()
        release.acquireUninterruptibly()
        VerifyBoundedRequests.response(200, new StringEntity("released"), () => ())
      })) { http =>
        val handler: HandlingUtils.HandlerFunc = (_, data) => HandlingUtils.bounded()(http, data)
        Using.resource(new SingleThreadedHTTPClient(handler, 1000)) { production =>
          val data = new HTTPRequestData(post) {
            override def toHTTPCore: HttpPost = post
          }
          val results = TaskContextTestSupport.withContext(context) {
            production.sendCancellableRequestsWithContext(
              Iterator(production.RequestWithContext(Some(data), None)), 1, 30.seconds)
          }
          try {
            assert(results.hasNext && started.await(5, TimeUnit.SECONDS))
            val before = System.nanoTime()
            if (cooperative) {
              context.complete()
              worker.get().join(5000)
              assert(!worker.get().isAlive)
            } else {
              val error = intercept[Exception](context.complete())
              assert(error.getMessage.contains("HTTP request workers did not stop"))
              assert(worker.get().isAlive)
            }
            assert((System.nanoTime() - before).nanos < 8.seconds)
            assert(aborts.get() == 1)
            intercept[CancellationException](results.hasNext)
          } finally {
            release.release()
            Seq(worker, abortWorker).foreach { reference =>
              Option(reference.get()).foreach { thread =>
                thread.join(5000)
                assert(!thread.isAlive)
              }
            }
          }
        }
      }
    }
  }

  test("an unresponsive abort fails bounded cleanup without starting another abort pass") {
    val started = new CountDownLatch(1)
    val release = new Semaphore(0)
    val abortRelease = new CountDownLatch(1)
    val aborts = new AtomicInteger()
    val worker = new AtomicReference[Thread]()
    val abortWorker = new AtomicReference[Thread]()
    val post = new HttpPost("http://example.invalid/test") {
      override def abort(): Unit = {
        abortWorker.set(Thread.currentThread())
        aborts.incrementAndGet()
        assert(abortRelease.await(10, TimeUnit.SECONDS))
        super.abort()
      }
    }
    Using.resource(VerifyBoundedRequests.client(() => {
      worker.set(Thread.currentThread())
      started.countDown()
      release.acquireUninterruptibly()
      VerifyBoundedRequests.response(200, new StringEntity("released"), () => ())
    })) { http =>
      val handler: HandlingUtils.HandlerFunc = (_, data) => HandlingUtils.bounded()(http, data)
      Using.resource(new SingleThreadedHTTPClient(handler, 1000)) { production =>
        val data = new HTTPRequestData(post) {
          override def toHTTPCore: HttpPost = post
        }
        val results = production.sendCancellableRequestsWithContext(
          Iterator(production.RequestWithContext(Some(data), None)), 1, 1.nanosecond)
        try {
          assert(results.hasNext && started.await(5, TimeUnit.SECONDS))
          val before = System.nanoTime()
          val error = intercept[TimeoutException](results.next())
          assert(error.getSuppressed.exists(_.getMessage.contains("HTTP request abort did not stop")))
          assert((System.nanoTime() - before).nanos < 8.seconds)
          intercept[CancellationException](results.hasNext)
          assert(aborts.get() == 1)
          eventually(timeout(Span(2, Seconds))) {
            assert(!abortWorker.get().isAlive, "Timed-out abort did not receive an interrupt")
          }
        } finally {
          abortRelease.countDown()
          release.release()
          Seq(worker, abortWorker).foreach { reference =>
            Option(reference.get()).filterNot(_ == Thread.currentThread()).foreach { thread =>
              thread.join(5000)
              assert(!thread.isAlive)
            }
          }
        }
      }
    }
  }

  Seq(false, true).foreach { interruptCaller =>
    val failureSource = if (interruptCaller) "abort wait interruption" else "exceptional abort completion"
    test(s"cleanup still drains HTTP workers after $failureSource") {
      val started = new CountDownLatch(1)
      val release = new Semaphore(0)
      val abortStarted = new CountDownLatch(1)
      val abortRelease = new CountDownLatch(1)
      val worker = new AtomicReference[Thread]()
      val abortWorker = new AtomicReference[Thread]()
      val failure = new AtomicReference[Throwable]()
      val callerInterrupted = new AtomicBoolean()
      val post = new HttpPost("http://example.invalid/test") {
        override def abort(): Unit = {
          abortWorker.set(Thread.currentThread())
          abortStarted.countDown()
          if (interruptCaller) {
            assert(abortRelease.await(10, TimeUnit.SECONDS))
            super.abort()
          } else {
            throw new InterruptedException("Synthetic abort failure")
          }
        }
      }
      Using.resource(VerifyBoundedRequests.client(() => {
        worker.set(Thread.currentThread())
        started.countDown()
        release.acquireUninterruptibly()
        VerifyBoundedRequests.response(200, new StringEntity("released"), () => ())
      })) { http =>
        val handler: HandlingUtils.HandlerFunc = (_, data) => HandlingUtils.bounded()(http, data)
        Using.resource(new SingleThreadedHTTPClient(handler, 1000)) { production =>
          val data = new HTTPRequestData(post) {
            override def toHTTPCore: HttpPost = post
          }
          val results = production.sendCancellableRequestsWithContext(
            Iterator(production.RequestWithContext(Some(data), None)), 1, 1.nanosecond)
          val consumer = new Thread(() => {
            try results.next() catch {
              case error: Throwable => failure.set(error)
            } finally callerInterrupted.set(Thread.currentThread().isInterrupted)
          })
          try {
            assert(results.hasNext && started.await(5, TimeUnit.SECONDS))
            consumer.start()
            assert(abortStarted.await(5, TimeUnit.SECONDS))
            if (interruptCaller) {
              eventually(timeout(Span(2, Seconds))) {
                assert(consumer.getStackTrace.exists(frame =>
                  frame.getClassName == "java.util.concurrent.FutureTask" && frame.getMethodName == "get"))
              }
              consumer.interrupt()
            }
            eventually(timeout(Span(2, Seconds))) {
              assert(consumer.getStackTrace.exists(frame =>
                frame.getClassName == "java.util.concurrent.ThreadPoolExecutor" &&
                  frame.getMethodName == "awaitTermination"), "Abort-wait failure skipped the worker drain")
              assert(!abortWorker.get().isAlive, "Failed abort wait left the owned abort thread running")
            }
          } finally {
            abortRelease.countDown()
            release.release()
            consumer.join(10000)
            Seq(worker, abortWorker).foreach { reference =>
              Option(reference.get()).foreach { thread =>
                thread.join(5000)
                assert(!thread.isAlive)
              }
            }
          }
          assert(!consumer.isAlive && failure.get().isInstanceOf[TimeoutException])
          val cleanup = failure.get().getSuppressed
          assert(cleanup.length == 1 && cleanup.head.getSuppressed.isEmpty)
          if (interruptCaller) {
            assert(cleanup.head.isInstanceOf[InterruptedException] && callerInterrupted.get())
          } else {
            assert(cleanup.head.isInstanceOf[ExecutionException] && !callerInterrupted.get())
          }
          intercept[CancellationException](results.hasNext)
        }
      }
    }
  }

  test("one blocking request abort cannot stall cancellation of another task") {
    val monitorEntered = new CountDownLatch(1)
    val monitorRelease = new CountDownLatch(1)
    val armed = new AtomicBoolean()
    val firstContext = new TaskContextTestSupport.ControlledContext(() => {
      if (armed.compareAndSet(true, false)) {
        monitorEntered.countDown()
        assert(monitorRelease.await(10, TimeUnit.SECONDS))
      }
    })
    val secondContext = new TaskContextTestSupport.ControlledContext(() => ())
    val abortEntered = new CountDownLatch(1)
    val abortRelease = new CountDownLatch(1)
    val firstStarted = new CountDownLatch(1)
    val secondStarted = new CountDownLatch(1)
    val secondStopped = new CountDownLatch(1)
    val workerRelease = new Semaphore(0)
    val firstWorker = new AtomicReference[Thread]()
    val secondWorker = new AtomicReference[Thread]()
    val failure = new AtomicReference[Throwable]()
    val post = new HttpPost("http://example.invalid/test") {
      override def abort(): Unit = {
        abortEntered.countDown()
        assert(abortRelease.await(10, TimeUnit.SECONDS))
        super.abort()
      }
    }
    Using.resource(VerifyBoundedRequests.client(() => {
      firstWorker.set(Thread.currentThread())
      firstStarted.countDown()
      workerRelease.acquireUninterruptibly()
      VerifyBoundedRequests.response(200, new StringEntity("released"), () => ())
    })) { http =>
      val firstHandler: HandlingUtils.HandlerFunc = (_, data) => HandlingUtils.bounded()(http, data)
      val secondHandler: HandlingUtils.HandlerFunc = (_, _) => {
        secondWorker.set(Thread.currentThread())
        secondStarted.countDown()
        try {
          new CountDownLatch(1).await()
          HTTPSchema.stringToResponse("released", 200, "OK")
        } finally secondStopped.countDown()
      }
      Using.resource(new SingleThreadedHTTPClient(firstHandler, 1000)) { first =>
        Using.resource(new SingleThreadedHTTPClient(secondHandler, 1000)) { second =>
          val request = new HTTPRequestData(new HttpPost("http://example.invalid/test"))
          val blockingRequest = new HTTPRequestData(post) {
            override def toHTTPCore: HttpPost = post
          }
          val firstResults = TaskContextTestSupport.withContext(firstContext) {
            first.sendCancellableRequestsWithContext(Iterator(first.RequestWithContext(Some(blockingRequest), None)),
              1, 30.seconds)
          }
          val secondResults = TaskContextTestSupport.withContext(secondContext) {
            second.sendCancellableRequestsWithContext(Iterator(second.RequestWithContext(Some(request), None)),
              1, 30.seconds)
          }
          val consumer = new Thread(() => {
            try firstResults.next() catch {
              case error: Throwable => failure.set(error)
            }
          })
          try {
            assert(firstResults.hasNext && secondResults.hasNext)
            assert(firstStarted.await(5, TimeUnit.SECONDS) && secondStarted.await(5, TimeUnit.SECONDS))
            armed.set(true)
            firstContext.interrupt()
            assert(monitorEntered.await(5, TimeUnit.SECONDS))
            secondContext.interrupt()
            monitorRelease.countDown()
            consumer.start()
            assert(abortEntered.await(5, TimeUnit.SECONDS))
            assert(secondStopped.await(2, TimeUnit.SECONDS), "Shared cancellation monitor waited for request abort")
            assert(consumer.isAlive, "The first scope must still be blocked in request abort")
          } finally {
            monitorRelease.countDown()
            abortRelease.countDown()
            workerRelease.release()
            consumer.join(10000)
            firstContext.complete()
            secondContext.complete()
            Seq(firstWorker, secondWorker).foreach { worker =>
              Option(worker.get()).foreach { thread =>
                thread.join(5000)
                assert(!thread.isAlive)
              }
            }
          }
          assert(!consumer.isAlive && failure.get().isInstanceOf[TaskKilledException])
        }
      }
    }
  }

  test("one draining scope cannot block cancellation of another task on the shared monitor") {
    val monitorEntered = new CountDownLatch(1)
    val monitorRelease = new CountDownLatch(1)
    val armed = new AtomicBoolean()
    val firstContext = new TaskContextTestSupport.ControlledContext(() => {
      if (armed.compareAndSet(true, false)) {
        monitorEntered.countDown()
        assert(monitorRelease.await(10, TimeUnit.SECONDS))
      }
    })
    val secondContext = new TaskContextTestSupport.ControlledContext(() => ())
    val firstStarted = new CountDownLatch(1)
    val secondStarted = new CountDownLatch(1)
    val secondStopped = new CountDownLatch(1)
    val release = new Semaphore(0)
    val firstWorker = new AtomicReference[Thread]()
    val secondWorker = new AtomicReference[Thread]()
    val failure = new AtomicReference[Throwable]()
    val firstHandler: HandlingUtils.HandlerFunc = (_, _) => {
      firstWorker.set(Thread.currentThread())
      firstStarted.countDown()
      release.acquireUninterruptibly()
      HTTPSchema.stringToResponse("released", 200, "OK")
    }
    val secondHandler: HandlingUtils.HandlerFunc = (_, _) => {
      secondWorker.set(Thread.currentThread())
      secondStarted.countDown()
      try {
        new CountDownLatch(1).await()
        HTTPSchema.stringToResponse("released", 200, "OK")
      } finally secondStopped.countDown()
    }
    Using.resource(new SingleThreadedHTTPClient(firstHandler, 1000)) { first =>
      Using.resource(new SingleThreadedHTTPClient(secondHandler, 1000)) { second =>
        val request = new HTTPRequestData(new HttpPost("http://example.invalid/test"))
        val firstResults = TaskContextTestSupport.withContext(firstContext) {
          first.sendCancellableRequestsWithContext(Iterator(first.RequestWithContext(Some(request), None)),
            1, 30.seconds)
        }
        val secondResults = TaskContextTestSupport.withContext(secondContext) {
          second.sendCancellableRequestsWithContext(Iterator(second.RequestWithContext(Some(request), None)),
            1, 30.seconds)
        }
        val consumer = new Thread(() => {
          try firstResults.next() catch {
            case error: Throwable => failure.set(error)
          }
        })
        try {
          assert(firstResults.hasNext && secondResults.hasNext)
          assert(firstStarted.await(5, TimeUnit.SECONDS) && secondStarted.await(5, TimeUnit.SECONDS))
          armed.set(true)
          firstContext.interrupt()
          assert(monitorEntered.await(5, TimeUnit.SECONDS))
          consumer.start()
          eventually(timeout(Span(5, Seconds))) {
            assert(consumer.getStackTrace.exists(frame =>
              frame.getClassName == "java.util.concurrent.ThreadPoolExecutor" &&
                frame.getMethodName == "awaitTermination"))
          }
          secondContext.interrupt()
          monitorRelease.countDown()
          assert(secondStopped.await(2, TimeUnit.SECONDS), "Shared cancellation monitor waited for another scope")
          assert(consumer.isAlive, "The first scope must still be draining its uncooperative worker")
        } finally {
          monitorRelease.countDown()
          release.release()
          consumer.join(10000)
          firstContext.complete()
          secondContext.complete()
          Seq(firstWorker, secondWorker).foreach { worker =>
            Option(worker.get()).foreach { thread =>
              thread.join(5000)
              assert(!thread.isAlive)
            }
          }
        }
        assert(!consumer.isAlive && failure.get().isInstanceOf[TaskKilledException])
      }
    }
  }

  test("nested iterator failures drain uncooperative HTTP workers only once") {
    val started = new CountDownLatch(1)
    val release = new Semaphore(0)
    val worker = new AtomicReference[Thread]()
    val original = new IllegalStateException("Synthetic input failure")
    val handler: HandlingUtils.HandlerFunc = (_, _) => {
      worker.set(Thread.currentThread())
      started.countDown()
      release.acquireUninterruptibly()
      HTTPSchema.stringToResponse("released", 200, "OK")
    }
    Using.resource(new SingleThreadedHTTPClient(handler, 1000)) { production =>
      val request = new HTTPRequestData(new HttpPost("http://example.invalid/test"))
      val first = production.RequestWithContext(Some(request), None)
      val requests = Iterator(0, 1).map { index =>
        if (index == 1) {
          assert(started.await(5, TimeUnit.SECONDS))
          throw original
        }
        first
      }
      val results = production.sendCancellableRequestsWithContext(requests, 2, 30.seconds)
      try {
        val before = System.nanoTime()
        val error = intercept[IllegalStateException](results.next())
        val elapsed = (System.nanoTime() - before).nanos
        assert(error eq original)
        assert(error.getSuppressed.length == 1)
        assert(error.getSuppressed.head.getMessage.contains("request quiescence is not confirmed"))
        assert(elapsed < 8.seconds, s"Cleanup repeated its worker drain: $elapsed")
        val retry = System.nanoTime()
        val cancelled = intercept[CancellationException](results.hasNext)
        assert(cancelled.getSuppressed.isEmpty && (System.nanoTime() - retry).nanos < 2.seconds)
      } finally {
        release.release()
        Option(worker.get()).foreach { thread =>
          thread.join(5000)
          assert(!thread.isAlive)
        }
      }
    }
  }
}
