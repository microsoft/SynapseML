// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import org.scalatest.concurrent.Eventually._
import org.scalatest.time.{Seconds, Span}
import spray.json._

import java.io.File
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

class AIPromptOutputCancellationSuite extends AIPromptOutputTestBase {
  Seq("one", "ROW_ERROR").foreach { text =>
    test(s"last-batch outcome flags do not imply owner release for $text") {
      delta()
      val session = spark
      import session.implicits._
      withServer { server =>
        val root = path()
        val stage = prompt(root, server)
        val stopped = new IllegalStateException("stop after last batch commit")
        val failure = intercept[IllegalStateException] {
          AIPromptOutput.transformObserved(stage, Seq(text).toDF("text"), event =>
            if (event == "batch_committed") throw stopped)
        }
        assert(failure eq stopped)
        val status = AIPrompt.getOutputStatus(session, root)
        val succeeds = text != "ROW_ERROR"
        assert(status.inputCount == 1 && status.committedCount == 1 && status.pendingCount == 0)
        assert(status.rowErrorCount == (if (succeeds) 0L else 1L))
        assert(status.isComplete && status.allSucceeded == succeeds)
        assert(status.state == "running" && status.ownerToken.isDefined)
        val json = AIPrompt.getOutputStatusJson(session, root).parseJson.asJsObject.fields
        assert(json("isComplete") == JsBoolean(true) && json("allSucceeded") == JsBoolean(succeeds))
        assert(json("state") == JsString("running") && json("ownerToken") == JsString(status.ownerToken.get))
        val committed = AIPrompt.loadOutput(session, root).collect().toSeq
        assert(committed.size == 1 && server.requests.get() == 1)
        intercept[IllegalArgumentException](stage.resume(session))
        assert(AIPrompt.getOutputStatus(session, root) == status && server.requests.get() == 1)
        AIPrompt.releaseOutputOwner(session, root, status.ownerToken.get, confirmStopped = true)
        assert(stage.resume(session).collect().toSeq == committed)
        val complete = AIPrompt.getOutputStatus(session, root)
        assert(complete == status.copy(state = "complete", ownerToken = None))
        assert(complete.isComplete && complete.allSucceeded == succeeds && server.requests.get() == 1)
      }
    }
  }

  test("durable preparation errors survive interrupted ownership release") {
    delta()
    val session = spark.newSession()
    import session.implicits._
    withServer { server =>
      val root = path()
      val stage = prompt(root, server).setPromptTemplate("{lower(text)}")
      val data = Seq("ONE", "TWO", "THREE").toDF("text")
      val startHolder = new CountDownLatch(1)
      val holding = new CountDownLatch(1)
      val release = new CountDownLatch(1)
      val driver = new AtomicReference[Thread]()
      val failure = new AtomicReference[Exception]()
      val interrupted = new AtomicBoolean()
      val pool = Executors.newFixedThreadPool(2)
      try {
        val holder = pool.submit(new Callable[Unit] {
          override def call(): Unit = {
            assert(startHolder.await(120, TimeUnit.SECONDS))
            AIPromptLocalTransaction(AIPromptDelta.qualify(session, root)) {
              holding.countDown()
              assert(release.await(120, TimeUnit.SECONDS))
            }
          }
        })
        val run = pool.submit(new Callable[Unit] {
          override def call(): Unit = {
            driver.set(Thread.currentThread())
            try {
              AIPromptOutput.transformObserved(stage, data, event => if (event == "batch_committed") {
                session.udf.register("lower", (text: String) => "changed:" + text)
                startHolder.countDown()
                assert(holding.await(10, TimeUnit.SECONDS))
              })
            } catch {
              case error: Exception => failure.set(error)
            } finally interrupted.set(Thread.currentThread().isInterrupted)
          }
        })
        assert(holding.await(120, TimeUnit.SECONDS))
        eventually(timeout(Span(60, Seconds))) {
          assert(driver.get().getStackTrace.exists(frame =>
            frame.getClassName == "java.util.concurrent.locks.ReentrantLock" &&
              frame.getMethodName == "lockInterruptibly"))
        }
        driver.get().interrupt()
        run.get(10, TimeUnit.SECONDS)
        assert(failure.get().isInstanceOf[IllegalArgumentException])
        assert(failure.get().getMessage.contains("stable built-in expressions"))
        assert(failure.get().getSuppressed.exists(_.isInstanceOf[InterruptedException]))
        assert(interrupted.get())
        val status = AIPrompt.getOutputStatus(session, root)
        assert(status.committedCount == 2 && status.pendingCount == 1 && status.ownerToken.isDefined)
        assert(server.requests.get() == 2)
        release.countDown()
        holder.get(10, TimeUnit.SECONDS)
        AIPrompt.releaseOutputOwner(session, root, status.ownerToken.get, confirmStopped = true)
        assert(AIPrompt.getOutputStatus(session, root).ownerToken.isEmpty)
      } finally {
        startHolder.countDown()
        release.countDown()
        pool.shutdownNow()
        assert(pool.awaitTermination(30, TimeUnit.SECONDS))
      }
    }
  }

  test("local control interruption preserves the caller flag and does not leak the lock") {
    val root = new File(path()).toPath
    val holding = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val caller = new AtomicReference[Thread]()
    val entered = new AtomicBoolean()
    val pool = Executors.newFixedThreadPool(2)
    try {
      val holder = pool.submit(new Callable[Unit] {
        override def call(): Unit = AIPromptLocalTransaction(root.toUri.toString) {
          holding.countDown()
          assert(release.await(30, TimeUnit.SECONDS))
        }
      })
      assert(holding.await(10, TimeUnit.SECONDS))
      val waiting = pool.submit(new Callable[Boolean] {
        override def call(): Boolean = {
          caller.set(Thread.currentThread())
          try {
            AIPromptLocalTransaction(root.toUri.toString) { entered.set(true) }
            false
          } catch {
            case _: InterruptedException => Thread.currentThread().isInterrupted
          }
        }
      })
      eventually(timeout(Span(10, Seconds))) {
        assert(Option(caller.get()).exists(_.getStackTrace.exists(frame =>
          frame.getClassName == "java.util.concurrent.locks.ReentrantLock" &&
            frame.getMethodName == "lockInterruptibly")))
      }
      caller.get().interrupt()
      assert(waiting.get(10, TimeUnit.SECONDS))
      assert(!entered.get())
      release.countDown()
      holder.get(10, TimeUnit.SECONDS)
      assert(AIPromptLocalTransaction(root.toUri.toString) { 3 } == 3)
    } finally {
      release.countDown()
      pool.shutdownNow()
      assert(pool.awaitTermination(10, TimeUnit.SECONDS))
    }
  }
}
