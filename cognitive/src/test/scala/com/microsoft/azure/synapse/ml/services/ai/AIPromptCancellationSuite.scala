// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.{TaskContext, TaskKilledException}
import org.apache.spark.injections.TaskContextUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StructType

import java.util.concurrent.{Callable, ConcurrentHashMap, CountDownLatch, ExecutionException, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.collection.JavaConverters._
import scala.util.Using

private[ai] object AICancellationGates {
  class Gate {
    val entered = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val completed = new CountDownLatch(1)
    val context = new AtomicReference[TaskContext]()
    val failure = new AtomicReference[Throwable]()
    val completionInterrupted = new AtomicBoolean()
  }

  val Active = new ConcurrentHashMap[String, Gate]()

  def block(id: String, answer: String): String = {
    val gate = Active.get(id)
    val context = TaskContext.get()
    gate.context.set(context)
    context.addTaskFailureListener((_: TaskContext, error: Throwable) => gate.failure.set(error))
    context.addTaskCompletionListener[Unit] { _ =>
      gate.completionInterrupted.set(Thread.currentThread().isInterrupted)
      gate.completed.countDown()
    }
    gate.entered.countDown()
    require(gate.release.await(30, TimeUnit.SECONDS), "Downstream cancellation test gate timed out")
    answer
  }
}

class AIPromptCancellationSuite extends TestBase {
  Seq(false, true).foreach { activeRequest =>
    test(s"Spark cancellation drains workers with blocked downstream and active request=$activeRequest") {
      val received = new CountDownLatch(1)
      val releaseResponse = new CountDownLatch(1)
      val reply = """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""
      Using.resource(new AIRecordingServer((_, body) => {
        if (body.contains("retry")) {
          received.countDown()
          if (activeRequest) {
            assert(releaseResponse.await(30, TimeUnit.SECONDS))
            200 -> reply
          } else 429 -> "{}"
        } else 200 -> reply
      }, retryAfter = "5")) { server =>
        val data = spark.createDataFrame(Seq(Row("ready"), Row("retry")).asJava,
          new StructType().add("text", "string")).coalesce(1)
        val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setInputCol("text")
          .setConcurrency(2).setTimeout(30).setOutputCol("answer")
        val context = spark.sparkContext
        val group = java.util.UUID.randomUUID().toString
        val gate = new AICancellationGates.Gate()
        AICancellationGates.Active.put(group, gate)
        val block = udf((answer: String) => AICancellationGates.block(group, answer))
        val result = stage.transform(data).select(block(col("answer")))
        val executor = Executors.newSingleThreadExecutor()
        val job = executor.submit(new Callable[Unit] {
          override def call(): Unit = {
            context.setJobGroup(group, "AIPrompt blocked downstream cancellation", interruptOnCancel = false)
            try result.collect() finally context.clearJobGroup()
          }
        })
        def workers: Iterable[Thread] = Thread.getAllStackTraces.keySet().asScala
          .filter(thread => thread.isAlive && thread.getName.startsWith("synapseml-bounded-http-"))
        try {
          assert(gate.entered.await(15, TimeUnit.SECONDS), "The first response did not block downstream")
          assert(received.await(15, TimeUnit.SECONDS), "The concurrent request never reached the server")
          if (!activeRequest) {
            val sleeping = System.nanoTime() + TimeUnit.SECONDS.toNanos(3)
            def inBackoff: Boolean = workers.exists(_.getStackTrace.exists(frame =>
              frame.getClassName == "java.lang.Thread" && frame.getMethodName == "sleep"))
            Iterator.continually(inBackoff)
              .takeWhile(ready => !ready && System.nanoTime() < sleeping).foreach(_ => Thread.sleep(10))
            assert(inBackoff, "The retry worker never entered backoff")
          }
          context.cancelJobGroup(group)
          val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
          Iterator.continually(gate.context.get().isInterrupted())
            .takeWhile(interrupted => !interrupted && System.nanoTime() < deadline).foreach(_ => Thread.sleep(10))
          assert(gate.context.get().isInterrupted(), "Spark did not deliver task cancellation")
          val drained = System.nanoTime() + TimeUnit.SECONDS.toNanos(3)
          Iterator.continually(workers.nonEmpty)
            .takeWhile(running => running && System.nanoTime() < drained).foreach(_ => Thread.sleep(10))
          assert(workers.isEmpty, "Canceled HTTP workers outlived cancellation while downstream was blocked")
          assert(!gate.context.get().isCompleted(), "Task completion incorrectly supplied the cancellation signal")
          Thread.sleep(6000)
          assert(server.requests.size() == 2, "A worker retried after task cancellation")
          gate.release.countDown()
          intercept[ExecutionException](job.get(15, TimeUnit.SECONDS))
          assert(gate.completed.await(15, TimeUnit.SECONDS))
          assert(!gate.completionInterrupted.get(), "Task-only cancellation interrupted executor cleanup")
          val killed = Iterator.iterate(gate.failure.get())(_.getCause).takeWhile(_ != null)
            .collectFirst { case error: TaskKilledException => error }
          val reason = TaskContextUtils.getKillReason(gate.context.get())
          assert(reason.nonEmpty && killed.exists(error => reason.contains(error.reason)))
        } finally {
          releaseResponse.countDown()
          gate.release.countDown()
          context.cancelJobGroup(group)
          job.cancel(true)
          executor.shutdownNow()
          assert(executor.awaitTermination(15, TimeUnit.SECONDS))
          AICancellationGates.Active.remove(group)
        }
      }
    }
  }
}
