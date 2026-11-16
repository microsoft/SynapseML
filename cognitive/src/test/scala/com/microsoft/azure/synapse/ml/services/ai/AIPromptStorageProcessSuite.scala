// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.io.http.HTTPTransformer
import org.apache.spark.sql.{DataFrame, Dataset, Encoders}
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.{Seconds, Span}

import java.io.File
import java.net.URLClassLoader
import java.nio.channels.{ClosedByInterruptException, FileLockInterruptionException}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

private[ai] object AIPromptStorageProcess {
  def mark(file: File): Unit = {
    Files.createDirectories(file.toPath.getParent)
    Files.write(file.toPath, "ready".getBytes(StandardCharsets.UTF_8))
  }

  @tailrec
  def awaitGate(gate: File): Unit = {
    if (!gate.exists()) {
      Thread.sleep(100)
      awaitGate(gate)
    }
  }

  def main(args: Array[String]): Unit = args match {
    case Array("hold-control", path, _, _, markerPath, gatePath) =>
      AIPromptLocalTransaction(new File(path).toURI.toString) {
        mark(new File(markerPath))
        awaitGate(new File(gatePath))
      }
    case _ => runSpark(args)
  }

  private def runSpark(args: Array[String]): Unit = {
    val Array(command, path, url, pauseAt, markerPath, gatePath) = args
    AIPromptDeltaTestSession.resetSparkSession(numCores = Some(2))
    val spark = AIPromptDeltaTestSession.spark
    val marker = new File(markerPath)
    val stage = if (pauseAt == "after_response") {
      new TestableAIPromptAfterResponsePause(markerPath, pause = command != "resume")
    } else new AIPrompt()
    val prompt = AIPromptStorageTestConfig.configure(stage, path, url)
    def event(boundary: String): Unit = {
      if (boundary == pauseAt) {
        mark(marker)
        if (gatePath.nonEmpty) awaitGate(new File(gatePath)) else new CountDownLatch(1).await()
      }
    }
    try {
      import spark.implicits._
      command match {
        case "create" =>
          AIPromptOutput.transformObserved(prompt, Seq("duplicate", "duplicate", "ROW_ERROR").toDF("text"), event)
        case "create-final" =>
          AIPromptOutput.transformObserved(prompt, Seq("duplicate", "duplicate").toDF("text"), event)
        case "resume" =>
          prompt.setSubscriptionKey("rotated-synthetic-key")
          AIPromptOutput.resumeObserved(prompt, spark, event)
      }
      mark(new File(markerPath + ".completed"))
      spark.stop()
    } catch {
      case error: Throwable =>
        error.printStackTrace()
        mark(new File(markerPath + ".failed"))
        spark.stop()
        System.exit(2)
    }
  }
}

/** Pauses a Spark task after an actual HTTP response, without materializing inference twice. */
private[ai] class TestableAIPromptAfterResponsePause(marker: String, pause: Boolean) extends AIPrompt {
  override private[ai] def effectiveCopy(): AIPrompt = this

  override private[ai] def infer(dataset: Dataset[_], hiddenColumns: Seq[String], http: HTTPTransformer): DataFrame = {
    val result = super.infer(dataset, hiddenColumns, http)
    val markerPath = marker
    if (!pause) result else result.map { row =>
      AIPromptStorageProcess.mark(new File(markerPath))
      new CountDownLatch(1).await()
      row
    }(Encoders.row(result.schema))
  }
}

/** Opt-in process tests require Java 9+ and forcibly terminate their own real Spark driver JVMs. */
class AIPromptStorageProcessSuite extends AIPromptOutputTestBase {
  private val driverWaitMinutes = 10
  private val processes = scala.collection.mutable.ArrayBuffer.empty[Process]

  private def enabled(): Unit = {
    assume(sys.env.get("AIPROMPT_RUN_PROCESS_TESTS").contains("true"),
      "Set AIPROMPT_RUN_PROCESS_TESTS=true to run real driver kill and acquisition race tests.")
    assert(classOf[Process].getMethods.exists(_.getName == "toHandle"),
      "Process integration tests require Java 9+ for argument files and descendant process cleanup.")
    delta()
  }

  private def root(): File = Files.createTempDirectory(tmpDir, "aiprompt-process-").toFile

  private def classpath: String = {
    @tailrec
    def urls(loader: ClassLoader, found: Seq[String]): Seq[String] = {
      Option(loader) match {
        case None => found
        case Some(current) =>
          val entries = current match {
            case paths: URLClassLoader => paths.getURLs.toSeq.filter(_.getProtocol == "file")
              .map(url => new File(url.toURI).getAbsolutePath)
            case _ => Seq.empty
          }
          urls(current.getParent, found ++ entries)
      }
    }
    sys.env.getOrElse("AIPROMPT_TEST_CLASSPATH",
      (urls(getClass.getClassLoader, Seq.empty) ++ System.getProperty("java.class.path").split(File.pathSeparator))
        .distinct.mkString(File.pathSeparator))
  }

  private def launch(command: String, run: File, server: AIPromptRecoveryRecordingServer, boundary: String,
                     marker: File, gate: Option[File] = None): Process = {
    val java = new File(System.getProperty("java.home"), "bin/java").getAbsolutePath
    val scratch = new File(marker.getParentFile, marker.getName + "-jvm")
    Files.createDirectories(scratch.toPath)
    val classpathFile = new File(scratch, "classpath.args")
    val classpathArguments = "-cp\n\"" + classpath.replace("\\", "\\\\").replace("\"", "\\\"") + "\"\n"
    Files.write(classpathFile.toPath, classpathArguments.getBytes(StandardCharsets.UTF_8))
    val process = new ProcessBuilder(java, "-Xmx768m", "-Djava.io.tmpdir=" + scratch.getAbsolutePath,
      "@" + classpathFile.getAbsolutePath,
      "com.microsoft.azure.synapse.ml.services.ai.AIPromptStorageProcess",
      command, run.getAbsolutePath, server.url, boundary, marker.getAbsolutePath,
      gate.map(_.getAbsolutePath).getOrElse(""))
      .redirectErrorStream(true).redirectOutput(new File(marker.getAbsolutePath + ".log")).start()
    processes += process
    process
  }

  private def waitForMarker(process: Process, marker: File): Unit = {
    val deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(driverWaitMinutes)
    @tailrec
    def waitUntilReady(): Unit = {
      if (!marker.exists()) {
        if (!process.isAlive) {
          val output = Files.readAllLines(new File(marker.getAbsolutePath + ".log").toPath, StandardCharsets.UTF_8)
            .asScala.takeRight(40).mkString("\n")
          fail(s"Driver exited before reaching ${marker.getName}:\n$output")
        }
        if (System.nanoTime() >= deadline) {
          stop(process)
          fail(s"Driver did not reach ${marker.getName} within $driverWaitMinutes minutes.")
        }
        Thread.sleep(100)
        waitUntilReady()
      }
    }
    waitUntilReady()
  }

  private def stop(process: Process): Unit = cleanup(Seq(() => {
    if (process.isAlive) {
      // Resolve the opt-in process API at runtime so the test sources still compile on Java 8.
      val handleClass = Class.forName("java.lang.ProcessHandle")
      val handle = classOf[Process].getMethod("toHandle").invoke(process)
      val descendants = handleClass.getMethod("descendants").invoke(handle)
        .asInstanceOf[java.util.stream.Stream[Object]]
      val destroy = handleClass.getMethod("destroyForcibly")
      try descendants.iterator().asScala.foreach(child => destroy.invoke(child)) finally descendants.close()
    }
  })) {
    if (process.isAlive) process.destroyForcibly()
    assert(process.waitFor(30, TimeUnit.SECONDS), "Test driver did not stop.")
  }

  private def finish(process: Process): Int = {
    if (!process.waitFor(driverWaitMinutes, TimeUnit.MINUTES)) {
      stop(process)
      fail("Test driver did not finish.")
    }
    process.exitValue()
  }

  private def release(run: File): Unit = {
    val state = AIPromptOutput.getOutputStatus(spark, run.getAbsolutePath)
    AIPromptOutput.releaseOutputOwner(spark, run.getAbsolutePath, state.ownerToken.get, confirmStopped = true)
  }

  private def cleanup(actions: Seq[() => Unit])(finish: => Unit): Unit = {
    val failures = scala.collection.mutable.ArrayBuffer.empty[Throwable]
    var interrupted = false
    def attempt(action: () => Unit): Unit = {
      try action() catch {
        case error: InterruptedException =>
          interrupted = true
          Thread.interrupted()
          failures += error
        case NonFatal(error) => failures += error
        case error: Throwable =>
          failures += error
          throw error
      }
    }
    try {
      try actions.foreach(attempt) finally attempt(() => finish)
      failures.headOption.foreach(throw _)
    } catch {
      case error: Throwable =>
        failures.filterNot(_ eq error).foreach(error.addSuppressed)
        throw error
    } finally {
      if (interrupted) Thread.currentThread().interrupt()
    }
  }

  override def afterAll(): Unit =
    cleanup(processes.map(process => () => stop(process)).toSeq)(super.afterAll())

  test("child cleanup attempts every driver and shared cleanup before reporting failures") {
    Seq(new IllegalStateException("driver did not stop"), new InterruptedException("interrupted wait")).foreach {
      first =>
        val second = new IllegalStateException("another driver did not stop")
        val attempted = scala.collection.mutable.ArrayBuffer.empty[String]
        val actions = Seq[() => Unit](
          () => {
            attempted += "first"
            if (first.isInstanceOf[InterruptedException]) Thread.currentThread().interrupt()
            throw first
          },
          () => {
            attempted += "second"
            assert(!Thread.currentThread().isInterrupted)
            throw second
          },
          () => { attempted += "third"; () })
        try {
          val error = intercept[Exception] {
            cleanup(actions) {
              attempted += "shared"
              assert(!Thread.currentThread().isInterrupted)
            }
          }
          assert(attempted.toSeq == Seq("first", "second", "third", "shared"))
          assert(error eq first)
          assert(error.getSuppressed.toSeq == Seq(second))
          assert(Thread.currentThread().isInterrupted == first.isInstanceOf[InterruptedException])
        } finally {
          Thread.interrupted()
        }
    }
  }

  test("shared cleanup still runs when a fatal child cleanup error escapes") {
    val failure = new LinkageError("child cleanup linkage failure")
    var finished = false
    val error = intercept[LinkageError] {
      cleanup(Seq(() => throw failure)) { finished = true }
    }
    assert((error eq failure) && finished)
  }

  test("fatal child cleanup retains prior and shared cleanup failures") {
    Seq(new IllegalStateException("shared failure"), new InterruptedException("shared interruption"),
      new LinkageError("shared linkage failure")).foreach { shared =>
      val prior = new IllegalStateException("prior driver failure")
      val fatal = new LinkageError("driver linkage failure")
      var laterDriverAttempted = false
      try {
        val error = intercept[Throwable] {
          cleanup(Seq(() => throw prior, () => throw fatal, () => { laterDriverAttempted = true })) {
            if (shared.isInstanceOf[InterruptedException]) Thread.currentThread().interrupt()
            throw shared
          }
        }
        val expected = if (shared.isInstanceOf[LinkageError]) shared else fatal
        val suppressed = if (expected eq fatal) Seq(prior, shared) else Seq(prior, fatal)
        assert(error eq expected)
        assert(error.getSuppressed.toSeq == suppressed)
        assert(!laterDriverAttempted)
        assert(Thread.currentThread().isInterrupted == shared.isInstanceOf[InterruptedException])
      } finally {
        Thread.interrupted()
      }
    }
  }

  test("administrative release can cancel a separate driver's local control lock wait") {
    enabled()
    val session = spark
    import session.implicits._
    withServer { server =>
      val directory = root()
      val run = new File(directory, "run")
      val stage = prompt(run.getAbsolutePath, server)
      intercept[IllegalStateException] {
        AIPromptOutput.transformObserved(stage, Seq("one", "two", "three").toDF("text"), event =>
          if (event == "batch_committed") throw new IllegalStateException("stop before owner release"))
      }
      val before = AIPrompt.getOutputStatus(session, run.getAbsolutePath)
      assert(before.committedCount == 2 && before.pendingCount == 1 && before.ownerToken.isDefined)
      val gate = new File(directory, "release-lock")
      val marker = new File(directory, "lock-held")
      val holder = launch("hold-control", run, server, "", marker, Some(gate))
      val pool = Executors.newFixedThreadPool(2)
      val caller = new AtomicReference[Thread]()
      val failure = new AtomicReference[Exception]()
      val interrupted = new AtomicBoolean()
      try {
        waitForMarker(holder, marker)
        val waiting = pool.submit(new Callable[Unit] {
          override def call(): Unit = {
            caller.set(Thread.currentThread())
            try {
              AIPrompt.releaseOutputOwner(session, run.getAbsolutePath, before.ownerToken.get, confirmStopped = true)
            } catch {
              case error: Exception => failure.set(error)
            } finally interrupted.set(Thread.currentThread().isInterrupted)
          }
        })
        eventually(timeout(Span(60, Seconds))) {
          val stack = Option(caller.get()).toSeq.flatMap(_.getStackTrace)
          assert(stack.exists(_.getClassName == AIPromptLocalTransaction.getClass.getName))
          assert(stack.exists(frame =>
            (frame.getClassName == "sun.nio.ch.FileChannelImpl" && frame.getMethodName == "lock") ||
              (frame.getClassName == "java.lang.Thread" && frame.getMethodName == "sleep")))
        }
        val cancel = pool.submit(new Callable[Unit] {
          override def call(): Unit = caller.get().interrupt()
        })
        cancel.get(10, TimeUnit.SECONDS)
        waiting.get(10, TimeUnit.SECONDS)
        assert(holder.isAlive && !gate.exists())
        assert(failure.get().isInstanceOf[InterruptedException] ||
          failure.get().isInstanceOf[FileLockInterruptionException] ||
          failure.get().isInstanceOf[ClosedByInterruptException])
        assert(interrupted.get())
        assert(AIPrompt.getOutputStatus(session, run.getAbsolutePath) == before)
        assert(server.requests.get() == 2)
        AIPromptStorageProcess.mark(gate)
        assert(finish(holder) == 0)
        AIPrompt.releaseOutputOwner(session, run.getAbsolutePath, before.ownerToken.get, confirmStopped = true)
        assert(stage.resume(session).count() == 3 && server.requests.get() == 3)
        val complete = AIPrompt.getOutputStatus(session, run.getAbsolutePath)
        assert(complete.allSucceeded && complete.ownerToken.isEmpty)
      } finally {
        cleanup(Seq(
          () => { AIPromptStorageProcess.mark(gate); stop(holder) },
          () => {
            pool.shutdownNow()
            assert(pool.awaitTermination(30, TimeUnit.SECONDS))
          })) { () }
      }
    }
  }

  test("driver death at every initialization write never fabricates a resumable run") {
    enabled()
    val server = new AIPromptRecoveryRecordingServer
    try {
      Seq("control_created", "input_committed", "results_created").foreach { boundary =>
        val directory = root()
        val run = new File(directory, "run")
        val marker = new File(directory, "paused")
        val process = launch("create", run, server, boundary, marker)
        waitForMarker(process, marker)
        stop(process)
        val error = intercept[IllegalArgumentException] {
          AIPromptOutput.loadOutput(spark, run.getAbsolutePath)
        }
        assert(error.getMessage.contains("initialization is incomplete"))
      }
      assert(server.requests.get() == 0)
    } finally server.close()
  }

  test("killed driver recovery skips committed batches and final lost acknowledgements") {
    enabled()
    val server = new AIPromptRecoveryRecordingServer
    try {
      Seq("create", "create-final").foreach { command =>
        val directory = root()
        val run = new File(directory, "run")
        val marker = new File(directory, "paused")
        val before = server.requests.get()
        val process = launch(command, run, server, "batch_committed", marker)
        waitForMarker(process, marker)
        stop(process)
        val state = AIPromptOutput.getOutputStatus(spark, run.getAbsolutePath)
        assert(state.committedCount == 2 && state.state != "complete")
        assert(server.requests.get() == before + 2)
        assert(AIPromptOutput.loadOutput(spark, run.getAbsolutePath).count() == 2)
        release(run)
        val resumed = launch("resume", run, server, "", new File(directory, "resumed"))
        assert(finish(resumed) == 0)
        val total = if (command == "create") 3 else 2
        assert(AIPromptOutput.loadOutput(spark, run.getAbsolutePath).count() == total)
        assert(server.requests.get() == before + total)
      }
    } finally server.close()
  }

  test("driver death after paid responses and before commit permits replay of only uncommitted input") {
    enabled()
    val server = new AIPromptRecoveryRecordingServer
    try {
      val directory = root()
      val run = new File(directory, "run")
      val marker = new File(directory, "response")
      val process = launch("create", run, server, "after_response", marker)
      waitForMarker(process, marker)
      stop(process)
      assert(server.requests.get() > 0)
      assert(AIPromptOutput.getOutputStatus(spark, run.getAbsolutePath).committedCount == 0)
      release(run)
      val paid = server.requests.get()
      assert(finish(launch("resume", run, server, "after_response", new File(directory, "resumed"))) == 0)
      assert(server.requests.get() == paid + 3)
      val status = AIPrompt.getOutputStatus(spark, run.getAbsolutePath)
      assert(status.isComplete && status.committedCount == 3 && status.ownerToken.isEmpty)
    } finally server.close()
  }

  test("separate drivers racing first creation and later acquisition have one inferencing winner") {
    enabled()
    val server = new AIPromptRecoveryRecordingServer
    try {
      Seq("create", "resume").foreach { command =>
        val directory = root()
        val run = new File(directory, "run")
        if (command == "resume") {
          val ready = new File(directory, "ready")
          val initializer = launch("create", run, server, "ready", ready)
          waitForMarker(initializer, ready)
          stop(initializer)
          release(run)
        }
        val gate = new File(directory, "gate")
        val left = new File(directory, "left")
        val right = new File(directory, "right")
        val boundary = if (command == "create") "before_control_create" else "before_acquire"
        val one = launch(command, run, server, boundary, left, Some(gate))
        val two = launch(command, run, server, boundary, right, Some(gate))
        waitForMarker(one, left)
        waitForMarker(two, right)
        val before = server.requests.get()
        AIPromptStorageProcess.mark(gate)
        val outcomes = Seq(finish(one), finish(two))
        assert(outcomes.count(_ == 0) == 1 && outcomes.count(_ != 0) == 1)
        assert(server.requests.get() == before + 3)
        val status = AIPromptOutput.getOutputStatus(spark, run.getAbsolutePath)
        assert(status.isComplete && status.ownerToken.isEmpty)
      }
    } finally server.close()
  }
}
