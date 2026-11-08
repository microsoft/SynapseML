// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.scalatest.TestData

import java.io.{File, FileOutputStream}
import java.net.URLClassLoader
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}
import java.util.jar.{Attributes, JarOutputStream, Manifest}
import scala.util.Using

object GlobalParamsConcurrencyProbe {
  private val Workers = 8
  private val Entries = 2000
  private val Rounds = 4
  private[param] val TotalEntries = Workers * Entries * Rounds
  private case object SharedKey extends GlobalKey[String]
  private case class WorkerKey(value: Int) extends GlobalKey[Int]

  private def registrationParam(index: Int): Param[String] =
    new Param[String](s"concurrent-$index", "value", "concurrent registration")

  private def exerciseValues(offset: Int): Unit = {
    val entries = (0 until Entries).map { index =>
      val key = WorkerKey(offset + index)
      val param = new Param[Int](s"concurrent-value-${key.value}", "value", "concurrent value")
      GlobalParams.registerParam(param, key)
      require(GlobalParams.getParam(param).isEmpty, s"Unexpected initial value: ${key.value}")
      key -> param
    }
    entries.foreach { case (key, _) => GlobalParams.setGlobalParam(key, key.value) }
    entries.foreach { case (key, param) =>
      require(GlobalParams.getGlobalParam(key).contains(key.value), s"Lost global value: ${key.value}")
      require(GlobalParams.getParam(param).contains(key.value), s"Lost parameter value: ${key.value}")
    }
    entries.foreach { case (key, param) =>
      GlobalParams.resetGlobalParam(key)
      require(GlobalParams.getGlobalParam(key).isEmpty, s"Reset failed: ${key.value}")
      require(GlobalParams.getParam(param).isEmpty, s"Parameter reset failed: ${key.value}")
    }
  }

  private def verifyCompletedRound(registration: Boolean, round: Int): Unit = {
    (0 until (round + 1) * Workers * Entries).foreach { index =>
      if (registration) {
        require(GlobalParams.getParam(registrationParam(index)).contains("shared"), s"Lost registration: $index")
      } else {
        require(GlobalParams.getGlobalParam(WorkerKey(index)).isEmpty, s"Reset did not persist: $index")
      }
    }
  }

  def main(args: Array[String]): Unit = {
    require(args.length == 1 && Set("registration", "values").contains(args.head),
      "Expected exactly one mode: registration or values")
    val registration = args.head == "registration"
    val executor = Executors.newFixedThreadPool(Workers)
    GlobalParams.setGlobalParam(SharedKey, "shared")
    try {
      (0 until Rounds).foreach { round =>
        val start = new CountDownLatch(1)
        val work = (0 until Workers).map { worker =>
          executor.submit(new Callable[Unit] {
            override def call(): Unit = {
              start.await()
              val offset = (round * Workers + worker) * Entries
              if (registration) {
                val params = (0 until Entries).map(index => registrationParam(offset + index))
                params.foreach(GlobalParams.registerParam(_, SharedKey))
                params.foreach { param =>
                  require(GlobalParams.getParam(param).contains("shared"), s"Lost registration: ${param.parent}")
                }
              } else {
                exerciseValues(offset)
              }
            }
          })
        }
        start.countDown()
        work.foreach(_.get(30, TimeUnit.SECONDS))
        verifyCompletedRound(registration, round)
      }
      println(s"GLOBAL_PARAMS_CONCURRENCY_OK mode=${args.head} verified=$TotalEntries")
    } finally {
      executor.shutdownNow()
    }
  }
}

object GlobalParamsBlockingProbe {
  def main(args: Array[String]): Unit = {
    System.in.read()
    ()
  }
}

class VerifyGlobalParams extends TestBase {

  // Test keys
  case object TestStringKey extends GlobalKey[String]
  case object TestIntKey extends GlobalKey[Int]
  case object AnotherStringKey extends GlobalKey[String]

  // GlobalParams is process-wide mutable state, so every test must start from a clean slate
  // and must not leak keys into suites that share the forked JVM.
  private def resetGlobalState(): Unit = {
    GlobalParams.resetGlobalParam(TestStringKey)
    GlobalParams.resetGlobalParam(TestIntKey)
    GlobalParams.resetGlobalParam(AnotherStringKey)
  }

  protected override def beforeEach(td: TestData): Unit = {
    super.beforeEach(td)
    resetGlobalState()
  }

  protected override def afterEach(td: TestData): Unit = {
    resetGlobalState()
    super.afterEach(td)
  }

  test("setGlobalParam and getGlobalParam work for String") {
    GlobalParams.setGlobalParam(TestStringKey, "test-value")
    val result = GlobalParams.getGlobalParam(TestStringKey)
    assert(result === Some("test-value"))
  }

  test("setGlobalParam and getGlobalParam work for Int") {
    GlobalParams.setGlobalParam(TestIntKey, 42)
    val result = GlobalParams.getGlobalParam(TestIntKey)
    assert(result === Some(42))
  }

  test("getGlobalParam returns None for unset key") {
    val result = GlobalParams.getGlobalParam(TestStringKey)
    assert(result.isEmpty)
  }

  test("resetGlobalParam removes the parameter") {
    GlobalParams.setGlobalParam(TestStringKey, "value")
    assert(GlobalParams.getGlobalParam(TestStringKey).isDefined)
    GlobalParams.resetGlobalParam(TestStringKey)
    assert(GlobalParams.getGlobalParam(TestStringKey).isEmpty)
  }

  test("setGlobalParam overwrites existing value") {
    GlobalParams.setGlobalParam(TestStringKey, "first")
    GlobalParams.setGlobalParam(TestStringKey, "second")
    assert(GlobalParams.getGlobalParam(TestStringKey) === Some("second"))
  }

  test("multiple keys can be set independently") {
    GlobalParams.setGlobalParam(TestStringKey, "string-value")
    GlobalParams.setGlobalParam(TestIntKey, 100)
    GlobalParams.setGlobalParam(AnotherStringKey, "another-value")

    assert(GlobalParams.getGlobalParam(TestStringKey) === Some("string-value"))
    assert(GlobalParams.getGlobalParam(TestIntKey) === Some(100))
    assert(GlobalParams.getGlobalParam(AnotherStringKey) === Some("another-value"))
  }

  test("registerParam and getParam work together") {
    // Create a test Params implementation
    class TestParams(override val uid: String) extends Params {
      val testParam = new Param[String](this, "testParam", "test param")
      override def copy(extra: ParamMap): Params = this
    }

    val params = new TestParams(Identifiable.randomUID("test"))
    GlobalParams.registerParam(params.testParam, TestStringKey)
    assert(GlobalParams.getParam(params.testParam).isEmpty)
    GlobalParams.setGlobalParam(TestStringKey, "global-value")

    val result = GlobalParams.getParam(params.testParam)
    assert(result === Some("global-value"))
    GlobalParams.resetGlobalParam(TestStringKey)
    assert(GlobalParams.getParam(params.testParam).isEmpty)
  }

  test("getParam returns None for unregistered param") {
    class TestParams(override val uid: String) extends Params {
      val unregisteredParam = new Param[String](this, "unregisteredParam", "not registered")
      override def copy(extra: ParamMap): Params = this
    }

    val params = new TestParams(Identifiable.randomUID("test"))
    val result = GlobalParams.getParam(params.unregisteredParam)
    assert(result.isEmpty)
  }

  test("global values preserve a present null until reset") {
    val value = Option.empty[String].orNull
    GlobalParams.setGlobalParam(TestStringKey, value)
    assert(GlobalParams.getGlobalParam(TestStringKey) == Some(value))
    GlobalParams.resetGlobalParam(TestStringKey)
    assert(GlobalParams.getGlobalParam(TestStringKey).isEmpty)
  }

  test("concurrency probe rejects missing, unknown, and extra mode arguments") {
    Seq(Array.empty[String], Array("unknown"), Array("values", "extra")).foreach { args =>
      val error = intercept[IllegalArgumentException] {
        GlobalParamsConcurrencyProbe.main(args)
      }
      assert(error.getMessage.contains("Expected exactly one mode: registration or values"))
    }
  }

  private def startProbe(probe: String, mode: String): (Process, Path) = {
    val loaderPaths = Iterator.iterate(getClass.getClassLoader)(_.getParent)
      .takeWhile(_ != null)
      .collect { case loader: URLClassLoader => loader }
      .flatMap(_.getURLs.iterator).filter(_.getProtocol == "file")
      .map(url => new File(url.toURI)).toVector
    val systemPaths = System.getProperty("java.class.path").split(File.pathSeparator).map(new File(_))
    val paths = (loaderPaths ++ systemPaths).map(_.getCanonicalFile).distinct
    // Java 8 lacks @argfile support; a manifest avoids Windows command-line length limits.
    val manifest = new Manifest()
    manifest.getMainAttributes.put(Attributes.Name.MANIFEST_VERSION, "1.0")
    manifest.getMainAttributes.put(Attributes.Name.CLASS_PATH, paths.map(_.toURI.toASCIIString).mkString(" "))
    val classpath = tmpDir.resolve(s"global-$mode-classpath.jar").toFile
    Using.resource(new FileOutputStream(classpath)) { output =>
      Using.resource(new JarOutputStream(output, manifest))(_ => ())
    }
    val log = tmpDir.resolve(s"global-$mode.log")
    val java = new File(System.getProperty("java.home"), "bin/java").getAbsolutePath
    val process = new ProcessBuilder(java, "-cp", classpath.getAbsolutePath, probe, mode)
      .redirectErrorStream(true).redirectOutput(log.toFile).start()
    process -> log
  }

  private def awaitProbe(process: Process, timeout: Long, unit: TimeUnit): (Boolean, Boolean) = {
    var terminated = false
    // Finish bounded child cleanup before restoring the caller's interrupt flag.
    val completed = try {
      Using.resource(new AutoCloseable {
        override def close(): Unit = {
          if (process.isAlive) {
            process.destroyForcibly()
          }
          terminated = process.waitFor(10, TimeUnit.SECONDS)
        }
      })(_ => process.waitFor(timeout, unit))
    } catch {
      case error: InterruptedException =>
        Thread.currentThread().interrupt()
        throw error
    }
    completed -> terminated
  }

  Seq(false, true).foreach { interrupt =>
    val mode = if (interrupt) "interrupt" else "timeout"
    test(s"blocked probe cleanup is bounded and preserves $mode outcome") {
      val (process, _) = startProbe(GlobalParamsBlockingProbe.getClass.getName.stripSuffix("$"), mode)
      val started = System.nanoTime()
      try {
        assert(process.isAlive)
        if (interrupt) {
          Thread.currentThread().interrupt()
          intercept[InterruptedException] {
            awaitProbe(process, 100, TimeUnit.MILLISECONDS)
          }
          assert(Thread.currentThread().isInterrupted)
        } else {
          val (completed, terminated) = awaitProbe(process, 100, TimeUnit.MILLISECONDS)
          assert(!completed)
          assert(terminated)
        }
        assert(!process.isAlive)
        assert(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(15))
      } finally {
        Thread.interrupted()
        if (process.isAlive) {
          process.destroyForcibly()
          assert(process.waitFor(10, TimeUnit.SECONDS), "Blocked test probe did not terminate")
        }
      }
    }
  }

  // Isolate permanent registrations and possible map corruption or hangs from the shared test JVM.
  Seq("registration", "values").foreach { mode =>
    test(s"concurrent global $mode completes without losing entries in an isolated JVM") {
      val probe = GlobalParamsConcurrencyProbe.getClass.getName.stripSuffix("$")
      val (process, log) = startProbe(probe, mode)
      val (completed, terminated) = awaitProbe(process, 45, TimeUnit.SECONDS)
      val output = new String(Files.readAllBytes(log), StandardCharsets.UTF_8)
      assert(terminated, s"GlobalParams probe did not terminate:\n$output")
      assert(completed, s"GlobalParams concurrency probe timed out:\n$output")
      assert(process.exitValue() == 0, output)
      assert(output.contains(
        s"GLOBAL_PARAMS_CONCURRENCY_OK mode=$mode verified=${GlobalParamsConcurrencyProbe.TotalEntries}"), output)
    }
  }
}
