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
import java.nio.file.Files
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}
import java.util.jar.{Attributes, JarOutputStream, Manifest}
import scala.util.Using

object GlobalParamsConcurrencyProbe {
  private val Workers = 8
  private val Entries = 2000
  private val Rounds = 4
  private case object SharedKey extends GlobalKey[String]
  private case class WorkerKey(value: Int) extends GlobalKey[Int]

  def main(args: Array[String]): Unit = {
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
              if (args.head == "registration") {
                val params = (0 until Entries).map { index =>
                  new Param[String](s"concurrent-${offset + index}", "value", "concurrent registration")
                }
                params.foreach(GlobalParams.registerParam(_, SharedKey))
                params.foreach { param =>
                  require(GlobalParams.getParam(param).contains("shared"), s"Lost registration: ${param.parent}")
                }
              } else {
                val keys = (0 until Entries).map(index => WorkerKey(offset + index))
                keys.foreach(key => GlobalParams.setGlobalParam(key, key.value))
                keys.foreach { key =>
                  require(GlobalParams.getGlobalParam(key).contains(key.value), s"Lost global value: ${key.value}")
                }
                keys.foreach { key =>
                  GlobalParams.resetGlobalParam(key)
                  require(GlobalParams.getGlobalParam(key).isEmpty, s"Reset failed: ${key.value}")
                }
              }
            }
          })
        }
        start.countDown()
        work.foreach(_.get(30, TimeUnit.SECONDS))
      }
      println("GLOBAL_PARAMS_CONCURRENCY_OK")
    } finally {
      executor.shutdownNow()
    }
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
    GlobalParams.setGlobalParam(TestStringKey, "global-value")

    val result = GlobalParams.getParam(params.testParam)
    assert(result === Some("global-value"))
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

  Seq("registration", "values").foreach { mode =>
    test(s"concurrent global $mode completes without losing entries in an isolated JVM") {
      val loaderPaths = Iterator.iterate(getClass.getClassLoader)(_.getParent)
        .takeWhile(_ != null)
        .collect { case loader: URLClassLoader => loader }
        .flatMap(_.getURLs.iterator).filter(_.getProtocol == "file")
        .map(url => new File(url.toURI)).toVector
      val systemPaths = System.getProperty("java.class.path").split(File.pathSeparator).map(new File(_))
      val paths = (loaderPaths ++ systemPaths).map(_.getCanonicalFile).distinct
      val manifest = new Manifest()
      manifest.getMainAttributes.put(Attributes.Name.MANIFEST_VERSION, "1.0")
      manifest.getMainAttributes.put(Attributes.Name.CLASS_PATH, paths.map(_.toURI.toASCIIString).mkString(" "))
      val classpath = tmpDir.resolve(s"global-$mode-classpath.jar").toFile
      Using.resource(new JarOutputStream(new FileOutputStream(classpath), manifest))(_ => ())
      val log = tmpDir.resolve(s"global-$mode.log")
      val java = new File(System.getProperty("java.home"), "bin/java").getAbsolutePath
      val probe = GlobalParamsConcurrencyProbe.getClass.getName.stripSuffix("$")
      val process = new ProcessBuilder(java, "-cp", classpath.getAbsolutePath, probe, mode)
        .redirectErrorStream(true).redirectOutput(log.toFile).start()
      val completed = try process.waitFor(45, TimeUnit.SECONDS) finally {
        if (process.isAlive) {
          process.destroyForcibly()
          assert(process.waitFor(10, TimeUnit.SECONDS), "GlobalParams probe did not terminate")
        }
      }
      val output = new String(Files.readAllBytes(log), StandardCharsets.UTF_8)
      assert(completed, s"GlobalParams concurrency probe timed out:\n$output")
      assert(process.exitValue() == 0, output)
      assert(output.contains("GLOBAL_PARAMS_CONCURRENCY_OK"), output)
    }
  }
}
