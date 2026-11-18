// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.env.{NativeLoader, StreamUtilities}
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.commons.io.IOUtils
import org.apache.spark.ml.linalg.Vectors
import org.vowpalwabbit.spark.VowpalWabbitNative

import java.io.{File, FileOutputStream}
import java.net.URLClassLoader
import java.util.jar.{JarEntry, JarOutputStream}

class VowpalWabbitArmNativeSuite extends TestBase {
  private def withArchitecture(arch: String)(body: => Unit): Unit = {
    val originalArch = System.getProperty("os.arch")
    try {
      System.setProperty("os.arch", arch)
      body
    } finally {
      System.setProperty("os.arch", originalArch)
    }
  }

  test("ARM native failures remain actionable after JNI initialization fails") {
    for (arch <- Seq("aarch64", "arm64")) {
      withArchitecture(arch) {
        val failures = Seq(
          new UnsatisfiedLinkError("missing native library"),
          new ExceptionInInitializerError(new UnsatisfiedLinkError("wrong native architecture")),
          new NoClassDefFoundError("Could not initialize class common.Native"))
        failures.foreach { failure =>
          val error = intercept[UnsupportedOperationException] {
            VowpalWabbitNativeSupport.withNativeSupport(throw failure)
          }
          assert(error.getMessage.contains("Vowpal Wabbit is not supported on ARM"))
          assert(error.getMessage.contains("x64 compute"))
          assert(error.getCause eq failure)
        }
      }
    }
  }

  test("ARM handling does not disguise invalid native arguments") {
    withArchitecture("aarch64") {
      val invalidArgs = new IllegalArgumentException("invalid arguments")
      val error = intercept[IllegalArgumentException] {
        VowpalWabbitNativeSupport.withNativeSupport(throw invalidArgs)
      }
      assert(error eq invalidArgs)
    }
  }

  test("a real failed VW initializer remains actionable on repeated ARM calls") {
    val jarFile = File.createTempFile("vw-missing-native", ".jar")
    try {
      StreamUtilities.using(new JarOutputStream(new FileOutputStream(jarFile))) { output =>
        output.putNextEntry(new JarEntry("common/Native.class"))
        StreamUtilities.using(classOf[VowpalWabbitNative].getResourceAsStream("/common/Native.class")) { input =>
          IOUtils.copy(input, output)
        }.get
        output.closeEntry()
      }.get
      val loader = new URLClassLoader(Array(jarFile.toURI.toURL), ClassLoader.getPlatformClassLoader) {
        override protected def findLibrary(name: String): String = jarFile.getAbsolutePath + ".missing"
      }
      StreamUtilities.using(loader) { isolated =>
        val nativeClass = Class.forName("common.Native", false, isolated)
        withArchitecture("aarch64") {
          val first = intercept[UnsupportedOperationException] {
            VowpalWabbitNativeSupport.withNativeSupport(nativeClass.getMethod("load").invoke(nativeClass))
          }
          assert(first.getCause.isInstanceOf[ExceptionInInitializerError])
          val repeated = intercept[UnsupportedOperationException] {
            VowpalWabbitNativeSupport.withNativeSupport(nativeClass.getMethod("load").invoke(nativeClass))
          }
          assert(repeated.getCause.isInstanceOf[NoClassDefFoundError])
          assert(repeated.getMessage.contains("x64 compute"))
        }
      }.get
    } finally {
      jarFile.delete()
    }
  }

  test("x64 native failures retain their original type and cause") {
    withArchitecture("amd64") {
      val failure = new UnsatisfiedLinkError("missing native library")
      val error = intercept[UnsatisfiedLinkError] {
        VowpalWabbitNativeSupport.withNativeSupport(throw failure)
      }
      assert(error eq failure)
    }
  }

  test("ARM detection does not block a working host native library") {
    assume(!NativeLoader.isArm64, "This test simulates ARM detection with the host's x64 native library")
    import spark.implicits._
    val data = Seq((0.0, Vectors.dense(0.0)), (1.0, Vectors.dense(1.0))).toDF("label", "features")
    val progressiveData = Seq("1 |a 1:1").toDF("input")
    withArchitecture("aarch64") {
      val model = new VowpalWabbitRegressor().fit(data.repartition(1))
      StreamUtilities.using(model.vw) { _ =>
        assert(model.transform(data).select("prediction").collect().length == 2)
        assert(model.getReadableModel.nonEmpty)
      }.get
      assert(new VowpalWabbitGenericProgressive().transform(progressiveData.repartition(1)).collect().length == 1)
    }
  }
}
