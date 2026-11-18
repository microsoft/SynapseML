// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.env

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

class NativeLoaderSuite extends TestBase {
  private def withPlatform(os: String, arch: String)(body: => Unit): Unit = {
    val originalOS = System.getProperty("os.name")
    val originalArch = System.getProperty("os.arch")
    try {
      System.setProperty("os.name", os)
      System.setProperty("os.arch", arch)
      body
    } finally {
      System.setProperty("os.name", originalOS)
      System.setProperty("os.arch", originalArch)
    }
  }

  test("ARM64 aliases select ARM resources instead of x64 resources") {
    for (arch <- Seq("aarch64", "arm64", "ARM64"); (os, prefix) <- Seq(
      "Linux" -> "linux", "Windows" -> "windows", "Mac OS X" -> "osx")) {
      withPlatform(os, arch) {
        assert(NativeLoader.getResourcesPath("/native") == s"/native/$prefix/aarch64/")
      }
    }
  }

  test("x64 resource paths remain unchanged") {
    for (arch <- Seq("amd64", "x86_64"); (os, prefix) <- Seq(
      "Linux" -> "linux", "Windows" -> "windows", "Mac OS X" -> "osx")) {
      withPlatform(os, arch) {
        assert(NativeLoader.getResourcesPath("/native") == s"/native/$prefix/x86_64/")
      }
    }
  }

  test("missing ARM native reports the component and remediation on repeated calls") {
    withPlatform("Linux", "aarch64") {
      val loader = new NativeLoader("/missing-test-native", "Test operator")
      for (_ <- 1 to 2) {
        val error = intercept[UnsupportedOperationException] {
          loader.loadLibraryByName("synapseml_nonexistent_test_native")
        }
        assert(error.getMessage.contains("Test operator is not supported on ARM"))
        assert(error.getMessage.contains("x64 compute"))
        assert(error.getMessage.contains("linux/aarch64"))
        assert(error.getCause.isInstanceOf[java.io.FileNotFoundException])
      }
    }
  }

  test("missing x64 native retains its existing error type") {
    withPlatform("Linux", "amd64") {
      intercept[UnsatisfiedLinkError] {
        new NativeLoader("/missing-test-native").loadLibraryByName("synapseml_nonexistent_test_native")
      }
    }
  }
}
