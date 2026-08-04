// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import com.microsoft.azure.synapse.ml.core.env.FileUtilities.readFile
import com.microsoft.azure.synapse.ml.core.env.PackageUtils
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.commons.io.{FileUtils => ApacheFileUtils}

import java.nio.file.Files

class VerifyRCodegen extends TestBase {

  test("generated R extension uses the supported Maven repository") {
    val tempDir = Files.createTempDirectory("synapseml-r-codegen").toFile
    val targetDir = new java.io.File(tempDir, "target")
    val conf = CodegenConfig(
      name = "synapseml-core",
      jarName = None,
      topDir = tempDir.getAbsolutePath,
      targetDir = targetDir.getAbsolutePath,
      version = "1.1.3",
      pythonizedVersion = "1.1.3",
      rVersion = "1.1.3",
      packageName = "com.microsoft.azure.synapse.ml.core"
    )

    try {
      RCodegen.generateRPackageData(conf)
      val registration = readFile(new java.io.File(conf.rSrcDir, "package_register.R"))

      assert(PackageUtils.PackageRepository === "https://mmlspark.blob.core.windows.net/maven")
      assert(registration.contains(s"""repositories = c("${PackageUtils.PackageRepository}")"""))
      assert(!registration.contains("azureedge.net"))
    } finally {
      ApacheFileUtils.deleteDirectory(tempDir)
    }
  }
}
