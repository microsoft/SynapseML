// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import com.microsoft.azure.synapse.ml.codegen.CodegenConfigProtocol._
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import java.io.File
import org.apache.commons.io.FileUtils
import spray.json._


object TestGen {

  import CodeGenUtils._
  import PyTestGen._
  import RTestGen._

  private def copyOverrides(sourceDir: File, targetDir: File): Unit = {
    if (toDir(sourceDir).exists()) {
      FileUtils.copyDirectoryToDirectory(toDir(sourceDir), toDir(targetDir))
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = args.head.parseJson.convertTo[CodegenConfig]
    clean(conf.testDataDir)
    clean(conf.testDir)
    generatePythonTests(conf)
    generateRTests(conf)
    TestBase.stopSparkSession()
    generatePyPackageData(conf)
    generateRPackageData(conf)
    copyOverrides(conf.pyTestOverrideDir, conf.pyTestDir)
    copyOverrides(conf.rTestOverrideDir, conf.rTestDir)
    makeInitFiles(conf)
  }

}
