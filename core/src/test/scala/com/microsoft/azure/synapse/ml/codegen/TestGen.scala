// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import com.microsoft.azure.synapse.ml.codegen.CodegenConfigProtocol._
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.commons.io.FileUtils
import spray.json._


object TestGen {

  import CodeGenUtils._
  import DotnetTestGen._
  import PyTestGen._

  def main(args: Array[String]): Unit = {
    val conf = args.head.parseJson.convertTo[CodegenConfig]
    clean(conf.testDataDir)
    clean(conf.testDir)
    generatePythonTests(conf)
    generateDotnetTests(conf)
    TestBase.stopSparkSession()
    generatePyPackageData(conf)
    if (toDir(conf.pyTestOverrideDir).exists()) {
      FileUtils.copyDirectoryToDirectory(toDir(conf.pyTestOverrideDir), toDir(conf.pyTestDir))
    }
    if (toDir(conf.dotnetTestOverrideDir).exists())
      FileUtils.copyDirectoryToDirectory(toDir(conf.dotnetTestOverrideDir), toDir(conf.dotnetTestDir))
    makeInitFiles(conf)
    generateDotnetTestProjFile(conf)
    generateDotnetHelperFile(conf)
  }
}
