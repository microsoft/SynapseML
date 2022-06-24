// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import com.microsoft.azure.synapse.ml.codegen.CodegenConfigProtocol._
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import java.io.File  // checkme
import java.util.concurrent.locks.{Lock, ReentrantLock} // checkme
import CodegenConfigProtocol._ // checkme
import org.apache.commons.io.FileUtils
import spray.json._


object TestGen {

  import CodeGenUtils._
  import DotnetTestGen._
  import PyTestGen._
  import RTestGen._

  private def copyOverrides(sourceDir: File, targetDir: File): Unit = {
    if (toDir(sourceDir).exists()) {
      FileUtils.copyDirectoryToDirectory(toDir(sourceDir), toDir(targetDir))
    }
  }

  def getListOfFiles(dir: File): Unit = {
    if (dir.exists && dir.isDirectory) {
      val files = dir.listFiles.filter(_.isFile).toList
      files.foreach(f => println(s"f: ${f.getName}"))
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = args.head.parseJson.convertTo[CodegenConfig]
    clean(conf.testDataDir)
    clean(conf.testDir)
    generatePythonTests(conf)
    generateRTests(conf)
    generateDotnetTests(conf)
    TestBase.stopSparkSession()
    generatePyPackageData(conf)
    generateRPackageData(conf)
    copyOverrides(conf.pyTestOverrideDir, conf.pyTestDir)
    copyOverrides(conf.rTestOverrideDir, conf.rTestDir)
    copyOverrides(conf.dotnetTestOverrideDir, conf.dotnetTestDir)
    makePyInitFiles(conf)
    //makeInitFiles(conf) // scheckme
    generateDotnetTestProjFile(conf)
    generateDotnetHelperFile(conf)
  }

}
