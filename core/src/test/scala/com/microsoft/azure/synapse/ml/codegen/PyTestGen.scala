// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import com.microsoft.azure.synapse.ml.codegen.CodegenConfigProtocol._
import com.microsoft.azure.synapse.ml.core.env.FileUtilities._
import com.microsoft.azure.synapse.ml.core.env.PackageUtils.{SparkMavenPackageList, SparkMavenRepositoryList}
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.PyTestFuzzing
import com.microsoft.azure.synapse.ml.core.utils.JarLoadingUtils.instantiateServices
import org.apache.commons.io.FileUtils
import spray.json._

import java.io.File


object PyTestGen {

  import CodeGenUtils._

  def generatePythonTests(conf: CodegenConfig): Unit = {
    instantiateServices[PyTestFuzzing[_]](conf.jarName).foreach { ltc =>
      try {
        ltc.makePyTestFile(conf)
      } catch {
        case err: NotImplementedError =>
          println(s"$err")
          println(s"ERROR: Could not generate Python test for ${ltc.testClassName} because of Complex Parameters")
      }
    }
  }

  def makeInitFiles(conf: CodegenConfig, packageFolder: String = ""): Unit = {
    val dir = new File(new File(conf.pyTestDir,  "synapsemltest"), packageFolder)
    if (!dir.exists()){
      dir.mkdirs()
    }
    writeFile(new File(dir, "__init__.py"), "")
    dir.listFiles().filter(_.isDirectory).foreach(f =>
      makeInitFiles(conf, packageFolder + "/" + f.getName)
    )
  }


  //noinspection ScalaStyle
  def generatePyPackageData(conf: CodegenConfig): Unit = {
    val dir = join(conf.pyTestDir, "synapsemltest")
    if (!dir.exists()) {
      dir.mkdirs()
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = args.head.parseJson.convertTo[CodegenConfig]
    clean(conf.pyTestDataDir)
    clean(conf.pyTestDir)
    generatePythonTests(conf)
    TestBase.stopSparkSession()
    generatePyPackageData(conf)
    if (toDir(conf.pyTestOverrideDir).exists()){
      FileUtils.copyDirectoryToDirectory(toDir(conf.pyTestOverrideDir), toDir(conf.pyTestDir))
    }

    makeInitFiles(conf)
  }
}
