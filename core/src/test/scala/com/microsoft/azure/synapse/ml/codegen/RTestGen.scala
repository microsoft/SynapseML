// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import java.io.File
import com.microsoft.azure.synapse.ml.codegen.CodegenConfigProtocol._
import com.microsoft.azure.synapse.ml.core.env.FileUtilities._
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.RTestFuzzing
import com.microsoft.azure.synapse.ml.core.utils.JarLoadingUtils.instantiateServices
import org.apache.commons.io.FileUtils
import spray.json._


object RTestGen {

  import CodeGenUtils._

  def generateRTests(conf: CodegenConfig): Unit = {
    instantiateServices[RTestFuzzing[_]](conf.jarName).foreach { ltc =>
      try {
        ltc.makeRTestFile(conf)
      } catch {
        case _: NotImplementedError =>
          println(s"ERROR: Could not generate R test for ${ltc.testClassName} because of Complex Parameters")
      }
    }
  }

/*
  def makeInitFiles(conf: CodegenConfig, packageFolder: String = ""): Unit = {
    val dir = new File(new File(conf.rTestDir,  "synapsemltest"), packageFolder)
    if (!dir.exists()){
      dir.mkdirs()
    }
    writeFile(new File(dir, "__init__.r"), "")
    dir.listFiles().filter(_.isDirectory).foreach(f =>
      makeInitFiles(conf, packageFolder + "/" + f.getName)
    )
  }
*/

  //noinspection ScalaStyle
  def generateRPackageData(conf: CodegenConfig): Unit = {
    if (!conf.rSrcDir.exists()) {
      conf.rSrcDir.mkdir()
    }
    writeFile(join(conf.rTestDir,"synapsemltest", "spark.R"),
      s"""
         |# Corright (C) Microsoft Corporation. All rights reserved.
         |# Licensed under the MIT License. See LICENSE in project root for information.
         |
         |from rspark.sql import SparkSession, SQLContext
         |import os
         |import synapse.ml
         |from synapse.ml.core import __spark_package_version__
         |
         |spark = (SparkSession.builder
         |    .master("local[*]")
         |    .appName("RsparkTests")
         |    .config("spark.jars.packages", "com.microsoft.azure:synapseml_2.12:" + __spark_package_version__)
         |    .config("spark.jars.repositories", "https://mmlspark.azureedge.net/maven")
         |    .config("spark.executor.heartbeatInterval", "60s")
         |    .config("spark.sql.shuffle.partitions", 10)
         |    .config("spark.sql.crossJoin.enabled", "true")
         |    .getOrCreate())
         |
         |sc = SQLContext(spark.sparkContext)
         |
         |""".stripMargin)
  }

  def main(args: Array[String]): Unit = {
    val conf = args.head.parseJson.convertTo[CodegenConfig]
    clean(conf.rTestDataDir)
    clean(conf.rTestDir)
    generateRTests(conf)
    TestBase.stopSparkSession()
    generateRPackageData(conf)
    if (toDir(conf.rTestOverrideDir).exists()){
      FileUtils.copyDirectoryToDirectory(toDir(conf.rTestOverrideDir), toDir(conf.rTestDir))
    }

  }
}
