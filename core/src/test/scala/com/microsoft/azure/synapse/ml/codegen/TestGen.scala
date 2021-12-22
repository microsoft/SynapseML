// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.PyTestFuzzing

import java.io.File
import CodegenConfigProtocol._
import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.core.env.FileUtilities._
import com.microsoft.azure.synapse.ml.core.utils.JarLoadingUtils.instantiateServices
import org.apache.commons.io.FileUtils
import spray.json._


object TestGen {

  import CodeGenUtils._
  import DotnetTestGen._
  import PyTestGen._

  def generatePythonTests(conf: CodegenConfig): Unit = {
    instantiateServices[PyTestFuzzing[_]](conf.jarName).foreach { ltc =>
      try {
        ltc.makePyTestFile(conf)
      } catch {
        case _: NotImplementedError =>
          println(s"ERROR: Could not generate test for ${ltc.testClassName} because of Complex Parameters")
      }
    }
  }

  private def makeInitFiles(conf: CodegenConfig, packageFolder: String = ""): Unit = {
    val dir = new File(new File(conf.pyTestDir, "synapsemltest"), packageFolder)
    if (!dir.exists()) {
      dir.mkdirs()
    }
    writeFile(new File(dir, "__init__.py"), "")
    dir.listFiles().filter(_.isDirectory).foreach(f =>
      makeInitFiles(conf, packageFolder + "/" + f.getName)
    )
  }


  //noinspection ScalaStyle
  def generatePyPackageData(conf: CodegenConfig): Unit = {
    if (!conf.pySrcDir.exists()) {
      conf.pySrcDir.mkdir()
    }
    val scalaVersion = BuildInfo.scalaVersion.split(".".toCharArray).dropRight(1).mkString(".")
    writeFile(join(conf.pyTestDir, "synapsemltest", "spark.py"),
      s"""
         |# Copyright (C) Microsoft Corporation. All rights reserved.
         |# Licensed under the MIT License. See LICENSE in project root for information.
         |
         |from pyspark.sql import SparkSession, SQLContext
         |import os
         |import synapse.ml
         |from synapse.ml.core import __spark_package_version__
         |
         |spark = (SparkSession.builder
         |    .master("local[*]")
         |    .appName("PysparkTests")
         |    .config("spark.jars.packages", "com.microsoft.azure:synapseml_$scalaVersion:" + __spark_package_version__)
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
