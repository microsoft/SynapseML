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
        println(s"grt: ${ltc.testClassName}")
        ltc.makeRTestFile(conf)
      } catch {
        case _: NotImplementedError =>
          println(s"ERROR: Could not generate R test for ${ltc.testClassName} because of Complex Parameters")
      }
    }
  }

  /*def makeRInitFiles(conf: CodegenConfig, packageFolder: String = ""): Unit = {
    val dir = new File(new File(conf.rTestDir,  "synapsemltest"), packageFolder)
    if (!dir.exists()){
      printf("----------------  making dir %s\n", dir.toString)
      dir.mkdirs()
    }
    writeFile(new File(dir, "__init__.r"), "")
    dir.listFiles().filter(_.isDirectory).foreach(f =>
      makeRInitFiles(conf, packageFolder + "/" + f.getName)
    )
  }*/

  //noinspection ScalaStyle
  def generateRPackageData(conf: CodegenConfig): Unit = {
    if (!conf.rSrcDir.exists()) {
      conf.rSrcDir.mkdir()
    }
    val synapsemlTestDir = join(conf.rTestDir,"synapsemltest")
    if (!synapsemlTestDir.exists()) {
      synapsemlTestDir.mkdirs()
    }

    // See https://spark.rstudio.com/packages/sparklyr/latest/reference/spark-connections.html
    writeFile(join(synapsemlTestDir, "spark.R"),
      s"""
         |# Copyright (C) Microsoft Corporation. All rights reserved.
         |# Licensed under the MIT License. See LICENSE in project root for information.
         |
         |#import synapse.ml
         |#from synapse.ml.core import __spark_package_version__ ??
         |
         |library(sparklyr)
         |library(dplyr)
         |# ? library(synapse.ml) ??
         |
         |conf <- spark_config()
         |conf$$`sparklyr.shell.conf` <- c(
         |  "spark.app.name=RSparkTests",
         |  "spark.jars.packages=om.microsoft.azure:synapseml_2.12:" + __spark_package_version__,
         |  "spark.jars.repositories=https://mmlspark.azureedge.net/maven",
         |  "spark.executor.heartbeatInterval=60s",
         |  "spark.sql.shuffle.partitions=10",
         |  "spark.sql.crossJoin.enabled=true")
         |
         |sc <- spark_connect(master = local[*], config = conf)
         |connection_is_open(sc)
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
