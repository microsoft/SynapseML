// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import com.microsoft.azure.synapse.ml.codegen.CodegenConfigProtocol._
import com.microsoft.azure.synapse.ml.core.env.FileUtilities._
import com.microsoft.azure.synapse.ml.core.env.PackageUtils.{SparkMavenPackageList, SparkMavenRepositoryList}
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.RTestFuzzing
import com.microsoft.azure.synapse.ml.core.utils.JarLoadingUtils.instantiateServices
import java.io.File
import org.apache.commons.io.FileUtils
import spray.json._


object RTestGen {

  import CodeGenUtils._

  def generateRTests(conf: CodegenConfig): Unit = {
    instantiateServices[RTestFuzzing[_]](conf.jarName)
      .filter(s => !isDeprecated(s.getClass.getName))
      .foreach { ltc =>
      try {
        ltc.makeRTestFile(conf)
      } catch {
        case _: NotImplementedError =>
          println(s"ERROR: Could not generate R test for ${ltc.testClassName} because of Complex Parameters")
      }
    }
  }

  // scalastyle:off method.length
  def generateRPackageData(conf: CodegenConfig): Unit = {
    // description file; need to encode version as decimal
    val today = new java.text.SimpleDateFormat("yyyy-MM-dd")
      .format(new java.util.Date())

    conf.rTestDir.mkdirs()
    val testsDir = conf.rTestDir
    val projectDir = testsDir.getParentFile
    writeFile(new File(projectDir, "DESCRIPTION"),
      s"""|Package: ${conf.name.replace("-", ".")}
          |Title: Access to SynapseML via R
          |Description: Provides an interface to SynapseML.
          |Version: ${conf.rVersion}
          |Date: $today
          |Author: Microsoft Corporation
          |Maintainer: SynapseML Team <synapseml-support@microsoft.com>
          |URL: https://github.com/Microsoft/SynapseML
          |BugReports: https://github.com/Microsoft/SynapseML/issues
          |Depends:
          |    R (>= 3.5.0)
          |Imports:
          |    sparklyr
          |License: MIT
          |Suggests:
          |    testthat (>= 3.0.0)
          |Config/testthat/edition: 3
          |""".stripMargin)

    writeFile(new File(projectDir, "synapsemltest.Rproj"),
      """
        |Version: 1.0
        |
        |RestoreWorkspace: Default
        |SaveWorkspace: Default
        |AlwaysSaveHistory: Default
        |
        |EnableCodeIndexing: Yes
        |UseSpacesForTab: Yes
        |NumSpacesForTab: 4
        |Encoding: UTF-8
        |
        |RnwWeave: Sweave
        |LaTeX: pdfLaTeX
        |
        |BuildType: Package
        |PackageUseDevtools: Yes
        |PackageInstallArgs: --no-multiarch --with-keep.source
        |
        |""".stripMargin)

    if (!conf.rTestThatDir.exists()) {
      conf.rTestThatDir.mkdirs()
    }
    writeFile(join(conf.rTestThatDir, "setup.R"),
      s"""
         |${useLibrary("sparklyr")}
         |
         |options(sparklyr.log.console = TRUE)
         |options(sparklyr.verbose = TRUE)
         |
         |conf <- spark_config()
         |conf$$sparklyr.shell.conf <- c(
         |  "spark.app.name=SparklyRTests",
         |  "spark.jars.packages=$SparkMavenPackageList",
         |  "spark.jars.repositories=$SparkMavenRepositoryList",
         |  "spark.executor.heartbeatInterval=60s",
         |  "spark.sql.shuffle.partitions=10",
         |  "spark.sql.crossJoin.enabled=true")
         |
         |sc <- spark_connect(master = "local", version = "3.3.1", config = conf)
         |
         |""".stripMargin, StandardOpenOption.CREATE)

    val library = conf.name.replaceAll("-", ".")
    writeFile(join(testsDir, "testthat.R"),
      s"""
         |${useLibrary("testthat")}
         |${useLibrary("jsonlite")}
         |${useLibrary("mlflow")}
         |library($library)
         |library(synapseml)
         |
         |testcheck("synapseml")
         |""".stripMargin)
  }
  // scalastyle:on method.length

  def useLibrary(name: String): String = {
    s"""
       |if (!require("${name}")) {
       |  install.packages("${name}")
       |  library("${name}")
       |}
       |""".stripMargin
  }

  def isDeprecated(name: String): Boolean = {
    name.contains("LIME") && !name.toLowerCase.contains("explainer")
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
