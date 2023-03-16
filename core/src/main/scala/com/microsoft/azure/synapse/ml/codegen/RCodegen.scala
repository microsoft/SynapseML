// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import com.microsoft.azure.synapse.ml.codegen.CodegenConfigProtocol._
import com.microsoft.azure.synapse.ml.core.env.FileUtilities._
import com.microsoft.azure.synapse.ml.core.env.PackageUtils.{SparkMavenPackageList, SparkMavenRepositoryList}
import com.microsoft.azure.synapse.ml.core.utils.JarLoadingUtils.instantiateServices
import org.apache.commons.io.FileUtils
import spray.json._

import java.io.File

object RCodegen {

  import CodeGenUtils._

  def generateRClasses(conf: CodegenConfig): Unit = {
    val instantiatedClasses = instantiateServices[RWrappable](conf.jarName)
    instantiatedClasses
      .foreach { w =>
        println(w.getClass.getName)
        w.makeRFile(conf)
    }
  }

  def generateRPackageData(conf: CodegenConfig): Unit = {  //scalastyle:ignore method.length
    // description file; need to encode version as decimal
    val today = new java.text.SimpleDateFormat("yyyy-MM-dd")
      .format(new java.util.Date())

    conf.rSrcDir.mkdirs()
    writeFile(new File(conf.rSrcDir.getParentFile, "DESCRIPTION"),
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
          |    R (>= 2.12.0)
          |Imports:
          |    sparklyr
          |License: MIT
          |Suggests:
          |    testthat (>= 3.0.0)
          |Config/testthat/edition: 3
          |""".stripMargin)

    writeFile(new File(conf.rSrcDir, "package_register.R"),
      s"""|#' @import sparklyr
          |spark_dependencies <- function(spark_version, scala_version, ...) {
          |    spark_dependency(
          |        jars = c(),
          |        packages = c("$SparkMavenPackageList"),
          |        repositories = c("$SparkMavenRepositoryList")
          |    )
          |}
          |
          |#' @import sparklyr
          |.onLoad <- function(libname, pkgname) {
          |    sparklyr::register_extension(pkgname)
          |}
          |""".stripMargin)

    writeFile(new File(conf.rSrcDir.getParentFile, "synapseml.Rproj"),
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

  }

  def rGen(conf: CodegenConfig): Unit = {
    println(s"Generating R for ${conf.jarName}")
    clean(conf.rSrcRoot)
    generateRPackageData(conf)
    generateRClasses(conf)
    if (conf.rSrcOverrideDir.exists())
      FileUtils.copyDirectoryToDirectory(toDir(conf.rSrcOverrideDir), toDir(conf.rSrcDir))
    if (conf.rTestOverrideDir.exists())
      FileUtils.copyDirectoryToDirectory(toDir(conf.rTestOverrideDir), toDir(conf.rTestDir))
  }

  def main(args: Array[String]): Unit = {
    val conf = args.head.parseJson.convertTo[CodegenConfig]
    clean(conf.rPackageDir)
    rGen(conf)
  }

}
