// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import java.io.File
import CodegenConfigProtocol._
import com.microsoft.azure.synapse.ml.core.env.FileUtilities._
import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils._
import com.microsoft.azure.synapse.ml.core.utils.JarLoadingUtils.instantiateServices
import org.apache.spark.ml.{Estimator, Model, PipelineStage}
import spray.json._

object CodeGenUtils {
  def clean(dir: File): Unit = if (dir.exists()) FileUtils.forceDelete(dir)

  def toDir(f: File): File = new File(f, File.separator)
}


object CodeGen {

  type Foo = Estimator[_ <: Model[_]]

  import CodeGenUtils._

  def generatePythonClasses(conf: CodegenConfig): Unit = {
    val instantiatedClasses = instantiateServices[PythonWrappable](conf.jarName)
    instantiatedClasses.foreach { w =>
      println(w.getClass.getName)
      w.makePyFile(conf)
    }
  }

  def generateRClasses(conf: CodegenConfig): Unit = {
    val instantiatedClasses = instantiateServices[RWrappable](conf.jarName)
    instantiatedClasses.foreach { w =>
      println(w.getClass.getName)
      w.makeRFile(conf)
    }
  }

  private def makeInitFiles(conf: CodegenConfig, packageFolder: String = ""): Unit = {
    val dir = join(conf.pySrcDir, "synapse", "ml", packageFolder)
    val packageString = if (packageFolder != "") packageFolder.replace("/", ".") else ""
    val importStrings =
      dir.listFiles.filter(_.isFile).sorted
        .map(_.getName)
        .filter(name => name.endsWith(".py") && !name.startsWith("_") && !name.startsWith("test"))
        .map(name => s"from synapse.ml$packageString.${getBaseName(name)} import *\n").mkString("")
    val initFile = new File(dir, "__init__.py")
    if (packageFolder != "") {
      writeFile(initFile, conf.packageHelp(importStrings))
    } else if (initFile.exists()) {
      initFile.delete()
    }
    dir.listFiles().filter(_.isDirectory).foreach(f =>
      makeInitFiles(conf, packageFolder + "/" + f.getName)
    )
  }

  //noinspection ScalaStyle
  def generateRPackageData(conf: CodegenConfig): Unit = {
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
          |Maintainer: SynapseML Team <mmlspark-support@microsoft.com>
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
          |        packages = c(
          |           "com.microsoft.azure:${conf.name}:${conf.version}"
          |        ),
          |        repositories = c("https://mmlspark.azureedge.net/maven")
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

  //noinspection ScalaStyle
  def generatePyPackageData(conf: CodegenConfig): Unit = {
    if (!conf.pySrcDir.exists()) {
      conf.pySrcDir.mkdir()
    }
    writeFile(join(conf.pySrcDir, "setup.py"),
      s"""
         |# Copyright (C) Microsoft Corporation. All rights reserved.
         |# Licensed under the MIT License. See LICENSE in project root for information.
         |
         |import os
         |from setuptools import setup, find_namespace_packages
         |import codecs
         |import os.path
         |
         |setup(
         |    name="${conf.name}",
         |    version="${conf.pythonizedVersion}",
         |    description="Microsoft ML for Spark",
         |    long_description="Microsoft ML for Apache Spark contains Microsoft's open source "
         |                     + "contributions to the Apache Spark ecosystem",
         |    license="MIT",
         |    packages=find_namespace_packages(include=['synapse.ml.*']),
         |    url="https://github.com/Microsoft/SynapseML",
         |    author="Microsoft",
         |    author_email="mmlspark-support@microsoft.com",
         |    classifiers=[
         |        "Development Status :: 4 - Beta",
         |        "Intended Audience :: Developers",
         |        "Intended Audience :: Data Scientists",
         |        "Topic :: Software Development :: Datascience Tools",
         |        "License :: OSI Approved :: MIT License",
         |        "Programming Language :: Python :: 2",
         |        "Programming Language :: Python :: 3",
         |    ],
         |    zip_safe=True,
         |    package_data={"synapseml": ["../LICENSE.txt", "../README.txt"]},
         |)
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

  def pyGen(conf: CodegenConfig): Unit = {
    println(s"Generating python for ${conf.jarName}")
    clean(conf.pySrcDir)
    generatePyPackageData(conf)
    generatePythonClasses(conf)
    if (conf.pySrcOverrideDir.exists())
      FileUtils.copyDirectoryToDirectory(toDir(conf.pySrcOverrideDir), toDir(conf.pySrcDir))
    makeInitFiles(conf)
  }

  def main(args: Array[String]): Unit = {
    val conf = args.head.parseJson.convertTo[CodegenConfig]
    clean(conf.packageDir)
    rGen(conf)
    pyGen(conf)
  }

}

