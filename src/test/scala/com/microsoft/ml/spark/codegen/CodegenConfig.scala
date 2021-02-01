// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.codegen

import java.io.File

import com.microsoft.ml.spark.build.BuildInfo

object Config {
  val DebugMode = sys.env.getOrElse("DEBUGMODE", "").trim.toLowerCase == "true"

  val TopDir = BuildInfo.baseDirectory
  val Version = BuildInfo.version
  val PackageName = BuildInfo.name
  val TargetDir = new File(TopDir, s"target/scala-${BuildInfo.scalaVersion.slice(0,4)}")
  val ScalaSrcDir = "src/main/scala"

  val GeneratedDir = new File(TargetDir, "generated")
  val PackageDir = new File(GeneratedDir, "package")
  val SrcDir = new File(GeneratedDir, "src")
  val TestDir = new File(GeneratedDir, "test")
  val DocDir = new File(GeneratedDir, "doc")

  //Python Codegen Constant
  val PySrcDir = new File(SrcDir, "python")
  val PyPackageDir = new File(PackageDir, "python")
  val PyTestDir = new File(TestDir, "python")
  val PySrcOverrideDir = new File(TopDir, "src/main/python")
  val PyTestOverrideDir = new File(TopDir, "src/test/python")

  //R Codegen Constants
  val RSrcDir = new File(SrcDir, "R")
  val SparklyRNamespacePath = new File(RSrcDir, "NAMESPACE")
  val RPackageDir = new File(PackageDir, "R")
  val RTestDir = new File(TestDir, "R")
  val RSrcOverrideDir = new File(TopDir, "src/main/R")
  //val rPackageFile = new File(rPackageDir, s"mmlspark-$mmlVer.zip")

  val InternalPrefix = "_"
  val ScopeDepth = " " * 4

  val CopyrightLines =
    s"""|# Copyright (C) Microsoft Corporation. All rights reserved.
        |# Licensed under the MIT License. See LICENSE in project root for information.
        |""".stripMargin

  // The __init__.py file
  def packageHelp(importString: String): String = {
    s"""|$CopyrightLines
        |
        |"\""
        |MicrosoftML is a library of Python classes to interface with the
        |Microsoft scala APIs to utilize Apache Spark to create distibuted
        |machine learning models.
        |
        |MicrosoftML simplifies training and scoring classifiers and
        |regressors, as well as facilitating the creation of models using the
        |CNTK library, images, and text.
        |"\""
        |
        |__version__ = "${BuildInfo.pythonizedVersion}"
        |__spark_package_version__ = "${BuildInfo.version}"
        |
        |$importString
        |""".stripMargin
  }
}
