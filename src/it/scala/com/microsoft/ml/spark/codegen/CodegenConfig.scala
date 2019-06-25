// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.codegen

import java.io.File

import com.microsoft.ml.spark.build.BuildInfo

object Config {
  val debugMode = sys.env.getOrElse("DEBUGMODE", "").trim.toLowerCase == "true"

  val topDir = BuildInfo.baseDirectory
  val version = BuildInfo.version
  val packageName = BuildInfo.name
  val targetDir = new File(topDir, "target/scala-2.11")
  val scalaSrcDir = "src/main/scala"

  val generatedDir = new File(targetDir, "generated")
  val packageDir = new File(generatedDir, "package")
  val srcDir = new File(generatedDir, "src")
  val testDir = new File(generatedDir, "test")
  val docDir = new File(generatedDir, "doc")

  //Python Codegen Constant
  val pySrcDir = new File(srcDir, "python")
  val pyPackageDir = new File(packageDir, "python")
  val pyTestDir = new File(testDir, "python")
  val pyDocDir = new File(docDir, "python")
  val pySrcOverrideDir = new File(topDir, "src/main/python")
  val pyTestOverrideDir = new File(topDir, "src/test/python")
  val tmpDocDir = new File(pyDocDir, "tmpDoc")

  //R Codegen Constants
  val rSrcDir = new File(srcDir, "R")
  val sparklyRNamespacePath = new File(rSrcDir, "NAMESPACE")
  val rPackageDir = new File(packageDir, "R")
  val rTestDir = new File(testDir, "R")
  val rSrcOverrideDir = new File(topDir, "src/main/R")
  //val rPackageFile = new File(rPackageDir, s"mmlspark-$mmlVer.zip")

  val internalPrefix = "_"
  val scopeDepth = " " * 4

  val copyrightLines =
    s"""|# Copyright (C) Microsoft Corporation. All rights reserved.
        |# Licensed under the MIT License. See LICENSE in project root for information.
        |""".stripMargin

}
