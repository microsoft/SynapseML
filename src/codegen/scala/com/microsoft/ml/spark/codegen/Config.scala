// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.codegen

import com.microsoft.ml.spark.core.env.FileUtilities._
import com.microsoft.ml.spark.build.BuildInfo

object Config {
  val mmlVer     = sys.env.getOrElse("MML_VERSION", BuildInfo.version)
  val debugMode  = sys.env.getOrElse("DEBUGMODE", "").trim.toLowerCase == "true"
  val topDir     = BuildInfo.baseDirectory
  val targetDir  = new File(getClass.getClassLoader.getResource("").toURI).getParent

  val artifactsDir = new File(targetDir, "generated-wrappers")
  val testResultsDir = new File(targetDir, "generated-tests")
  val outputDir  = new File(artifactsDir, "sdk")

  val pyDir      = new File(artifactsDir, "packages/python/mmlspark")
  val pySdkDir   = new File(outputDir,"python")
  val pyZipFile  = new File(pySdkDir, "mmlspark.zip")
  val pyTestDir  = new File(testResultsDir, "generated_pytests")
  val pyDocDir   = new File(artifactsDir, "pydocsrc")
  val pyRelPath  = "src/main/python"

  val rDir       = new File(artifactsDir, "packages/R/mmlspark")
  val rSrcDir    = new File(rDir, "R")
  val rSdkDir   = new File(outputDir,"R")
  val rZipFile   = new File(rSdkDir, s"mmlspark-$mmlVer.zip")
  val sparklyRNamespacePath = new File(rDir, "NAMESPACE")
  val rTestDir   = new File(testResultsDir, "generated_Rtests")
  val rRelPath   = "src/main/R"

  val jarRelPath = "target/scala-" + sys.env("SCALA_VERSION")
  val internalPrefix  = "_"
  val scopeDepth = " " * 4
  val tmpDocDir  = new File(pyDocDir, "tmpDoc")
  val txtRelPath = "src/main/scala"

  val copyrightLines =
    s"""|# Copyright (C) Microsoft Corporation. All rights reserved.
        |# Licensed under the MIT License. See LICENSE in project root for information.
        |""".stripMargin

}
