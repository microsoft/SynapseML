// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.codegen

import com.microsoft.ml.spark.core.env.FileUtilities._
import sys.process.Process
import com.microsoft.ml.spark.build.BuildInfo

object Config {

  val mmlVer     = sys.env.getOrElse("MML_VERSION", BuildInfo.version)
  val debugMode  = sys.env.getOrElse("DEBUGMODE", "").trim.toLowerCase == "true"

  val srcDir     = BuildInfo.baseDirectory
  val topDir     = BuildInfo.baseDirectory
  val rootsFile  = new File(srcDir, "project/project-roots.txt")
  val artifactsDir = new File(topDir, "BuildArtifacts")
  val outputDir  = new File(artifactsDir, "sdk")
  val pyDir      = new File(artifactsDir, "packages/python/mmlspark")
  val pyZipFile  = new File(outputDir, "mmlspark.zip")
  val rDir       = new File(artifactsDir, "packages/R/mmlspark")
  val rSrcDir    = new File(rDir, "R")
  val rZipFile   = new File(s"$rDir-$mmlVer.zip")
  val pyTestDir  = new File(topDir, "TestResults/generated_pytests")
  val rTestDir   = new File(topDir, "TestResults/generated_Rtests")
  val pyDocDir   = new File(artifactsDir, "pydocsrc")
  val jarRelPath = "target/scala-" + sys.env("SCALA_VERSION")
  val pyRelPath  = "src/main/python"
  val rRelPath   = "src/main/R"
  val internalPrefix  = "_"
  val scopeDepth = " " * 4
  val tmpDocDir  = new File(pyDocDir, "tmpDoc")
  val txtRelPath = "src/main/scala"
  val sparklyRNamespacePath = new File(rDir, "NAMESPACE")

  val copyrightLines =
    s"""|# Copyright (C) Microsoft Corporation. All rights reserved.
        |# Licensed under the MIT License. See LICENSE in project root for information.
        |""".stripMargin

}
