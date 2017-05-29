// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.codegen

import com.microsoft.ml.spark.FileUtilities._
import sys.process.Process

object Config {

  val srcDir     = new File(".").getCanonicalFile()
  val topDir     = new File("..").getCanonicalFile()
  val rootsFile  = new File(srcDir, "project/project-roots.txt")
  val outputDir  = new File(topDir, "BuildArtifacts/sdk")
  val toZipDir   = new File(srcDir, "src/main/resources/mmlspark")
  val zipFile    = new File(outputDir, "mmlspark.zip")
  val pyTestDir  = new File(topDir, "TestResults/generated_pytests")
  val docDir     = new File(topDir, "BuildArtifacts/docs")
  val inDocDir   = new File(docDir, "source")
  val jarRelPath = "target/scala-" + sys.env("SCALA_VERSION")
  val pyRelPath  = "src/main/python"
  val mmlVer     = sys.env.getOrElse("MML_VERSION",
                                     Process("../tools/runme/show-version").!!.trim)
  val debugMode  = sys.env.getOrElse("DEBUGMODE", "").trim.toLowerCase == "true"
  val internalPrefix  = "_"

  val copyrightLines =
    s"""|# Copyright (C) Microsoft Corporation. All rights reserved.
        |# Licensed under the MIT License. See LICENSE in the project root for information.
        |""".stripMargin

}
