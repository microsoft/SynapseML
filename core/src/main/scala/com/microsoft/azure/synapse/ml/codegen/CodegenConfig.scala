// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import java.io.File


case class CodegenConfig(name: String,
                         jarName: Option[String],
                         topDir: String,
                         targetDir: String,
                         version: String,
                         pythonizedVersion: String,
                         rVersion: String,
                         packageName: String) {
  def generatedDir: File = new File(targetDir, "generated")

  def packageDir: File = new File(generatedDir, "package")

  def srcDir: File = new File(generatedDir, "src")

  def testDir: File = new File(generatedDir, "test")

  def docDir: File = new File(generatedDir, "doc")

  def testDataDir: File = new File(generatedDir, "test-data")

  //Python Codegen Constant
  def pySrcDir: File = new File(srcDir, "python")

  def pyPackageDir: File = new File(packageDir, "python")

  def pyTestDir: File = new File(testDir, "python")
  def pyTestDataDir: File = new File(testDataDir, "python")
  def pySrcOverrideDir: File = new File(topDir, "src/main/python")

  def pyTestOverrideDir: File = new File(topDir, "src/test/python")

  //R Codegen Constants
  def rSrcRoot: File = new File(srcDir, "R")
  def rSrcDir: File = new File(rSrcRoot, "synapseml/R")
  def rPackageDir: File = new File(packageDir, "R")
  def rTestDir: File = new File(testDir, "R/tests")
  def rTestThatDir: File = new File(rTestDir, "testthat")
  def rTestDataDir: File = new File(testDataDir, "R")
  def rTestOverrideDir: File = new File(topDir, "src/test/R")

  def rSrcOverrideDir: File = new File(topDir, "src/main/R")

  //val rPackageFile = new File(rPackageDir, s"mmlspark-$mmlVer.zip")
  def internalPrefix: String = "_"

  def scopeDepth: String = " " * 4

  def copyrightLines: String =
    s"""|# Copyright (C) Microsoft Corporation. All rights reserved.
        |# Licensed under the MIT License. See LICENSE in project root for information.
        |""".stripMargin

  // The __init__.py file
  def packageHelp(importString: String): String = {
    s"""|$copyrightLines
        |
        |"\""
        |SynapseML is an ecosystem of tools aimed towards expanding the distributed computing framework
        |Apache Spark in several new directions. SynapseML adds many deep learning and data science tools to the Spark
        |ecosystem, including seamless integration of Spark Machine Learning pipelines with
        |Microsoft Cognitive Toolkit (CNTK), LightGBM and OpenCV. These tools enable powerful and
        |highly-scalable predictive and analytical models for a variety of datasources.
        |
        |SynapseML also brings new networking capabilities to the Spark Ecosystem. With the HTTP on Spark project,
        |users can embed any web service into their SparkML models. In this vein, SynapseML provides easy to use SparkML
        |transformers for a wide variety of Microsoft Cognitive Services. For production grade deployment,
        |the Spark Serving project enables high throughput, sub-millisecond latency web services,
        |backed by your Spark cluster.
        |
        |SynapseML requires Scala 2.12, Spark 3.0+, and Python 3.6+.
        |"\""
        |
        |__version__ = "$pythonizedVersion"
        |__spark_package_version__ = "$version"
        |
        |$importString
        |""".stripMargin
  }
}

object CodegenConfigProtocol extends DefaultJsonProtocol {
  implicit val CCFormat: RootJsonFormat[CodegenConfig] = jsonFormat8(CodegenConfig.apply)
}
