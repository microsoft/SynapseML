// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import com.microsoft.azure.synapse.ml.param.{EstimatorParam, ModelParam, PipelineStageParam, TransformerParam}
import org.apache.commons.io.FileUtils
import org.apache.spark.ml.feature.SQLTransformer
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.Files

class RCodegenSuite extends AnyFunSuite {

  test("R setup enables ANSI double-quoted identifiers") {
    val tempDir = Files.createTempDirectory("RCodegenSuite")
    val conf = CodegenConfig(
      "r-codegen-suite",
      None,
      tempDir.toString,
      tempDir.resolve("target").toString,
      "1.0.0",
      "1.0.0",
      "1.0.0",
      "synapseml"
    )

    try {
      RTestGen.generateRPackageData(conf)
      val setup = Files.readString(conf.rTestThatDir.toPath.resolve("setup.R"), StandardCharsets.UTF_8)

      assert(setup.contains("\"spark.sql.ansi.enabled=true\""))
      assert(setup.contains("\"spark.sql.ansi.doubleQuotedIdentifiers=true\""))
    } finally {
      FileUtils.deleteDirectory(tempDir.toFile)
    }
  }

  test("Nested pipeline stages load without eager sparklyr constructors") {
    val parent = new SQLTransformer()
    val params = Seq(
      new EstimatorParam(parent, "stage", "stage"),
      new ModelParam(parent, "stage", "stage"),
      new PipelineStageParam(parent, "stage", "stage"),
      new TransformerParam(parent, "stage", "stage")
    )

    params.foreach { param =>
      val loadLine = param.rLoadLine(0)

      assert(loadLine.contains("sparklyr:::new_ml_pipeline_stage"))
      assert(loadLine.contains("""invoke(spark_jobj(stageModel), "getStages")[[1]]"""))
      assert(!loadLine.contains("ml_stages"))
    }
  }
}
