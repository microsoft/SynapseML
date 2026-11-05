// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StructType

import java.io.File
import java.util.{Collections, UUID}

/** Test-jar bridge for managed-runtime storage guards and interruption after a real storage commit. */
object AIPromptFabricRecovery {
  def verifyLocalStorageRejected(spark: SparkSession): Unit = {
    require(!spark.sparkContext.isLocal, "This regression requires a non-local Spark master")
    val directory = new File(System.getProperty("java.io.tmpdir"), "aiprompt-reject-local-" + UUID.randomUUID())
    val path = directory.toURI.toString
    require(!directory.exists(), "The local rejection probe requires a fresh path")
    def rejected(action: => Any): Unit = {
      try {
        action
        throw new IllegalStateException("Non-local Spark accepted file: durable storage")
      } catch {
        case error: IllegalArgumentException =>
          require(error.getMessage.contains("Spark local mode"), "Expected the local storage runtime guard")
      }
      require(!directory.exists(), "The runtime guard must reject before creating storage")
    }
    rejected(AIPromptDelta.requireRuntime(spark, path))
    val input = spark.createDataFrame(Collections.singletonList(Row("hello")),
      new StructType().add("text", "string"))
    val stage = new AIPrompt().setUrl("http://127.0.0.1:1/v1").setModel("synthetic-model")
      .setInputCol("text").setOutputPath(path)
    rejected(stage.transform(input))
    rejected(stage.resume(spark))
    rejected(AIPrompt.loadOutput(spark, path))
    rejected(AIPrompt.getOutputStatus(spark, path))
    rejected(AIPrompt.releaseOutputOwner(spark, path, "synthetic-owner", confirmStopped = true))
  }

  def stopAfterFirstBatch(prompt: AIPrompt, input: DataFrame): Unit = {
    val stopped = new IllegalStateException("AIPrompt managed-runtime post-commit interruption")
    try {
      AIPromptOutput.transformObserved(prompt, input, event =>
        if (event == "batch_committed") throw stopped)
      throw new IllegalStateException("Expected a nonempty input and a committed batch")
    } catch {
      case error: IllegalStateException if error eq stopped => ()
    }
  }
}
