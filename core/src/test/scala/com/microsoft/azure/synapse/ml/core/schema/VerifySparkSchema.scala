// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.schema

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

/** Verifies the spark schema functions. */
class VerifySparkSchema extends TestBase {

  val labelColumn = "label"
  val rawPredictionColumn = "rawPrediction"
  val probabilityColumn = "probability"
  val predictionColumn = "prediction"
  test("Spark schema should be able to set and get label, score, probability and scored labels column name") {
    val dataset = spark.createDataFrame(Seq(
      (0, Array("Hi", "I", "can", "not", "foo"), 0.50, 0.60, 0),
      (1, Array("I"),                            0.40, 0.50, 1),
      (2, Array("Logistic", "regression"),       0.78, 0.99, 2),
      (3, Array("Log","f", "reg"),               0.12, 0.34, 3)
    )).toDF(labelColumn, "words", rawPredictionColumn, probabilityColumn, predictionColumn)

    val modelName = "Test model name"
    val datasetWithLabel =
      SparkSchema.setLabelColumnName(dataset, modelName, labelColumn, SchemaConstants.RegressionKind)
    val labelColumnNameRetrieved =
      SparkSchema.getLabelColumnName(datasetWithLabel, modelName)

    assert(labelColumnNameRetrieved == labelColumn)

    val datasetWithScore =
      SparkSchema.updateColumnMetadata(dataset, modelName, rawPredictionColumn, SchemaConstants.RegressionKind)
    val scoreColumnNameRetrieved =
      SparkSchema.getSparkRawPredictionColumnName(datasetWithScore, modelName)

    assert(scoreColumnNameRetrieved == rawPredictionColumn)

    val datasetWithProbability =
      SparkSchema.updateColumnMetadata(dataset, modelName, probabilityColumn,
                                                   SchemaConstants.RegressionKind)
    val probabilityColumnNameRetrieved =
      SparkSchema.getSparkProbabilityColumnName(datasetWithProbability, modelName)

    assert(probabilityColumnNameRetrieved == probabilityColumn)

    val datasetWithScoredLabels =
      SparkSchema.updateColumnMetadata(dataset, modelName, predictionColumn, SchemaConstants.RegressionKind)
    val scoredLabelsColumnNameRetrieved =
      SparkSchema.getSparkPredictionColumnName(datasetWithScoredLabels.schema, modelName)

    assert(scoredLabelsColumnNameRetrieved == predictionColumn)
  }

}
