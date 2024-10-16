// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{StructField}
import org.vowpalwabbit.spark.VowpalWabbitExample

/**
  * Base implementation for models that use Spark-based features and result in Spark vectors
  */
trait VowpalWabbitBaseModelSpark
  extends VowpalWabbitBaseModel
    with org.apache.spark.ml.param.shared.HasFeaturesCol
    with org.apache.spark.ml.param.shared.HasRawPredictionCol
    with HasAdditionalFeatures {

  @transient
  lazy val example: VowpalWabbitExample = vw.createExample()

  /**
    * Store the detailed prediction output of VW
    */
  val vowpalWabbitPredictionCol: String = "vowpalWabbitPredictionCol"

  protected def transformImplInternal(dataset: Dataset[_]): DataFrame = {
    // pre-compute namespace hashes
    val featureColIndices = VowpalWabbitUtil.generateNamespaceInfos(
      dataset.schema,
      vwArgs.getHashSeed,
      Seq(getFeaturesCol) ++ getAdditionalFeatures)

    // translate prediction to Seq(...)
    val predictToSeq = VowpalWabbitPrediction.getPredictionFunc(vw)
    // get the schema for the prediction type
    val schemaForPredictionType = VowpalWabbitPrediction.getSchema(vw)

    // add the detailed prediction column
    val outputSchema = dataset.schema.add(StructField(vowpalWabbitPredictionCol, schemaForPredictionType, false))

    // create a fitting row encoder
    val rowEncoder = ExpressionEncoder(outputSchema)

    dataset.toDF.mapPartitions(inputRows => {
      inputRows.map { row => {
        // clear re-used example
        example.clear()

        // transfer features
        VowpalWabbitUtil.addFeaturesToExample(featureColIndices, row, example)

        // predict
        val prediction = example.predict

        // add the prediction column to the output
        Row.fromSeq(row.toSeq :+ Row.fromSeq(predictToSeq(prediction)))
      }}
    })(rowEncoder)
      .toDF()
  }
}
