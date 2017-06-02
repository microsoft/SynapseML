// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.schema.SchemaConstants._
import com.microsoft.ml.spark.schema.{CategoricalUtilities, SchemaConstants, SparkSchema}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * Contains constants used by Compute Per Instance Statistics.
  */
object ComputePerInstanceStatistics extends DefaultParamsReadable[ComputePerInstanceStatistics] {
  // Regression metrics
  val L1LossMetric  = "L1_loss"
  val L2LossMetric  = "L2_loss"

  // Classification metrics
  val LogLossMetric = "log_loss"

  val epsilon = 1e-15
}

/**
  * Evaluates the given scored dataset with per instance metrics.
  */
class ComputePerInstanceStatistics(override val uid: String) extends Transformer with MMLParams {

  def this() = this(Identifiable.randomUID("ComputePerInstanceStatistics"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    // TODO: evaluate all models; for now, get first model name found
    val firstModelName = dataset.schema.collectFirst {
      case StructField(c, t, _, m) if (getFirstModelName(m) != null && !getFirstModelName(m).isEmpty) => {
        getFirstModelName(m).get
      }
    }
    val modelName = if (!firstModelName.isEmpty) firstModelName.get
                    else throw new Exception("Please score the model prior to evaluating")
    val dataframe = dataset.toDF()
    val labelColumnName = SparkSchema.getLabelColumnName(dataframe, modelName)

    val scoreValueKind = SparkSchema.getScoreValueKind(dataframe, modelName, labelColumnName)

    if (scoreValueKind == SchemaConstants.ClassificationKind) {
      // Compute the LogLoss for classification case
      val scoredLabelsColumnName = SparkSchema.getScoredLabelsColumnName(dataframe, modelName)

      // Get levels if categorical
      val levels = CategoricalUtilities.getLevels(dataframe.schema, labelColumnName)
      val numLevels =
        if (!levels.isEmpty && levels.get != null) {
          if (levels.get.length > 2) levels.get.length else 2
        } else {
          // Otherwise compute unique levels
          dataset.select(col(labelColumnName).cast(DoubleType)).rdd.distinct().count().toInt
        }

      val logLossFunc = udf((scoredLabel: Double, scores: org.apache.spark.ml.linalg.Vector) =>
        if (scoredLabel < numLevels) {
          -Math.log(Math.min(1, Math.max(ComputePerInstanceStatistics.epsilon, scores(scoredLabel.toInt))))
        } else {
          // penalize if no label seen in training
          -Math.log(ComputePerInstanceStatistics.epsilon)
        })
      val probabilitiesColumnName = SparkSchema.getScoredProbabilitiesColumnName(dataframe, modelName)
      dataframe.withColumn(ComputePerInstanceStatistics.LogLossMetric,
        logLossFunc(dataset(scoredLabelsColumnName), dataset(probabilitiesColumnName)))
    } else {
      val scoresColumnName = SparkSchema.getScoresColumnName(dataframe, modelName)
      // Compute the L1 and L2 loss for regression case
      val scoresAndLabels =
        dataset.select(col(scoresColumnName), col(labelColumnName).cast(DoubleType)).rdd.map {
          case Row(prediction: Double, label: Double) => (prediction, label)
        }
      val l1LossFunc = udf((trueLabel:Double, scoredLabel: Double) => math.abs(trueLabel - scoredLabel))
      val l2LossFunc = udf((trueLabel:Double, scoredLabel: Double) =>
        {
          val loss = math.abs(trueLabel - scoredLabel)
          loss * loss
        })
      dataframe.withColumn(ComputePerInstanceStatistics.L1LossMetric,
        l1LossFunc(dataset(labelColumnName), dataset(scoresColumnName)))
        .withColumn(ComputePerInstanceStatistics.L2LossMetric,
          l2LossFunc(dataset(labelColumnName), dataset(scoresColumnName)))
    }
  }

  private def getFirstModelName(colMetadata: Metadata): Option[String] = {
    if (!colMetadata.contains(MMLTag)) null
    else {
      val mlTagMetadata = colMetadata.getMetadata(MMLTag)
      val metadataKeys = MetadataUtilities.getMetadataKeys(mlTagMetadata)
      metadataKeys.find(key => key.startsWith(SchemaConstants.ScoreModelPrefix))
    }
  }

  override def copy(extra: ParamMap): Transformer = new ComputePerInstanceStatistics()

  // TODO: This should be based on the retrieved score value kind
  override def transformSchema(schema: StructType): StructType =
    schema.add(new StructField(ComputePerInstanceStatistics.L1LossMetric, DoubleType))
      .add(new StructField(ComputePerInstanceStatistics.L2LossMetric, DoubleType))

}
