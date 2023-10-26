// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.train

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts._
import com.microsoft.azure.synapse.ml.core.metrics.{MetricConstants, MetricUtils}
import com.microsoft.azure.synapse.ml.core.schema.{CategoricalUtilities, SchemaConstants, SparkSchema}
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object ComputePerInstanceStatistics extends DefaultParamsReadable[ComputePerInstanceStatistics] {
  val Epsilon = 1e-15
}

trait CPISParams extends Wrappable with DefaultParamsWritable
  with HasLabelCol with HasScoresCol with HasScoredLabelsCol with HasScoredProbabilitiesCol with HasEvaluationMetric {
  /** Param "evaluationMetric" is the metric to evaluate the models with. Default is "all"
    *
    *   If using a native Spark ML model, you will need to specify either "classifier" or "regressor"
    *     - classifier
    *     - regressor
    *
    * @group param
    */
  setDefault(evaluationMetric -> MetricConstants.AllSparkMetrics)
}

/** Evaluates the given scored dataset with per instance metrics.
  *
  * The Regression metrics are:
  * - L1_loss
  * - L2_loss
  *
  * The Classification metrics are:
  * - log_loss
  */
class ComputePerInstanceStatistics(override val uid: String) extends Transformer
  with CPISParams with SynapseMLLogging {
  logClass(FeatureNames.Core)

  def this() = this(Identifiable.randomUID("ComputePerInstanceStatistics"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      val (modelName, labelColumnName, scoreValueKind) =
        MetricUtils.getSchemaInfo(
          dataset.schema,
          if (isDefined(labelCol)) Some(getLabelCol) else None,
          getEvaluationMetric)

      val dataframe = dataset.toDF()

      if (scoreValueKind == SchemaConstants.ClassificationKind) {
        // Compute the LogLoss for classification case
        val scoredLabelsColumnName =
          if (isDefined(scoredLabelsCol)) getScoredLabelsCol
          else SparkSchema.getSparkPredictionColumnName(dataframe, modelName)

        // Get levels if categorical
        val levels = CategoricalUtilities.getLevels(dataframe.schema, labelColumnName)
        val numLevels =
          if (levels.isDefined && levels.get != null) {
            if (levels.get.length > 2) levels.get.length else 2
          } else {
            // Otherwise compute unique levels
            dataset.select(col(labelColumnName).cast(DoubleType)).rdd.distinct().count().toInt
          }

        val logLossFunc = udf((scoredLabel: Double, scores: org.apache.spark.ml.linalg.Vector) =>
          if (scoredLabel < numLevels) {
            -Math.log(Math.min(1, Math.max(ComputePerInstanceStatistics.Epsilon, scores(scoredLabel.toInt))))
          } else {
            // penalize if no label seen in training
            -Math.log(ComputePerInstanceStatistics.Epsilon)
          })
        val probabilitiesColumnName =
          if (isDefined(scoredProbabilitiesCol)) getScoredProbabilitiesCol
          else SparkSchema.getSparkProbabilityColumnName(dataframe, modelName)
        dataframe.withColumn(MetricConstants.LogLossMetric,
          logLossFunc(dataset(scoredLabelsColumnName), dataset(probabilitiesColumnName)))
      } else {
        val scoresColumnName =
          if (isDefined(scoresCol)) getScoresCol
          else SparkSchema.getSparkPredictionColumnName(dataframe, modelName)
        // Compute the L1 and L2 loss for regression case
        val l1LossFunc = udf((trueLabel: Double, scoredLabel: Double) => math.abs(trueLabel - scoredLabel))
        val l2LossFunc = udf((trueLabel: Double, scoredLabel: Double) => {
          val loss = math.abs(trueLabel - scoredLabel)
          loss * loss
        })
        dataframe.withColumn(MetricConstants.L1LossMetric,
          l1LossFunc(dataset(labelColumnName), dataset(scoresColumnName)))
          .withColumn(MetricConstants.L2LossMetric,
            l2LossFunc(dataset(labelColumnName), dataset(scoresColumnName)))
      }
    }, dataset.columns.length)
  }

  override def copy(extra: ParamMap): Transformer = new ComputePerInstanceStatistics()

  // TODO: This should be based on the retrieved score value kind
  override def transformSchema(schema: StructType): StructType =
    schema.add(StructField(MetricConstants.L1LossMetric, DoubleType))
          .add(StructField(MetricConstants.L2LossMetric, DoubleType))

}
