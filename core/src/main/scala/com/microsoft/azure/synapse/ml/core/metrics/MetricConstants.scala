// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.metrics

/** Contains constants used by modules for metrics. */
object MetricConstants {
  // Regression metrics
  val MseSparkMetric  = "mse"
  val RmseSparkMetric = "rmse"
  val R2SparkMetric   = "r2"
  val MaeSparkMetric  = "mae"
  val RegressionMetricsName = "regression"

  val RegressionMetrics: Set[String] = Set(MseSparkMetric, RmseSparkMetric, R2SparkMetric,
    MaeSparkMetric, RegressionMetricsName)

  // Binary Classification metrics
  val AreaUnderROCMetric   = "areaUnderROC"
  val AucSparkMetric       = "AUC"
  val AccuracySparkMetric  = "accuracy"
  val PrecisionSparkMetric = "precision"
  val RecallSparkMetric    = "recall"
  val ClassificationMetricsName = "classification"

  val ClassificationMetrics: Set[String] = Set(AreaUnderROCMetric, AucSparkMetric, AccuracySparkMetric,
    PrecisionSparkMetric, RecallSparkMetric, ClassificationMetricsName)

  val AllSparkMetrics      = "all"

  // Regression column names
  val MseColumnName  = "mean_squared_error"
  val RmseColumnName = "root_mean_squared_error"
  val R2ColumnName   = "R^2"
  val MaeColumnName  = "mean_absolute_error"

  // Binary Classification column names
  val AucColumnName = "AUC"

  // Binary and Multiclass (micro-averaged) column names
  val PrecisionColumnName = "precision"
  val RecallColumnName    = "recall"
  val AccuracyColumnName  = "accuracy"

  // Multiclass Classification column names
  val AverageAccuracy        = "average_accuracy"
  val MacroAveragedRecall    = "macro_averaged_recall"
  val MacroAveragedPrecision = "macro_averaged_precision"

  val ConfusionMatrix = "confusion_matrix"

  // Metric to column name
  val MetricToColumnName: Map[String, String] = Map(AccuracySparkMetric -> AccuracyColumnName,
    PrecisionSparkMetric -> PrecisionColumnName,
    RecallSparkMetric    -> RecallColumnName,
    MseSparkMetric       -> MseColumnName,
    RmseSparkMetric      -> RmseColumnName,
    R2SparkMetric        -> R2ColumnName,
    MaeSparkMetric       -> MaeColumnName)

  val ClassificationColumns = List(AccuracyColumnName, PrecisionColumnName, RecallColumnName)

  val RegressionColumns = List(MseColumnName, RmseColumnName, R2ColumnName, MaeColumnName)

  val ClassificationEvaluationType = "Classification"
  val EvaluationType = "evaluation_type"

  val FpRateROCColumnName = "false_positive_rate"
  val TpRateROCColumnName = "true_positive_rate"

  val FpRateROCLog = "fpr"
  val TpRateROCLog = "tpr"

  val BinningThreshold = 1000

  // Regression per instance metrics
  val L1LossMetric  = "L1_loss"
  val L2LossMetric  = "L2_loss"

  val RegressionPerInstanceMetrics: Set[String] = Set(RegressionMetricsName)

  // Classification per instance metrics
  val LogLossMetric = "log_loss"

  val ClassificationPerInstanceMetrics: Set[String] = Set(ClassificationMetricsName)

  val FindBestModelMetrics: Set[String] =
    Set(MetricConstants.MseSparkMetric,
        MetricConstants.RmseSparkMetric,
        MetricConstants.R2SparkMetric,
        MetricConstants.MaeSparkMetric,
        MetricConstants.AccuracySparkMetric,
        MetricConstants.PrecisionSparkMetric,
        MetricConstants.RecallSparkMetric,
        MetricConstants.AucSparkMetric)

}
