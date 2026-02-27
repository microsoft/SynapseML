// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.metrics

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

class VerifyMetricConstants extends TestBase {

  // Regression metrics tests
  test("regression metric constants have expected values") {
    assert(MetricConstants.MseSparkMetric === "mse")
    assert(MetricConstants.RmseSparkMetric === "rmse")
    assert(MetricConstants.R2SparkMetric === "r2")
    assert(MetricConstants.MaeSparkMetric === "mae")
    assert(MetricConstants.RegressionMetricsName === "regression")
  }

  test("RegressionMetrics set contains all regression metrics") {
    assert(MetricConstants.RegressionMetrics.contains(MetricConstants.MseSparkMetric))
    assert(MetricConstants.RegressionMetrics.contains(MetricConstants.RmseSparkMetric))
    assert(MetricConstants.RegressionMetrics.contains(MetricConstants.R2SparkMetric))
    assert(MetricConstants.RegressionMetrics.contains(MetricConstants.MaeSparkMetric))
    assert(MetricConstants.RegressionMetrics.contains(MetricConstants.RegressionMetricsName))
    assert(MetricConstants.RegressionMetrics.size === 5)
  }

  // Classification metrics tests
  test("classification metric constants have expected values") {
    assert(MetricConstants.AreaUnderROCMetric === "areaUnderROC")
    assert(MetricConstants.AucSparkMetric === "AUC")
    assert(MetricConstants.AccuracySparkMetric === "accuracy")
    assert(MetricConstants.PrecisionSparkMetric === "precision")
    assert(MetricConstants.RecallSparkMetric === "recall")
    assert(MetricConstants.ClassificationMetricsName === "classification")
  }

  test("ClassificationMetrics set contains all classification metrics") {
    assert(MetricConstants.ClassificationMetrics.contains(MetricConstants.AreaUnderROCMetric))
    assert(MetricConstants.ClassificationMetrics.contains(MetricConstants.AucSparkMetric))
    assert(MetricConstants.ClassificationMetrics.contains(MetricConstants.AccuracySparkMetric))
    assert(MetricConstants.ClassificationMetrics.contains(MetricConstants.PrecisionSparkMetric))
    assert(MetricConstants.ClassificationMetrics.contains(MetricConstants.RecallSparkMetric))
    assert(MetricConstants.ClassificationMetrics.contains(MetricConstants.ClassificationMetricsName))
    assert(MetricConstants.ClassificationMetrics.size === 6)
  }

  test("AllSparkMetrics constant") {
    assert(MetricConstants.AllSparkMetrics === "all")
  }

  // Column name tests
  test("regression column names have expected values") {
    assert(MetricConstants.MseColumnName === "mean_squared_error")
    assert(MetricConstants.RmseColumnName === "root_mean_squared_error")
    assert(MetricConstants.R2ColumnName === "R^2")
    assert(MetricConstants.MaeColumnName === "mean_absolute_error")
  }

  test("classification column names have expected values") {
    assert(MetricConstants.AucColumnName === "AUC")
    assert(MetricConstants.PrecisionColumnName === "precision")
    assert(MetricConstants.RecallColumnName === "recall")
    assert(MetricConstants.AccuracyColumnName === "accuracy")
  }

  test("multiclass column names have expected values") {
    assert(MetricConstants.AverageAccuracy === "average_accuracy")
    assert(MetricConstants.MacroAveragedRecall === "macro_averaged_recall")
    assert(MetricConstants.MacroAveragedPrecision === "macro_averaged_precision")
    assert(MetricConstants.ConfusionMatrix === "confusion_matrix")
  }

  // MetricToColumnName mapping tests
  test("MetricToColumnName contains correct mappings") {
    assert(MetricConstants.MetricToColumnName(MetricConstants.AccuracySparkMetric) ===
      MetricConstants.AccuracyColumnName)
    assert(MetricConstants.MetricToColumnName(MetricConstants.PrecisionSparkMetric) ===
      MetricConstants.PrecisionColumnName)
    assert(MetricConstants.MetricToColumnName(MetricConstants.RecallSparkMetric) ===
      MetricConstants.RecallColumnName)
    assert(MetricConstants.MetricToColumnName(MetricConstants.MseSparkMetric) ===
      MetricConstants.MseColumnName)
    assert(MetricConstants.MetricToColumnName(MetricConstants.RmseSparkMetric) ===
      MetricConstants.RmseColumnName)
    assert(MetricConstants.MetricToColumnName(MetricConstants.R2SparkMetric) ===
      MetricConstants.R2ColumnName)
    assert(MetricConstants.MetricToColumnName(MetricConstants.MaeSparkMetric) ===
      MetricConstants.MaeColumnName)
  }

  // Column lists tests
  test("ClassificationColumns contains expected columns") {
    assert(MetricConstants.ClassificationColumns === List(
      MetricConstants.AccuracyColumnName,
      MetricConstants.PrecisionColumnName,
      MetricConstants.RecallColumnName))
  }

  test("RegressionColumns contains expected columns") {
    assert(MetricConstants.RegressionColumns === List(
      MetricConstants.MseColumnName,
      MetricConstants.RmseColumnName,
      MetricConstants.R2ColumnName,
      MetricConstants.MaeColumnName))
  }

  // Evaluation type tests
  test("evaluation type constants have expected values") {
    assert(MetricConstants.ClassificationEvaluationType === "Classification")
    assert(MetricConstants.EvaluationType === "evaluation_type")
  }

  // ROC column names tests
  test("ROC column names have expected values") {
    assert(MetricConstants.FpRateROCColumnName === "false_positive_rate")
    assert(MetricConstants.TpRateROCColumnName === "true_positive_rate")
    assert(MetricConstants.FpRateROCLog === "fpr")
    assert(MetricConstants.TpRateROCLog === "tpr")
  }

  test("BinningThreshold has expected value") {
    assert(MetricConstants.BinningThreshold === 1000)
  }

  // Per instance metrics tests
  test("per instance metric constants have expected values") {
    assert(MetricConstants.L1LossMetric === "L1_loss")
    assert(MetricConstants.L2LossMetric === "L2_loss")
    assert(MetricConstants.LogLossMetric === "log_loss")
  }

  test("RegressionPerInstanceMetrics contains expected metrics") {
    assert(MetricConstants.RegressionPerInstanceMetrics.contains(MetricConstants.RegressionMetricsName))
    assert(MetricConstants.RegressionPerInstanceMetrics.size === 1)
  }

  test("ClassificationPerInstanceMetrics contains expected metrics") {
    assert(MetricConstants.ClassificationPerInstanceMetrics.contains(MetricConstants.ClassificationMetricsName))
    assert(MetricConstants.ClassificationPerInstanceMetrics.size === 1)
  }

  // FindBestModelMetrics tests
  test("FindBestModelMetrics contains all expected metrics") {
    assert(MetricConstants.FindBestModelMetrics.contains(MetricConstants.MseSparkMetric))
    assert(MetricConstants.FindBestModelMetrics.contains(MetricConstants.RmseSparkMetric))
    assert(MetricConstants.FindBestModelMetrics.contains(MetricConstants.R2SparkMetric))
    assert(MetricConstants.FindBestModelMetrics.contains(MetricConstants.MaeSparkMetric))
    assert(MetricConstants.FindBestModelMetrics.contains(MetricConstants.AccuracySparkMetric))
    assert(MetricConstants.FindBestModelMetrics.contains(MetricConstants.PrecisionSparkMetric))
    assert(MetricConstants.FindBestModelMetrics.contains(MetricConstants.RecallSparkMetric))
    assert(MetricConstants.FindBestModelMetrics.contains(MetricConstants.AucSparkMetric))
    assert(MetricConstants.FindBestModelMetrics.size === 8)
  }
}
