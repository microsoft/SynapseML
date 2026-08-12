// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.automl

import com.microsoft.azure.synapse.ml.core.metrics.MetricConstants
import com.microsoft.azure.synapse.ml.core.schema.SchemaConstants
import com.microsoft.azure.synapse.ml.core.test.base.TestBase

class VerifyEvaluationUtils extends TestBase {

  test("ModelTypeUnsupportedErr constant has expected value") {
    assert(EvaluationUtils.ModelTypeUnsupportedErr === "Model type not supported for evaluation")
  }

  test("getMetricWithOperator returns correct metric for regression MSE") {
    val (metricName, ordering) = EvaluationUtils.getMetricWithOperator(
      SchemaConstants.RegressionKind,
      MetricConstants.MseSparkMetric
    )
    assert(metricName === MetricConstants.MseColumnName)
    // MSE should use lowest (reverse ordering)
    assert(ordering.compare(1.0, 2.0) > 0) // 1.0 is "better" than 2.0 for MSE
  }

  test("getMetricWithOperator returns correct metric for regression RMSE") {
    val (metricName, ordering) = EvaluationUtils.getMetricWithOperator(
      SchemaConstants.RegressionKind,
      MetricConstants.RmseSparkMetric
    )
    assert(metricName === MetricConstants.RmseColumnName)
    // RMSE should use lowest
    assert(ordering.compare(1.0, 2.0) > 0)
  }

  test("getMetricWithOperator returns correct metric for regression R2") {
    val (metricName, ordering) = EvaluationUtils.getMetricWithOperator(
      SchemaConstants.RegressionKind,
      MetricConstants.R2SparkMetric
    )
    assert(metricName === MetricConstants.R2ColumnName)
    // R2 should use highest
    assert(ordering.compare(2.0, 1.0) > 0) // 2.0 is "better" than 1.0 for R2
  }

  test("getMetricWithOperator returns correct metric for regression MAE") {
    val (metricName, ordering) = EvaluationUtils.getMetricWithOperator(
      SchemaConstants.RegressionKind,
      MetricConstants.MaeSparkMetric
    )
    assert(metricName === MetricConstants.MaeColumnName)
    // MAE should use lowest
    assert(ordering.compare(1.0, 2.0) > 0)
  }

  test("getMetricWithOperator returns correct metric for classification AUC") {
    val (metricName, ordering) = EvaluationUtils.getMetricWithOperator(
      SchemaConstants.ClassificationKind,
      MetricConstants.AucSparkMetric
    )
    assert(metricName === MetricConstants.AucColumnName)
    // AUC should use highest
    assert(ordering.compare(2.0, 1.0) > 0)
  }

  test("getMetricWithOperator returns correct metric for classification Precision") {
    val (metricName, ordering) = EvaluationUtils.getMetricWithOperator(
      SchemaConstants.ClassificationKind,
      MetricConstants.PrecisionSparkMetric
    )
    assert(metricName === MetricConstants.PrecisionColumnName)
    // Precision should use highest
    assert(ordering.compare(2.0, 1.0) > 0)
  }

  test("getMetricWithOperator returns correct metric for classification Recall") {
    val (metricName, ordering) = EvaluationUtils.getMetricWithOperator(
      SchemaConstants.ClassificationKind,
      MetricConstants.RecallSparkMetric
    )
    assert(metricName === MetricConstants.RecallColumnName)
    // Recall should use highest
    assert(ordering.compare(2.0, 1.0) > 0)
  }

  test("getMetricWithOperator returns correct metric for classification Accuracy") {
    val (metricName, ordering) = EvaluationUtils.getMetricWithOperator(
      SchemaConstants.ClassificationKind,
      MetricConstants.AccuracySparkMetric
    )
    assert(metricName === MetricConstants.AccuracyColumnName)
    // Accuracy should use highest
    assert(ordering.compare(2.0, 1.0) > 0)
  }

  test("getMetricWithOperator throws for unsupported regression metric") {
    assertThrows[Exception] {
      EvaluationUtils.getMetricWithOperator(
        SchemaConstants.RegressionKind,
        "unsupported_metric"
      )
    }
  }

  test("getMetricWithOperator throws for unsupported classification metric") {
    assertThrows[Exception] {
      EvaluationUtils.getMetricWithOperator(
        SchemaConstants.ClassificationKind,
        "unsupported_metric"
      )
    }
  }

  test("getMetricWithOperator throws for unsupported model type") {
    assertThrows[Exception] {
      EvaluationUtils.getMetricWithOperator(
        "unsupported_model_type",
        MetricConstants.MseSparkMetric
      )
    }
  }
}
