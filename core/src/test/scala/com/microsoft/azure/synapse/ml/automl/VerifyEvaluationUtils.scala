// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.automl

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.metrics.MetricConstants
import com.microsoft.azure.synapse.ml.core.schema.SchemaConstants

class VerifyEvaluationUtils extends TestBase {

  test("getMetricWithOperator returns correct metric for regression MSE") {
    val (name, _) = EvaluationUtils.getMetricWithOperator(
      SchemaConstants.RegressionKind, MetricConstants.MseSparkMetric)
    assert(name === MetricConstants.MseColumnName)
  }

  test("getMetricWithOperator returns correct metric for regression RMSE") {
    val (name, _) = EvaluationUtils.getMetricWithOperator(
      SchemaConstants.RegressionKind, MetricConstants.RmseSparkMetric)
    assert(name === MetricConstants.RmseColumnName)
  }

  test("getMetricWithOperator returns correct metric for regression R2") {
    val (name, _) = EvaluationUtils.getMetricWithOperator(
      SchemaConstants.RegressionKind, MetricConstants.R2SparkMetric)
    assert(name === MetricConstants.R2ColumnName)
  }

  test("getMetricWithOperator returns correct metric for regression MAE") {
    val (name, _) = EvaluationUtils.getMetricWithOperator(
      SchemaConstants.RegressionKind, MetricConstants.MaeSparkMetric)
    assert(name === MetricConstants.MaeColumnName)
  }

  test("getMetricWithOperator returns correct metric for classification AUC") {
    val (name, _) = EvaluationUtils.getMetricWithOperator(
      SchemaConstants.ClassificationKind, MetricConstants.AucSparkMetric)
    assert(name === MetricConstants.AucColumnName)
  }

  test("getMetricWithOperator returns correct metric for classification accuracy") {
    val (name, _) = EvaluationUtils.getMetricWithOperator(
      SchemaConstants.ClassificationKind, MetricConstants.AccuracySparkMetric)
    assert(name === MetricConstants.AccuracyColumnName)
  }

  test("regression metrics use chooseLowest ordering (except R2)") {
    val (_, mseOrd) = EvaluationUtils.getMetricWithOperator(
      SchemaConstants.RegressionKind, MetricConstants.MseSparkMetric)
    // MSE should prefer lower values
    assert(mseOrd.compare(1.0, 2.0) > 0)

    val (_, r2Ord) = EvaluationUtils.getMetricWithOperator(
      SchemaConstants.RegressionKind, MetricConstants.R2SparkMetric)
    // R2 should prefer higher values
    assert(r2Ord.compare(1.0, 2.0) < 0)
  }

  test("classification metrics use chooseHighest ordering") {
    val (_, aucOrd) = EvaluationUtils.getMetricWithOperator(
      SchemaConstants.ClassificationKind, MetricConstants.AucSparkMetric)
    // AUC should prefer higher values
    assert(aucOrd.compare(1.0, 2.0) < 0)
  }

  test("unsupported regression metric throws") {
    assertThrows[Exception] {
      EvaluationUtils.getMetricWithOperator(SchemaConstants.RegressionKind, "bogus_metric")
    }
  }

  test("unsupported classification metric throws") {
    assertThrows[Exception] {
      EvaluationUtils.getMetricWithOperator(SchemaConstants.ClassificationKind, "bogus_metric")
    }
  }

  test("unsupported model type throws") {
    assertThrows[Exception] {
      EvaluationUtils.getMetricWithOperator("unsupported_type", MetricConstants.MseSparkMetric)
    }
  }

  test("ModelTypeUnsupportedErr constant is defined") {
    assert(EvaluationUtils.ModelTypeUnsupportedErr.nonEmpty)
  }
}
