// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.automl

import com.microsoft.azure.synapse.ml.core.metrics.MetricConstants
import com.microsoft.azure.synapse.ml.core.schema.SchemaConstants
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.regression.{DecisionTreeRegressor, GBTRegressor, LinearRegression, RandomForestRegressor}

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

  test("getMetricWithOperator returns correct metric for classification accuracy") {
    val (name, _) = EvaluationUtils.getMetricWithOperator(
      SchemaConstants.ClassificationKind, MetricConstants.AccuracySparkMetric)
    assert(name === MetricConstants.AccuracyColumnName)
  }

  test("classification metrics use chooseHighest ordering") {
    val (_, aucOrd) = EvaluationUtils.getMetricWithOperator(
      SchemaConstants.ClassificationKind, MetricConstants.AucSparkMetric)
    // AUC should prefer higher values
    assert(aucOrd.compare(1.0, 2.0) < 0)
  }

  test("getMetricWithOperator throws for unsupported regression metric") {
    assertThrows[Exception] {
      EvaluationUtils.getMetricWithOperator(
        SchemaConstants.RegressionKind,
        "unsupported_metric"
      )
    }
  }

  test("unsupported regression metric throws") {
    assertThrows[Exception] {
      EvaluationUtils.getMetricWithOperator(SchemaConstants.RegressionKind, "bogus_metric")
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

  test("unsupported classification metric throws") {
    assertThrows[Exception] {
      EvaluationUtils.getMetricWithOperator(SchemaConstants.ClassificationKind, "bogus_metric")
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

  test("unsupported model type throws") {
    assertThrows[Exception] {
      EvaluationUtils.getMetricWithOperator("unsupported_type", MetricConstants.MseSparkMetric)
    }
  }

  test("getModelType returns ClassificationKind for a Classifier") {
    assert(EvaluationUtils.getModelType(new LogisticRegression()) === SchemaConstants.ClassificationKind)
  }

  test("getModelType returns RegressionKind for LinearRegression") {
    assert(EvaluationUtils.getModelType(new LinearRegression()) === SchemaConstants.RegressionKind)
  }

  test("getModelType returns RegressionKind for DecisionTreeRegressor") {
    assert(EvaluationUtils.getModelType(new DecisionTreeRegressor()) === SchemaConstants.RegressionKind)
  }

  test("getModelType returns RegressionKind for GBTRegressor") {
    assert(EvaluationUtils.getModelType(new GBTRegressor()) === SchemaConstants.RegressionKind)
  }

  test("getModelType returns RegressionKind for RandomForestRegressor") {
    assert(EvaluationUtils.getModelType(new RandomForestRegressor()) === SchemaConstants.RegressionKind)
  }

  test("getModelType throws ModelTypeUnsupportedErr for an unsupported stage") {
    val caught = intercept[Exception] {
      EvaluationUtils.getModelType(new Tokenizer())
    }
    assert(caught.getMessage === EvaluationUtils.ModelTypeUnsupportedErr)
  }
}
