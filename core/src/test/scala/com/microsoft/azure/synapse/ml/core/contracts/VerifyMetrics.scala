// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.contracts

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

class VerifyMetrics extends TestBase {

  test("TypedMetric stores name and value correctly") {
    val metric = TypedMetric("accuracy", 0.95)
    assert(metric.name === "accuracy")
    assert(metric.value === 0.95)
  }

  test("TypedMetric works with different types") {
    val doubleMetric = TypedMetric[Double]("score", 1.5)
    val stringMetric = TypedMetric[String]("label", "positive")
    val intMetric = TypedMetric[Int]("count", 42)

    assert(doubleMetric.value === 1.5)
    assert(stringMetric.value === "positive")
    assert(intMetric.value === 42)
  }

  test("DoubleMetric stores name and double value") {
    val metric = DoubleMetric("precision", 0.85)
    assert(metric.name === "precision")
    assert(metric.value === 0.85)
  }

  test("StringMetric stores name and string value") {
    val metric = StringMetric("category", "classification")
    assert(metric.name === "category")
    assert(metric.value === "classification")
  }

  test("IntegralMetric stores name and long value") {
    val metric = IntegralMetric("count", 1000L)
    assert(metric.name === "count")
    assert(metric.value === 1000L)
  }

  test("TypenameMetricGroup stores name and values map") {
    val metrics = Map(
      "group1" -> Seq(DoubleMetric("m1", 1.0), DoubleMetric("m2", 2.0)),
      "group2" -> Seq(StringMetric("s1", "test"))
    )
    val group = TypenameMetricGroup("myGroup", metrics)
    assert(group.name === "myGroup")
    assert(group.values.size === 2)
    assert(group.values("group1").length === 2)
  }

  test("MetricData stores data, metricType, and modelName") {
    val data = Map("accuracy" -> Seq(0.9, 0.91, 0.92))
    val metricData = MetricData(data, "classification", "logisticRegression")

    assert(metricData.data === data)
    assert(metricData.metricType === "classification")
    assert(metricData.modelName === "logisticRegression")
  }

  test("MetricData.create converts single values to sequences") {
    val singleValues = Map("accuracy" -> 0.95, "precision" -> 0.90)
    val metricData = MetricData.create(singleValues, "classification", "svm")

    assert(metricData.data("accuracy") === List(0.95))
    assert(metricData.data("precision") === List(0.90))
    assert(metricData.metricType === "classification")
    assert(metricData.modelName === "svm")
  }

  test("MetricData.createTable preserves sequences") {
    val tableData = Map(
      "mse" -> Seq(0.1, 0.2, 0.3),
      "rmse" -> Seq(0.316, 0.447, 0.548)
    )
    val metricData = MetricData.createTable(tableData, "regression", "linearRegression")

    assert(metricData.data("mse") === Seq(0.1, 0.2, 0.3))
    assert(metricData.data("rmse").length === 3)
    assert(metricData.metricType === "regression")
    assert(metricData.modelName === "linearRegression")
  }

  test("MetricData.create handles empty map") {
    val metricData = MetricData.create(Map.empty[String, Double], "test", "model")
    assert(metricData.data.isEmpty)
  }

  test("MetricData.createTable handles empty map") {
    val metricData = MetricData.createTable(Map.empty[String, Seq[Double]], "test", "model")
    assert(metricData.data.isEmpty)
  }

  test("ConvenienceTypes type aliases work correctly") {
    import ConvenienceTypes._
    val name: UniqueName = "testMetric"
    val table: MetricTable = Map(name -> Seq(TypedMetric("m1", 1.0)))

    assert(name === "testMetric")
    assert(table.contains("testMetric"))
  }
}
