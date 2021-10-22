// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.exploratory

import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.functions.{array, col}

class AggregateBalanceMeasureSuite extends DataBalanceTestBase with TransformerFuzzing[AggregateBalanceMeasure] {

  override def testObjects(): Seq[TestObject[AggregateBalanceMeasure]] = Seq(
    new TestObject(aggregateBalanceMeasure, sensitiveFeaturesDf)
  )

  override def reader: MLReadable[_] = AggregateBalanceMeasure

  import AggregateMetrics._
  import spark.implicits._

  private def aggregateBalanceMeasure: AggregateBalanceMeasure =
    new AggregateBalanceMeasure()
      .setSensitiveCols(features)
      .setVerbose(true)

  test("AggregateBalanceMeasure can calculate Aggregate Balance Measures end-to-end") {
    val df = aggregateBalanceMeasure.transform(sensitiveFeaturesDf)
    df.show(truncate = false)
    df.printSchema()
  }

  private def actualOneFeature: Map[String, Double] =
    METRICS zip new AggregateBalanceMeasure()
      .setSensitiveCols(Array(feature1))
      .setVerbose(true)
      .transform(sensitiveFeaturesDf)
      .select(array(col("AggregateBalanceMeasure.*")))
      .as[Array[Double]]
      .head toMap

  private def actualOneFeatureDiffEpsilon: Map[String, Double] =
    METRICS zip new AggregateBalanceMeasure()
      .setSensitiveCols(Array(feature1))
      .setEpsilon(0.9)
      .setVerbose(true)
      .transform(sensitiveFeaturesDf)
      .select(array(col("AggregateBalanceMeasure.*")))
      .as[Array[Double]]
      .head toMap

  private def oneFeatureProbabilities =
    getFeatureStats(sensitiveFeaturesDf.groupBy(feature1)).select(featureProbCol).as[Double].collect()

  private object ExpectedOneFeature {
    // Values were computed using:
    // val CALC = AggregateMetricsCalculator(oneFeatureProbabilities, 1d, 1e-12)
    val ATKINSONINDEX = 0.03850028646172776
    val THEILLINDEX = 0.039261011885461196
    val THEILTINDEX = 0.03775534151008828
  }

  private object ExpectedOneFeatureDiffEpsilon {
    // Values were computed using:
    // val CALC = AggregateMetricsCalculator(oneFeatureProbabilities, 0.9, 1e-12)
    val ATKINSONINDEX = 0.03461369487253463
  }

  test("AggregateBalanceMeasure can calculate Atkinson Index for Default Epsilon (1.0) for 1 sensitive feature") {
    assert(actualOneFeature(ATKINSONINDEX) === ExpectedOneFeature.ATKINSONINDEX)
  }

  test("AggregateBalanceMeasure can calculate Atkinson Index for Nondefault Epsilon (0.9) for 1 sensitive feature") {
    assert(actualOneFeatureDiffEpsilon(ATKINSONINDEX) === ExpectedOneFeatureDiffEpsilon.ATKINSONINDEX)
  }

  test("AggregateBalanceMeasure can calculate Theil L Index for 1 sensitive feature") {
    assert(actualOneFeature(THEILLINDEX) === ExpectedOneFeature.THEILLINDEX)
  }

  test("AggregateBalanceMeasure can calculate Theil T Index for 1 sensitive feature") {
    assert(actualOneFeature(THEILTINDEX) === ExpectedOneFeature.THEILTINDEX)
  }

  private def actualTwoFeatures: Map[String, Double] =
    METRICS zip new AggregateBalanceMeasure()
      .setSensitiveCols(features)
      .setVerbose(true)
      .transform(sensitiveFeaturesDf)
      .select(array(col("AggregateBalanceMeasure.*")))
      .as[Array[Double]]
      .head toMap


  private def actualTwoFeaturesDiffEpsilon: Map[String, Double] =
    METRICS zip new AggregateBalanceMeasure()
      .setSensitiveCols(features)
      .setEpsilon(0.9)
      .setVerbose(true)
      .transform(sensitiveFeaturesDf)
      .select(array(col("AggregateBalanceMeasure.*")))
      .as[Array[Double]]
      .head toMap


  private def twoFeaturesProbabilities =
    getFeatureStats(sensitiveFeaturesDf.groupBy(features map col: _*)).select(featureProbCol).as[Double].collect()

  private object ExpectedBothFeatures {
    // Values were computed using:
    // val CALC = AggregateMetricsCalculator(twoFeaturesProbabilities, 1d, 1e-12)
    val ATKINSONINDEX = 0.030659793186437745
    val THEILLINDEX = 0.03113963808639034
    val THEILTINDEX = 0.03624967113471546
  }

  private object ExpectedBothFeaturesDiffEpsilon {
    // Values were computed using:
    // val CALC = AggregateMetricsCalculator(twoFeaturesProbabilities, 0.9, 1e-12)
    val ATKINSONINDEX = 0.02806492487875245
  }

  test("AggregateBalanceMeasure can calculate Atkinson Index for Default Epsilon (1.0) for 2 sensitive features") {
    assert(actualTwoFeatures(ATKINSONINDEX) === ExpectedBothFeatures.ATKINSONINDEX)
  }

  test("AggregateBalanceMeasure can calculate Atkinson Index for Nondefault Epsilon (0.9) for 2 sensitive features") {
    assert(actualTwoFeaturesDiffEpsilon(ATKINSONINDEX) === ExpectedBothFeaturesDiffEpsilon.ATKINSONINDEX)
  }

  test("AggregateBalanceMeasure can calculate Theil L Index for 2 sensitive features") {
    assert(actualTwoFeatures(THEILLINDEX) === ExpectedBothFeatures.THEILLINDEX)
  }

  test("AggregateBalanceMeasure can calculate Theil T Index for 2 sensitive features") {
    assert(actualTwoFeatures(THEILTINDEX) === ExpectedBothFeatures.THEILTINDEX)
  }
}
