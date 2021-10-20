// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.exploratory.imbalance

import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.functions.{array, col}

import scala.math.abs

class AggregateMeasuresSuite extends DataBalanceTestBase with TransformerFuzzing[AggregateMeasures] {

  override def testObjects(): Seq[TestObject[AggregateMeasures]] = Seq(
    new TestObject(aggregateMeasures, sensitiveFeaturesDf)
  )

  override def reader: MLReadable[_] = AggregateMeasures

  import AggregateMetrics._
  import spark.implicits._

  private def aggregateMeasures: AggregateMeasures =
    new AggregateMeasures()
      .setSensitiveCols(features)
      .setVerbose(true)

  test("AggregateMeasures can calculate Aggregate Measures end-to-end") {
    val df = aggregateMeasures.transform(sensitiveFeaturesDf)
    df.show(truncate = false)
    df.printSchema()
  }

  private def actualOneFeature: Map[String, Double] =
    METRICS zip new AggregateMeasures()
      .setSensitiveCols(Array(feature1))
      .setVerbose(true)
      .transform(sensitiveFeaturesDf)
      .select(array(col("AggregateMeasures.*")))
      .as[Array[Double]]
      .head toMap

  private def actualOneFeatureDiffEpsilon: Map[String, Double] =
    METRICS zip new AggregateMeasures()
      .setSensitiveCols(Array(feature1))
      .setEpsilon(0.9)
      .setVerbose(true)
      .transform(sensitiveFeaturesDf)
      .select(array(col("AggregateMeasures.*")))
      .as[Array[Double]]
      .head toMap

  private def oneFeatureProbabilities =
    getFeatureStats(sensitiveFeaturesDf.groupBy(feature1)).select(featureProbCol).as[Double].collect()

  private object ExpectedOneFeature {
    // Values were computed using:
    // val CALCULATOR = AggregateMeasureCalculator(oneFeatureProbabilities, 1d, 1e-12)
    val ATKINSONINDEX = 0.03850028646172776
    val THEILLINDEX = 0.039261011885461196
    val THEILTINDEX = 0.03775534151008828
  }

  private object ExpectedOneFeatureDiffEpsilon {
    // Values were computed using:
    // val CALCULATOR = AggregateMeasureCalculator(oneFeatureProbabilities, 0.9, 1e-12)
    val ATKINSONINDEX = 0.03461369487253463
  }

  test("AggregateMeasures can calculate Atkinson Index for Default Epsilon (1.0) for 1 sensitive feature") {
    assert(abs(actualOneFeature(ATKINSONINDEX) - ExpectedOneFeature.ATKINSONINDEX) < errorTolerance)
  }

  test("AggregateMeasures can calculate Atkinson Index for Nondefault Epsilon (0.9) for 1 sensitive feature") {
    assert(abs(
      actualOneFeatureDiffEpsilon(ATKINSONINDEX) - ExpectedOneFeatureDiffEpsilon.ATKINSONINDEX) < errorTolerance)
  }

  test("AggregateMeasures can calculate Theil L Index for 1 sensitive feature") {
    assert(abs(actualOneFeature(THEILLINDEX) - ExpectedOneFeature.THEILLINDEX) < errorTolerance)
  }

  test("AggregateMeasures can calculate Theil T Index for 1 sensitive feature") {
    assert(abs(actualOneFeature(THEILTINDEX) - ExpectedOneFeature.THEILTINDEX) < errorTolerance)
  }

  private def actualTwoFeatures: Map[String, Double] =
    METRICS zip new AggregateMeasures()
      .setSensitiveCols(features)
      .setVerbose(true)
      .transform(sensitiveFeaturesDf)
      .select(array(col("AggregateMeasures.*")))
      .as[Array[Double]]
      .head toMap


  private def actualTwoFeaturesDiffEpsilon: Map[String, Double] =
    METRICS zip new AggregateMeasures()
      .setSensitiveCols(features)
      .setEpsilon(0.9)
      .setVerbose(true)
      .transform(sensitiveFeaturesDf)
      .select(array(col("AggregateMeasures.*")))
      .as[Array[Double]]
      .head toMap


  private def twoFeaturesProbabilities =
    getFeatureStats(sensitiveFeaturesDf.groupBy(features map col: _*)).select(featureProbCol).as[Double].collect()

  private object ExpectedBothFeatures {
    // Values were computed using:
    // val CALCULATOR = AggregateMeasureCalculator(twoFeaturesProbabilities, 1d, 1e-12)
    val ATKINSONINDEX = 0.030659793186437745
    val THEILLINDEX = 0.03113963808639034
    val THEILTINDEX = 0.03624967113471546
  }

  private object ExpectedBothFeaturesDiffEpsilon {
    // Values were computed using:
    // val CALCULATOR = AggregateMeasureCalculator(twoFeaturesProbabilities, 0.9, 1e-12)
    val ATKINSONINDEX = 0.02806492487875245
  }

  test("AggregateMeasures can calculate Atkinson Index for Default Epsilon (1.0) for 2 sensitive features") {
    assert(abs(actualTwoFeatures(ATKINSONINDEX) - ExpectedBothFeatures.ATKINSONINDEX) < errorTolerance)
  }

  test("AggregateMeasures can calculate Atkinson Index for Nondefault Epsilon (0.9) for 2 sensitive features") {
    assert(abs(
      actualTwoFeaturesDiffEpsilon(ATKINSONINDEX) - ExpectedBothFeaturesDiffEpsilon.ATKINSONINDEX) < errorTolerance)
  }

  test("AggregateMeasures can calculate Theil L Index for 2 sensitive features") {
    assert(abs(actualTwoFeatures(THEILLINDEX) - ExpectedBothFeatures.THEILLINDEX) < errorTolerance)
  }

  test("AggregateMeasures can calculate Theil T Index for 2 sensitive features") {
    assert(abs(actualTwoFeatures(THEILTINDEX) - ExpectedBothFeatures.THEILTINDEX) < errorTolerance)
  }
}
