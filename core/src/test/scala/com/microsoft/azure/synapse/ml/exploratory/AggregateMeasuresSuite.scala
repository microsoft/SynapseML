// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.exploratory

import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.functions.col

import scala.math.abs

class AggregateMeasuresSuite extends DataBalanceTestBase with TransformerFuzzing[AggregateMeasures] {

  override def testObjects(): Seq[TestObject[AggregateMeasures]] = Seq(
    new TestObject(aggregateMeasures, sensitiveFeaturesDf)
  )

  override def reader: MLReadable[_] = AggregateMeasures

  import spark.implicits._

  private lazy val aggregateMeasures: AggregateMeasures =
    new AggregateMeasures()
      .setSensitiveCols(features)
      .setVerbose(true)

  test("AggregateMeasures can calculate Aggregate Measures end-to-end") {
    val df = aggregateMeasures.transform(sensitiveFeaturesDf)
    df.show(truncate = false)
    df.printSchema()
  }

  private lazy val actualOneFeature: Map[String, Double] =
    new AggregateMeasures()
      .setSensitiveCols(Array(feature1))
      .setVerbose(true)
      .transform(sensitiveFeaturesDf)
      .as[Map[String, Double]]
      .collect()(0)

  private lazy val actualOneFeatureDiffEpsilon: Map[String, Double] =
    new AggregateMeasures()
      .setSensitiveCols(Array(feature1))
      .setEpsilon(0.9)
      .setVerbose(true)
      .transform(sensitiveFeaturesDf)
      .as[Map[String, Double]]
      .collect()(0)

  private lazy val oneFeatureProbabilities = getProbabilitiesAndCounts(sensitiveFeaturesDf.groupBy(feature1))
    .select(featureProbCol).as[Double].collect()

  private lazy val expectedOneFeature = AggregateMeasureCalculator(oneFeatureProbabilities, 1d, 1e-12)

  private lazy val expectedOneFeatureDiffEpsilon = AggregateMeasureCalculator(oneFeatureProbabilities, 0.9, 1e-12)

  test("AggregateMeasures can calculate Atkinson Index for Default Epsilon (1.0) for 1 sensitive feature") {
    assert(abs(actualOneFeature("atkinson_index") - expectedOneFeature.atkinsonIndex) < errorTolerance)
  }

  test("AggregateMeasures can calculate Atkinson Index for Nondefault Epsilon (0.9) for 1 sensitive feature") {
    assert(abs(
      actualOneFeatureDiffEpsilon("atkinson_index") - expectedOneFeatureDiffEpsilon.atkinsonIndex) < errorTolerance)
  }

  test("AggregateMeasures can calculate Theil L Index for 1 sensitive feature") {
    assert(abs(actualOneFeature("theil_l_index") - expectedOneFeature.theilLIndex) < errorTolerance)
  }

  test("AggregateMeasures can calculate Theil T Index for 1 sensitive feature") {
    assert(abs(actualOneFeature("theil_t_index") - expectedOneFeature.theilTIndex) < errorTolerance)
  }

  private lazy val actualTwoFeatures: Map[String, Double] =
    new AggregateMeasures()
      .setSensitiveCols(features)
      .setVerbose(true)
      .transform(sensitiveFeaturesDf)
      .as[Map[String, Double]]
      .collect()(0)

  private lazy val actualTwoFeaturesDiffEpsilon: Map[String, Double] =
    new AggregateMeasures()
      .setSensitiveCols(features)
      .setEpsilon(0.9)
      .setVerbose(true)
      .transform(sensitiveFeaturesDf)
      .as[Map[String, Double]]
      .collect()(0)

  private lazy val twoFeaturesProbabilities =
    getProbabilitiesAndCounts(sensitiveFeaturesDf.groupBy(features map col: _*))
      .select(featureProbCol).as[Double].collect()

  private lazy val expectedBothFeatures = AggregateMeasureCalculator(twoFeaturesProbabilities, 1d, 1e-12)

  private lazy val expectedBothFeaturesDiffEpsilon = AggregateMeasureCalculator(twoFeaturesProbabilities, 0.9, 1e-12)

  test("AggregateMeasures can calculate Atkinson Index for Default Epsilon (1.0) for 2 sensitive features") {
    assert(abs(actualTwoFeatures("atkinson_index") - expectedBothFeatures.atkinsonIndex) < errorTolerance)
  }

  test("AggregateMeasures can calculate Atkinson Index for Nondefault Epsilon (0.9) for 2 sensitive features") {
    assert(abs(
      actualTwoFeaturesDiffEpsilon("atkinson_index") - expectedBothFeaturesDiffEpsilon.atkinsonIndex) < errorTolerance)
  }

  test("AggregateMeasures can calculate Theil L Index for 2 sensitive features") {
    assert(abs(actualTwoFeatures("theil_l_index") - expectedBothFeatures.theilLIndex) < errorTolerance)
  }

  test("AggregateMeasures can calculate Theil T Index for 2 sensitive features") {
    assert(abs(actualTwoFeatures("theil_t_index") - expectedBothFeatures.theilTIndex) < errorTolerance)
  }
}
