// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.exploratory

import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array, col}

class DistributionBalanceMeasureSuite extends DataBalanceTestBase with TransformerFuzzing[DistributionBalanceMeasure] {

  override def testObjects(): Seq[TestObject[DistributionBalanceMeasure]] = Seq(
    new TestObject(distributionBalanceMeasure, sensitiveFeaturesDf)
  )

  override def reader: MLReadable[_] = DistributionBalanceMeasure

  import DistributionMetrics._
  import spark.implicits._

  private def distributionBalanceMeasure: DistributionBalanceMeasure =
    new DistributionBalanceMeasure()
      .setSensitiveCols(features)
      .setVerbose(true)

  test("DistributionBalanceMeasure can calculate Distribution Balance Measures end-to-end") {
    val df = distributionBalanceMeasure.transform(sensitiveFeaturesDf)
    df.show(truncate = false)
    df.printSchema()
  }

  private def actual: DataFrame =
    new DistributionBalanceMeasure()
      .setSensitiveCols(features)
      .setVerbose(true)
      .transform(sensitiveFeaturesDf)

  private def actualFeature1: Map[String, Double] =
    METRICS zip actual.filter(col("FeatureName") === feature1)
      .select(array(col("DistributionBalanceMeasure.*")))
      .as[Array[Double]]
      .head toMap

  private def expectedFeature1 = getFeatureStats(sensitiveFeaturesDf.groupBy(feature1))
    .select(featureProbCol, featureCountCol)
    .as[(Double, Double)].collect()

  private object ExpectedFeature1 {
    // Values were computed using:
    // val CALC =
    //  DistributionMetricsCalculator(expectedFeature1.map(_._1), expectedFeature1.map(_._2), sensitiveFeaturesDf.count)
    val KLDIVERGENCE = 0.03775534151008829
    val JSDISTANCE = 0.09785224086736323
    val INFNORMDISTANCE = 0.1111111111111111
    val TOTALVARIATIONDISTANCE = 0.1111111111111111
    val WASSERSTEINDISTANCE = 0.07407407407407407
    val CHISQUAREDTESTSTATISTIC = 0.6666666666666666
    val CHISQUAREDPVALUE = 0.7165313105737893
  }

  test(s"DistributionBalanceMeasure can calculate Distribution Balance Measures for $feature1") {
    val actual = actualFeature1
    val expected = ExpectedFeature1
    assert(actual(KLDIVERGENCE) === expected.KLDIVERGENCE)
    assert(actual(JSDISTANCE) === expected.JSDISTANCE)
    assert(actual(INFNORMDISTANCE) === expected.INFNORMDISTANCE)
    assert(actual(TOTALVARIATIONDISTANCE) === expected.TOTALVARIATIONDISTANCE)
    assert(actual(WASSERSTEINDISTANCE) === expected.WASSERSTEINDISTANCE)
    assert(actual(CHISQUAREDTESTSTATISTIC) === expected.CHISQUAREDTESTSTATISTIC)
    assert(actual(CHISQUAREDPVALUE) === expected.CHISQUAREDPVALUE)
  }

  private def actualFeature2: Map[String, Double] =
    METRICS zip actual.filter(col("FeatureName") === feature2)
      .select(array(col("DistributionBalanceMeasure.*")))
      .as[Array[Double]]
      .head toMap

  private def expectedFeature2 = getFeatureStats(sensitiveFeaturesDf.groupBy(feature2))
    .select(featureProbCol, featureCountCol)
    .as[(Double, Double)].collect()

  private object ExpectedFeature2 {
    // Values were computed using:
    // val CALC =
    //  DistributionMetricsCalculator(expectedFeature2.map(_._1), expectedFeature2.map(_._2), sensitiveFeaturesDf.count)
    val KLDIVERGENCE = 0.07551068302017659
    val JSDISTANCE = 0.14172745151398888
    val INFNORMDISTANCE = 0.1388888888888889
    val TOTALVARIATIONDISTANCE = 0.16666666666666666
    val WASSERSTEINDISTANCE = 0.08333333333333333
    val CHISQUAREDTESTSTATISTIC = 1.222222222222222
    val CHISQUAREDPVALUE = 0.7476795872877147
  }

  test(s"DistributionBalanceMeasure can calculate Distribution Balance Measures for $feature2") {
    val actual = actualFeature2
    val expected = ExpectedFeature2
    assert(actual(KLDIVERGENCE) === expected.KLDIVERGENCE)
    assert(actual(JSDISTANCE) === expected.JSDISTANCE)
    assert(actual(INFNORMDISTANCE) === expected.INFNORMDISTANCE)
    assert(actual(TOTALVARIATIONDISTANCE) === expected.TOTALVARIATIONDISTANCE)
    assert(actual(WASSERSTEINDISTANCE) === expected.WASSERSTEINDISTANCE)
    assert(actual(CHISQUAREDTESTSTATISTIC) === expected.CHISQUAREDTESTSTATISTIC)
    assert(actual(CHISQUAREDPVALUE) === expected.CHISQUAREDPVALUE)
  }
}
