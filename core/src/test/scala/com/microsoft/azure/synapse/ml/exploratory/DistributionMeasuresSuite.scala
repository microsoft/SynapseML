// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.exploratory

import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class DistributionMeasuresSuite extends DataBalanceTestBase with TransformerFuzzing[DistributionMeasures] {

  override def testObjects(): Seq[TestObject[DistributionMeasures]] = Seq(
    new TestObject(distributionMeasures, sensitiveFeaturesDf)
  )

  override def reader: MLReadable[_] = DistributionMeasures

  import spark.implicits._

  private lazy val distributionMeasures: DistributionMeasures =
    new DistributionMeasures()
      .setSensitiveCols(features)
      .setVerbose(true)


  test("DistributionMeasures can calculate Distribution Measures end-to-end") {
    val df = distributionMeasures.transform(sensitiveFeaturesDf)
    df.show(truncate = false)
    df.printSchema()
  }

  private lazy val actual: DataFrame =
    new DistributionMeasures()
      .setSensitiveCols(features)
      .setVerbose(true)
      .transform(sensitiveFeaturesDf)

  private lazy val actualFeature1: Map[String, Double] =
    actual.filter(col("FeatureName") === feature1)
      .as[(String, Map[String, Double])]
      .collect()(0)._2

  private lazy val expectedFeature1 = {
    val values = getProbabilitiesAndCounts(sensitiveFeaturesDf.groupBy(feature1))
      .select(featureProbCol, featureCountCol)
      .as[(Double, Double)].collect()
    DistributionMeasureCalculator(values.map(_._1), values.map(_._2), sensitiveFeaturesDf.count)
  }

  test(s"DistributionMeasures can calculate Distribution Measures for $feature1") {
    assert(actualFeature1("kl_divergence") == expectedFeature1.klDivergence)
    assert(actualFeature1("js_dist") == expectedFeature1.jsDistance)
    assert(actualFeature1("inf_norm_dist") == expectedFeature1.infNormDistance)
    assert(actualFeature1("total_variation_dist") == expectedFeature1.totalVariationDistance)
    assert(actualFeature1("wasserstein_dist") == expectedFeature1.wassersteinDistance)
    assert(actualFeature1("chi_sq_stat") == expectedFeature1.chiSqTestStatistic)
    assert(actualFeature1("chi_sq_p_value") == expectedFeature1.chiSqPValue)
  }

  private lazy val actualFeature2: Map[String, Double] =
    actual.filter(col("FeatureName") === feature2)
      .as[(String, Map[String, Double])]
      .collect()(0)._2

  private lazy val expectedFeature2 = {
    val values = getProbabilitiesAndCounts(sensitiveFeaturesDf.groupBy(feature2))
      .select(featureProbCol, featureCountCol)
      .as[(Double, Double)].collect()
    DistributionMeasureCalculator(values.map(_._1), values.map(_._2), sensitiveFeaturesDf.count)
  }

  test(s"DistributionMeasures can calculate Distribution Measures for $feature2") {
    assert(actualFeature2("kl_divergence") == expectedFeature2.klDivergence)
    assert(actualFeature2("js_dist") == expectedFeature2.jsDistance)
    assert(actualFeature2("inf_norm_dist") == expectedFeature2.infNormDistance)
    assert(actualFeature2("total_variation_dist") == expectedFeature2.totalVariationDistance)
    assert(actualFeature2("wasserstein_dist") == expectedFeature2.wassersteinDistance)
    assert(actualFeature2("chi_sq_stat") == expectedFeature2.chiSqTestStatistic)
    assert(actualFeature2("chi_sq_p_value") == expectedFeature2.chiSqPValue)
  }
}
