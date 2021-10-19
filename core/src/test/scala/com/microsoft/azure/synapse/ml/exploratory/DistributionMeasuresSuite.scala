package com.microsoft.azure.synapse.ml.exploratory

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class DistributionMeasuresSuite extends DataImbalanceTestBase {

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

  test(s"DistributionMeasures can calculate Kullback–Leibler divergence for $feature1") {
    assert(actualFeature1("kl_divergence") == expectedFeature1.klDivergence)
  }

  test(s"DistributionMeasures can calculate Jensen-Shannon distance for $feature1") {
    assert(actualFeature1("js_dist") == expectedFeature1.jsDistance)
  }

  test(s"DistributionMeasures can calculate Infinity norm distance for $feature1") {
    assert(actualFeature1("inf_norm_dist") == expectedFeature1.infNormDistance)
  }

  test(s"DistributionMeasures can calculate Total variation distance for $feature1") {
    assert(actualFeature1("total_variation_dist") == expectedFeature1.totalVariationDistance)
  }

  test(s"DistributionMeasures can calculate Wasserstein distance for $feature1") {
    assert(actualFeature1("wasserstein_dist") == expectedFeature1.wassersteinDistance)
  }

  test(s"DistributionMeasures can calculate Chi-square test statistic for $feature1") {
    assert(actualFeature1("chi_sq_stat") == expectedFeature1.chiSqTestStatistic)
  }

  test(s"DistributionMeasures can calculate Chi-square p value for $feature1") {
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

  test(s"DistributionMeasures can calculate Kullback–Leibler divergence for $feature2") {
    assert(actualFeature2("kl_divergence") == expectedFeature2.klDivergence)
  }

  test(s"DistributionMeasures can calculate Jensen-Shannon distance for $feature2") {
    assert(actualFeature2("js_dist") == expectedFeature2.jsDistance)
  }

  test(s"DistributionMeasures can calculate Infinity norm distance for $feature2") {
    assert(actualFeature2("inf_norm_dist") == expectedFeature2.infNormDistance)
  }

  test(s"DistributionMeasures can calculate Total variation distance for $feature2") {
    assert(actualFeature2("total_variation_dist") == expectedFeature2.totalVariationDistance)
  }

  test(s"DistributionMeasures can calculate Wasserstein distance for $feature2") {
    assert(actualFeature2("wasserstein_dist") == expectedFeature2.wassersteinDistance)
  }

  test(s"DistributionMeasures can calculate Chi-square test statistic for $feature2") {
    assert(actualFeature2("chi_sq_stat") == expectedFeature2.chiSqTestStatistic)
  }

  test(s"DistributionMeasures can calculate Chi-square p value for $feature2") {
    assert(actualFeature2("chi_sq_p_value") == expectedFeature2.chiSqPValue)
  }
}
