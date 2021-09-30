package com.microsoft.ml.spark.exploratory

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, lit}
import org.apache.spark.sql.types.DoubleType

class DistributionMeasuresSuite extends DataImbalanceTestBase {

  import spark.implicits._

  private lazy val distributionMeasureSimulated: DistributionMeasures = {
    spark
    new DistributionMeasures()
      .setSensitiveCols(Array("Gender", "Ethnicity"))
      .setVerbose(true)
  }

  test("DistributionMeasures can calculate Distribution Measures end-to-end") {
    val distributionMeasures = distributionMeasureSimulated.transform(sensitiveFeaturesDf)
    distributionMeasures.show(truncate = false)
    distributionMeasures.printSchema()
  }

  private lazy val expectedSensitiveFeature1 = DistributionMeasureCalculator(
    sensitiveFeaturesDf
      .groupBy("Gender")
      .agg(count("*").cast(DoubleType).alias("countSensitive"))
      .withColumn("countAll", lit(sensitiveFeaturesDf.count.toDouble))
      .withColumn("countSensitiveProb", col("countSensitive") / col("countAll"))
      .select("countSensitiveProb").as[Double].collect(), sensitiveFeaturesDf.count)

  private lazy val expectedSensitiveFeature2 = DistributionMeasureCalculator(
    sensitiveFeaturesDf
      .groupBy("Ethnicity")
      .agg(count("*").cast(DoubleType).alias("countSensitive"))
      .withColumn("countAll", lit(sensitiveFeaturesDf.count.toDouble))
      .withColumn("countSensitiveProb", col("countSensitive") / col("countAll"))
      .select("countSensitiveProb").as[Double].collect(), sensitiveFeaturesDf.count)

  private lazy val actualSensitiveFeatures: DataFrame = {
    spark
    new DistributionMeasures()
      .setSensitiveCols(Array("Gender", "Ethnicity"))
      .setVerbose(true)
      .transform(sensitiveFeaturesDf)
  }

  private lazy val actualSensitiveFeature1: Map[String, Double] =
    actualSensitiveFeatures.filter(col("FeatureName") === "Gender")
      .as[(String, Map[String, Double])]
      .collect()(0)._2

  private lazy val actualSensitiveFeature2: Map[String, Double] =
    actualSensitiveFeatures.filter(col("FeatureName") === "Ethnicity")
      .as[(String, Map[String, Double])]
      .collect()(0)._2

  test("DistributionMeasureTransformer can calculate Kullback–Leibler divergence for Sensitive Feature 1") {
    assert(actualSensitiveFeature1("kl_divergence") == expectedSensitiveFeature1.klDivergence)
  }

  test("DistributionMeasureTransformer can calculate Kullback–Leibler divergence for Sensitive Feature 2") {
    assert(actualSensitiveFeature2("kl_divergence") == expectedSensitiveFeature2.klDivergence)
  }

  test("DistributionMeasureTransformer can calculate Jensen-Shannon distance for Sensitive Feature 1") {
    assert(actualSensitiveFeature1("js_dist") == expectedSensitiveFeature1.jsDistance)
  }

  test("DistributionMeasureTransformer can calculate Jensen-Shannon distance for Sensitive Feature 2") {
    assert(actualSensitiveFeature2("js_dist") == expectedSensitiveFeature2.jsDistance)
  }

  test("DistributionMeasureTransformer can calculate Infinity norm distance for Sensitive Feature 1") {
    assert(actualSensitiveFeature1("inf_norm_dist") == expectedSensitiveFeature1.infNormDistance)
  }

  test("DistributionMeasureTransformer can calculate Infinity norm distance for Sensitive Feature 2") {
    assert(actualSensitiveFeature2("inf_norm_dist") == expectedSensitiveFeature2.infNormDistance)
  }

  test("DistributionMeasureTransformer can calculate Total variation distance for Sensitive Feature 1") {
    assert(actualSensitiveFeature1("total_variation_dist") == expectedSensitiveFeature1.totalVariationDistance)
  }

  test("DistributionMeasureTransformer can calculate Total variation distance for Sensitive Feature 2") {
    assert(actualSensitiveFeature2("total_variation_dist") == expectedSensitiveFeature2.totalVariationDistance)
  }

  //  test("DistributionMeasureTransformer can calculate Wasserstein distance for Sensitive Feature 1") {
  //    assert(actualSensitiveFeature1("wasserstein_dist") == expectedSensitiveFeature1.wassersteinDistance)
  //  }
  //
  //  test("DistributionMeasureTransformer can calculate Wasserstein distance for Sensitive Feature 2") {
  //    assert(actualSensitiveFeature2("wasserstein_dist") == expectedSensitiveFeature2.wassersteinDistance)
  //  }

  test("DistributionMeasureTransformer can calculate Chi-square test statistic for Sensitive Feature 1") {
    assert(actualSensitiveFeature1("chi_sq_stat") == expectedSensitiveFeature1.chiSqTestStatistic)
  }

  test("DistributionMeasureTransformer can calculate Chi-square test statistic for Sensitive Feature 2") {
    assert(actualSensitiveFeature2("chi_sq_stat") == expectedSensitiveFeature2.chiSqTestStatistic)
  }

  //  test("DistributionMeasureTransformer can calculate Chi-square p value for Sensitive Feature 1") {
  //    assert(actualSensitiveFeature1("chi_sq_p_value") == expectedSensitiveFeature1.chiSqPValue)
  //  }
  //
  //  test("DistributionMeasureTransformer can calculate Infinity norm distance for Sensitive Feature 2") {
  //    assert(actualSensitiveFeature2("chi_sq_p_value") == expectedSensitiveFeature2.chiSqPValue)
  //  }
}
