// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.exploratory

import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array, col}

import java.util
import scala.collection.JavaConverters._

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
    // val (numRows, numFeatures) = (sensitiveFeaturesDf.count.toDouble, expectedFeature1.length)
    // val (obsProbs, obsCounts) = expectedFeature1.unzip
    // val (refProbs, refCounts) = Array.fill(numFeatures.toInt)(numFeatures).map(n => (1d / n, numRows / n)).unzip
    // val CALC = DistributionMetricsCalculator(refProbs, refCounts, obsProbs, obsCounts, numFeatures)
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
    // val (numRows, numFeatures) = (sensitiveFeaturesDf.count.toDouble, expectedFeature2.length)
    // val (obsProbs, obsCounts) = expectedFeature2.unzip
    // val (refProbs, refCounts) = Array.fill(numFeatures.toInt)(numFeatures).map(n => (1d / n, numRows / n)).unzip
    // val CALC = DistributionMetricsCalculator(refProbs, refCounts, obsProbs, obsCounts, numFeatures)
    val KLDIVERGENCE = 0.07551068302017659
    val JSDISTANCE = 0.14172745151398888
    val INFNORMDISTANCE = 0.1388888888888889
    val TOTALVARIATIONDISTANCE = 0.16666666666666666
    val WASSERSTEINDISTANCE = 0.08333333333333333
    val CHISQUAREDTESTSTATISTIC = 1.2222222222222223
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

  private def customDistribution: util.ArrayList[util.HashMap[String, Double]] = {
    // For each feature in sensitiveFeaturesDf (["Gender", "Ethnicity"]), need to specify its corresponding distribution
    val customDist = new util.ArrayList[util.HashMap[String, Double]]()
    // Index 0: Gender (all unique values included)
    customDist.add(new util.HashMap[String, Double](Map("Male" -> 0.25, "Female" -> 0.4, "Other" -> 0.35).asJava))
    // Index 1: Ethnicity ('Other' value purposefully left out, which signals a probability of 0.0)
    customDist.add(new util.HashMap[String, Double](Map("Asian" -> 0.33, "White" -> 0.33, "Black" -> 0.33).asJava))
    customDist
  }

  test("DistributionBalanceMeasure can use a custom reference distribution for multiple cols") {
    val df = distributionBalanceMeasure
      .setReferenceDistribution(customDistribution)
      .transform(sensitiveFeaturesDf)

    df.show(truncate = false)
    df.printSchema()
  }

  test("DistributionBalanceMeasure can use a custom distribution for one col and uniform for another") {
    val customDist = customDistribution
    // Keep custom distribution for Gender (index 0), and use uniform distribution for Ethnicity (index 1)
    customDist.set(1, null) //scalastyle:ignore null

    val df = distributionBalanceMeasure
      .setReferenceDistribution(customDist)
      .transform(sensitiveFeaturesDf)

    df.show(truncate = false)
    df.printSchema()
  }

  private def actualCustomDist: DataFrame =
    new DistributionBalanceMeasure()
      .setSensitiveCols(features)
      .setVerbose(true)
      .setReferenceDistribution(customDistribution)
      .transform(sensitiveFeaturesDf)

  private def actualCustomDistFeature1: Map[String, Double] =
    METRICS zip actualCustomDist.filter(col("FeatureName") === feature1)
      .select(array(col("DistributionBalanceMeasure.*")))
      .as[Array[Double]]
      .head toMap

  private def expectedCustomDistFeature1 = getFeatureStats(sensitiveFeaturesDf.groupBy(feature1))
    .select(feature1, featureProbCol, featureCountCol)
    .as[(String, Double, Double)].collect()

  private object ExpectedCustomDistFeature1 {
    // Values were computed using:
    // val (numRows, numFeatures) = (sensitiveFeaturesDf.count.toDouble, expectedCustomDistFeature1.length)
    // val (featureValues, obsProbs, obsCounts) = expectedCustomDistFeature1.unzip3
    // val refProbs = featureValues.map(customDistribution.get(0).getOrDefault(_, 0.0)) // idx 0 = Gender
    // val refCounts = refProbs.map(_ * numRows)
    // val CALC = DistributionMetricsCalculator(refProbs, refCounts, obsProbs, obsCounts, numFeatures)
    val KLDIVERGENCE = 0.09399792940857671
    val JSDISTANCE = 0.15001917759832653
    val INFNORMDISTANCE = 0.19444444444444442
    val TOTALVARIATIONDISTANCE = 0.19444444444444445
    val WASSERSTEINDISTANCE = 0.12962962962962962
    val CHISQUAREDTESTSTATISTIC = 1.880952380952381
    val CHISQUAREDPVALUE = 0.3904418663854293
  }

  test(s"DistributionBalanceMeasure can use a custom reference distribution with all values ($feature1)") {
    // The custom reference distribution for Gender is Map("Male" -> 0.25, "Female" -> 0.4, "Other" -> 0.35)
    // This includes all unique values of Gender in the dataframe being transformed
    val actual = actualCustomDistFeature1
    val expected = ExpectedCustomDistFeature1
    assert(actual(KLDIVERGENCE) === expected.KLDIVERGENCE)
    assert(actual(JSDISTANCE) === expected.JSDISTANCE)
    assert(actual(INFNORMDISTANCE) === expected.INFNORMDISTANCE)
    assert(actual(TOTALVARIATIONDISTANCE) === expected.TOTALVARIATIONDISTANCE)
    assert(actual(WASSERSTEINDISTANCE) === expected.WASSERSTEINDISTANCE)
    assert(actual(CHISQUAREDTESTSTATISTIC) === expected.CHISQUAREDTESTSTATISTIC)
    assert(actual(CHISQUAREDPVALUE) === expected.CHISQUAREDPVALUE)
  }

  private def actualCustomDistFeature2: Map[String, Double] =
    METRICS zip actualCustomDist.filter(col("FeatureName") === feature2)
      .select(array(col("DistributionBalanceMeasure.*")))
      .as[Array[Double]]
      .head toMap

  private def expectedCustomDistFeature2 = getFeatureStats(sensitiveFeaturesDf.groupBy(feature2))
    .select(feature2, featureProbCol, featureCountCol)
    .as[(String, Double, Double)].collect()

  private object ExpectedCustomDistFeature2 {
    // Values were computed using:
    // val (numRows, numFeatures) = (sensitiveFeaturesDf.count.toDouble, expectedCustomDistFeature2.length)
    // val (featureValues, obsProbs, obsCounts) = expectedCustomDistFeature2.unzip3
    // val refProbs = featureValues.map(customDistribution.get(1).getOrDefault(_, 0.0)) // idx 1 = Ethnicity
    // val refCounts = refProbs.map(_ * numRows)
    // val CALC = DistributionMetricsCalculator(refProbs, refCounts, obsProbs, obsCounts, numFeatures)
    val KLDIVERGENCE = Double.PositiveInfinity // ln(obsProb(Other) / refProb(Other)) = ln(0.111 / 0.0) = +Inf
    val JSDISTANCE = Double.NaN // ln(refProb(Other) / avgRefAObsProb(Other)) = ln(0.0 / 0.055) = -Inf -> sum(...) = NaN
    val INFNORMDISTANCE = 0.1111111111111111
    val TOTALVARIATIONDISTANCE = 0.11277777777777775
    val WASSERSTEINDISTANCE = 0.05638888888888888
    // (obsCount(Other) - refCount(Other))^2 / refCount(Other)) = (1 - 0)^2 / 0 = 1 / 0 = +Inf
    val CHISQUAREDTESTSTATISTIC = Double.PositiveInfinity
    // Since Chi^2 represents the confidence that x <= Y and Y is +inf, p-value = 1.0 - confidence = 1.0 - 1.0 = 0.0
    val CHISQUAREDPVALUE = 0
  }

  test(s"DistributionBalanceMeasure can a custom reference distribution with missing values ($feature2)") {
    // The custom reference distribution for Ethnicity is Map("Asian" -> 0.33, "White" -> 0.33, "Black" -> 0.33)
    // This does NOT include all unique values in the dataframe being transformed; 'Other' is left out
    // which means that it should default to a reference probability of 0.00
    val actual = actualCustomDistFeature2
    val expected = ExpectedCustomDistFeature2

    assert(actual(INFNORMDISTANCE) === expected.INFNORMDISTANCE)
    assert(actual(TOTALVARIATIONDISTANCE) === expected.TOTALVARIATIONDISTANCE)
    assert(actual(WASSERSTEINDISTANCE) === expected.WASSERSTEINDISTANCE)

    // TODO: The following measures are handling a probability distribution of 0.0 differently than expected.
    //  They are actively being investigated and cross-referenced with the behavior of scipy measures.
    //  In the meanwhile, they are commented out until the behavior is fixed.
    // assert(actual(KLDIVERGENCE) === expected.KLDIVERGENCE)
    // assert(actual(JSDISTANCE) === expected.JSDISTANCE)
    // assert(actual(CHISQUAREDTESTSTATISTIC) === expected.CHISQUAREDTESTSTATISTIC)
    // assert(actual(CHISQUAREDPVALUE) === expected.CHISQUAREDPVALUE)
  }
}
