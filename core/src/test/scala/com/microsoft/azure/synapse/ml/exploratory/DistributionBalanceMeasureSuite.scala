// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.exploratory

import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{array, col}

class DistributionBalanceMeasureSuite extends DataBalanceTestBase with TransformerFuzzing[DistributionBalanceMeasure] {

  override def testObjects(): Seq[TestObject[DistributionBalanceMeasure]] = Seq(
    new TestObject(distributionBalanceMeasure, sensitiveFeaturesDf)
  )

  override def reader: MLReadable[_] = DistributionBalanceMeasure

  import DistributionMetrics._
  import spark.implicits._

  private val jsDistanceTolerance = 1e-12

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

  private def jsDistance(observed: Seq[Double], reference: Seq[Double]): Double = {
    require(observed.nonEmpty)
    require(observed.length == reference.length)
    Seq(observed, reference).foreach { distribution =>
      require(distribution.forall(probability => java.lang.Double.isFinite(probability) && probability >= 0d))
      require(math.abs(distribution.sum - 1d) <= jsDistanceTolerance)
    }
    val observedProbabilityCol = "observedProbability"
    val referenceProbabilityCol = "referenceProbability"
    val observedCountCol = "observedCount"
    val referenceCountCol = "referenceCount"
    val unusedCount = 0d
    val probabilities = observed.zip(reference)
      .map { case (observedProbability, referenceProbability) =>
        (observedProbability, referenceProbability, unusedCount, unusedCount)
      }
      .toDF(observedProbabilityCol, referenceProbabilityCol, observedCountCol, referenceCountCol)
    val metrics = DistributionMetrics(
      observed.length,
      observedProbabilityCol,
      observedCountCol,
      referenceProbabilityCol,
      referenceCountCol)

    probabilities.agg(metrics.jsDistance.alias(JSDISTANCE)).head().getAs[Double](JSDISTANCE)
  }

  private def assertJsDistance(actual: Double, expected: Double): Unit = {
    assert(!actual.isNaN)
    assert(math.abs(actual - expected) <= jsDistanceTolerance)
  }

  test("Jensen-Shannon distance has normalized endpoints and handles zero-probability support") {
    assertJsDistance(jsDistance(Seq(0.5d, 0.5d), Seq(0.5d, 0.5d)), 0d)
    assertJsDistance(jsDistance(Seq(1d, 0d), Seq(0d, 1d)), 1d)
    assertJsDistance(
      jsDistance(Seq(0.5d, 0.5d, 0d), Seq(0.5d, 0d, 0.5d)),
      math.sqrt(0.5d))
    assertJsDistance(
      jsDistance(Seq(0.9999999d, 0.0000001d), Seq(0.0000001d, 0.9999999d)),
      0.999998765189656d)
    assertJsDistance(
      jsDistance(
        Seq(0.36096096457437404d, 0.0463302746586729d, 0.592708760766953d),
        Seq(0.36096096457437415d, 0.04633027465867293d, 0.5927087607669529d)),
      0d)
  }

  test("Jensen-Shannon distance is symmetric for an intermediate distribution") {
    val observed = Seq(0.7d, 0.2d, 0.1d)
    val reference = Seq(0.1d, 0.3d, 0.6d)
    val expected = 0.5768458213445598
    val forward = jsDistance(observed, reference)

    assertJsDistance(forward, expected)
    assertJsDistance(jsDistance(reference, observed), expected)
    assertJsDistance(jsDistance(observed.reverse, reference.reverse), expected)
  }

  test("Jensen-Shannon distance stays within its documented bounds") {
    val distributions = Seq(
      (Seq(1d, 0d), Seq(0d, 1d)),
      (Seq(0.75d, 0.25d), Seq(0.25d, 0.75d)),
      (Seq(0.5d, 0.5d), Seq(1d, 0d)))

    distributions.foreach { case (observed, reference) =>
      val distance = jsDistance(observed, reference)
      assert(!distance.isNaN)
      assert(distance >= -jsDistanceTolerance && distance <= 1d + jsDistanceTolerance)
    }
  }

  test("DistributionBalanceMeasure exposes normalized JS distance end-to-end without changing its schema") {
    val source = Seq("red", "red", "red", "blue").toDF("color")
    val measure = new DistributionBalanceMeasure()
      .setSensitiveCols(Array("color"))
      .setReferenceDistribution(Array(Map("red" -> 0.25d, "blue" -> 0.75d)))
    val result = measure.transform(source)
    val rows = result.collect()

    assert(result.schema === measure.transformSchema(source.schema))
    assert(rows.length === 1)
    assert(rows.head.getAs[String]("FeatureName") === "color")
    assertJsDistance(
      rows.head.getAs[Row]("DistributionBalanceMeasure").getAs[Double](JSDISTANCE),
      0.4344213111034807)
  }

  private def actualFeature1: Map[String, Double] =
    (METRICS zip actual.filter(col("FeatureName") === feature1)
      .select(array(col("DistributionBalanceMeasure.*")))
      .as[Array[Double]]
      .head).toMap

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
    val JSDISTANCE = 0.11753251925575922
    val INFNORMDISTANCE = 0.1111111111111111
    val TOTALVARIATIONDISTANCE = 0.1111111111111111
    val WASSERSTEINDISTANCE = 0.07407407407407407
    val CHISQUAREDTESTSTATISTIC = 0.6666666666666666
    val CHISQUAREDPVALUE = 0.7165313105737893
  }

  test(s"DistributionBalanceMeasure normalizes JS distance without changing other measures for $feature1") {
    val actual = actualFeature1
    val expected = ExpectedFeature1
    assert(actual(KLDIVERGENCE) === expected.KLDIVERGENCE)
    assertJsDistance(actual(JSDISTANCE), expected.JSDISTANCE)
    assert(actual(INFNORMDISTANCE) === expected.INFNORMDISTANCE)
    assert(actual(TOTALVARIATIONDISTANCE) === expected.TOTALVARIATIONDISTANCE)
    assert(actual(WASSERSTEINDISTANCE) === expected.WASSERSTEINDISTANCE)
    assert(actual(CHISQUAREDTESTSTATISTIC) === expected.CHISQUAREDTESTSTATISTIC)
    assert(actual(CHISQUAREDPVALUE) === expected.CHISQUAREDPVALUE)
  }

  private def actualFeature2: Map[String, Double] =
    (METRICS zip actual.filter(col("FeatureName") === feature2)
      .select(array(col("DistributionBalanceMeasure.*")))
      .as[Array[Double]]
      .head).toMap

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
    val JSDISTANCE = 0.1702320179536471
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
    assertJsDistance(actual(JSDISTANCE), expected.JSDISTANCE)
    assert(actual(INFNORMDISTANCE) === expected.INFNORMDISTANCE)
    assert(actual(TOTALVARIATIONDISTANCE) === expected.TOTALVARIATIONDISTANCE)
    assert(actual(WASSERSTEINDISTANCE) === expected.WASSERSTEINDISTANCE)
    assert(actual(CHISQUAREDTESTSTATISTIC) === expected.CHISQUAREDTESTSTATISTIC)
    assert(actual(CHISQUAREDPVALUE) === expected.CHISQUAREDPVALUE)
  }

  // For each feature in sensitiveFeaturesDf (["Gender", "Ethnicity"]), need to specify its corresponding distribution
  private def customDistribution: Array[Map[String, Double]] = Array(
    // Index 0: Gender (all unique values included)
    Map("Male" -> 0.25, "Female" -> 0.4, "Other" -> 0.35),
    // Index 1: Ethnicity ('Other' value purposefully left out, which signals a probability of 0.0)
    Map("Asian" -> 1/3d, "White" -> 1/3d, "Black" -> 1/3d)
  )

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
    // Specifying empty map defaults to the uniform distribution
    customDist.update(1, Map())

    val df = distributionBalanceMeasure
      .setReferenceDistribution(customDist)
      .transform(sensitiveFeaturesDf)

    df.show(truncate = false)
    df.printSchema()
  }

  test("DistributionBalanceMeasure expects the custom distribution to be the same length as sensitive columns") {
    val emptyDist: Array[Map[String, Double]] = Array.empty
    assertThrows[Exception] {
      distributionBalanceMeasure
        .setReferenceDistribution(emptyDist)
        .transform(sensitiveFeaturesDf)
    }

    val mismatchedLenDist = Array(Map("ColA" -> 0.25))
    assertThrows[Exception] {
      distributionBalanceMeasure
        .setReferenceDistribution(mismatchedLenDist)
        .transform(sensitiveFeaturesDf)
    }
  }

  private def actualCustomDist: DataFrame =
    new DistributionBalanceMeasure()
      .setSensitiveCols(features)
      .setVerbose(true)
      .setReferenceDistribution(customDistribution)
      .transform(sensitiveFeaturesDf)

  private def actualCustomDistFeature1: Map[String, Double] =
    (METRICS zip actualCustomDist.filter(col("FeatureName") === feature1)
      .select(array(col("DistributionBalanceMeasure.*")))
      .as[Array[Double]]
      .head).toMap

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
    val JSDISTANCE = 0.18019139596106415
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
    assertJsDistance(actual(JSDISTANCE), expected.JSDISTANCE)
    assert(actual(INFNORMDISTANCE) === expected.INFNORMDISTANCE)
    assert(actual(TOTALVARIATIONDISTANCE) === expected.TOTALVARIATIONDISTANCE)
    assert(actual(WASSERSTEINDISTANCE) === expected.WASSERSTEINDISTANCE)
    assert(actual(CHISQUAREDTESTSTATISTIC) === expected.CHISQUAREDTESTSTATISTIC)
    assert(actual(CHISQUAREDPVALUE) === expected.CHISQUAREDPVALUE)
  }

  private def actualCustomDistFeature2: Map[String, Double] =
    (METRICS zip actualCustomDist.filter(col("FeatureName") === feature2)
      .select(array(col("DistributionBalanceMeasure.*")))
      .as[Array[Double]]
      .head).toMap

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
    val KLDIVERGENCE = Double.PositiveInfinity
    val JSDISTANCE = 0.25223963779252284
    val INFNORMDISTANCE = 0.1111111111111111
    val TOTALVARIATIONDISTANCE = 0.1111111111111111
    val WASSERSTEINDISTANCE = 0.05555555555555555
    val CHISQUAREDTESTSTATISTIC = Double.PositiveInfinity
    val CHISQUAREDPVALUE = 1d
  }

  test(s"DistributionBalanceMeasure can a custom reference distribution with missing values ($feature2)") {
    // The custom reference distribution for Ethnicity is Map("Asian" -> 0.33, "White" -> 0.33, "Black" -> 0.33)
    // This does NOT include all unique values in the dataframe being transformed; 'Other' is left out
    // which means that it should default to a reference probability of 0.00
    val actual = actualCustomDistFeature2
    val expected = ExpectedCustomDistFeature2
    assert(actual(KLDIVERGENCE) === expected.KLDIVERGENCE)
    assertJsDistance(actual(JSDISTANCE), expected.JSDISTANCE)
    assert(actual(INFNORMDISTANCE) === expected.INFNORMDISTANCE)
    assert(actual(TOTALVARIATIONDISTANCE) === expected.TOTALVARIATIONDISTANCE)
    assert(actual(WASSERSTEINDISTANCE) === expected.WASSERSTEINDISTANCE)
    assert(actual(CHISQUAREDTESTSTATISTIC) === expected.CHISQUAREDTESTSTATISTIC)
    assert(actual(CHISQUAREDPVALUE) === expected.CHISQUAREDPVALUE)
  }
}
