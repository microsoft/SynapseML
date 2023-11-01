// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.exploratory

import breeze.stats.distributions.{ChiSquared, RandBasis}
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.sql.functions.{col, count, lit}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}

import scala.math.{abs, log, pow, sqrt}

trait DataBalanceTestBase extends TestBase {

  import spark.implicits._

  lazy val errorTolerance: Double = 1e-8

  lazy val featureProbCol = "featureProb"
  lazy val positiveFeatureCountCol = "positiveFeatureCount"
  lazy val featureCountCol = "featureCount"
  lazy val positiveCountCol = "positiveCount"
  lazy val rowCountCol = "rowCount"

  lazy val label: String = "Label"
  lazy val features: Array[String] = Array("Gender", "Ethnicity")
  lazy val feature1: String = features(0)
  lazy val feature2: String = features(1)

  def sensitiveFeaturesDf: DataFrame = Seq(
    (0, "Male", "Asian"),
    (0, "Male", "White"),
    (1, "Male", "Other"),
    (1, "Male", "Black"),
    (0, "Female", "White"),
    (0, "Female", "Black"),
    (1, "Female", "Black"),
    (0, "Other", "Asian"),
    (0, "Other", "White")
  ).toDF("Label", "Gender", "Ethnicity").cache

  def getFeatureStats(df: RelationalGroupedDataset): DataFrame =
    df
      .agg(count("*").cast(DoubleType).alias(featureCountCol))
      .withColumn(rowCountCol, lit(sensitiveFeaturesDf.count.toDouble))
      .withColumn(featureProbCol, col(featureCountCol) / col(rowCountCol))
}

case class AssociationMetricsCalculator(numRows: Double,
                                        pY: Double,
                                        pX1: Double,
                                        pX1andY: Double,
                                        pX2: Double,
                                        pX2andY: Double) {
  val pYgivenX1: Double = pX1andY / pX1
  val pX1givenY: Double = pX1andY / pY
  val pYgivenX2: Double = pX2andY / pX2
  val pX2givenY: Double = pX2andY / pY

  val dpGap: Double = pYgivenX1 - pYgivenX2
  val sdcGap: Double = pX1andY / (pX1 + pY) - pX2andY / (pX2 + pY)
  val jiGap: Double = pX1andY / (pX1 + pY - pX1andY) - pX2andY / (pX2 + pY - pX2andY)
  val llrGap: Double = log(pX1givenY) - log(pX2givenY)
  val pmiGap: Double = log(pYgivenX1) - log(pYgivenX2)
  val nPmiYGap: Double = log(pYgivenX1) / log(pY) - log(pYgivenX2) / log(pY)
  val nPmiXYGap: Double = log(pYgivenX1) / log(pX1andY) - log(pYgivenX2) / log(pX2andY)
  val sPmiGap: Double = log(pow(pX1andY, 2) / (pX1 * pY)) - log(pow(pX2andY, 2) / (pX2 * pY))
  val krcGap: Double = {
    val aX1 = pow(numRows, 2) * (1 - 2 * pX1 - 2 * pY + 2 * pX1andY + 2 * pX1 * pY)
    val bX1 = numRows * (2 * pX1 + 2 * pY - 4 * pX1andY - 1)
    val cX1 = pow(numRows, 2) * sqrt((pX1 - pow(pX1, 2)) * (pY - pow(pY, 2)))

    val aX2 = pow(numRows, 2) * (1 - 2 * pX2 - 2 * pY + 2 * pX2andY + 2 * pX2 * pY)
    val bX2 = numRows * (2 * pX2 + 2 * pY - 4 * pX2andY - 1)
    val cX2 = pow(numRows, 2) * sqrt((pX2 - pow(pX2, 2)) * (pY - pow(pY, 2)))

    (aX1 + bX1) / cX1 - (aX2 + bX2) / cX2
  }
  val tTestGap: Double = (pX1andY - pX1 * pY) / sqrt(pX1 * pY) - (pX2andY - pX2 * pY) / sqrt(pX2 * pY)
}

case class AggregateMetricsCalculator(featureProbabilities: Array[Double], epsilon: Double, errorTolerance: Double) {
  val numFeatures: Double = featureProbabilities.length
  val meanFeatures: Double = featureProbabilities.sum / numFeatures
  val normFeatureProbabilities: Array[Double] = featureProbabilities.map(_ / meanFeatures)

  val atkinsonIndex: Double = {
    val alpha = 1d - epsilon
    if (abs(alpha) < errorTolerance) {
      1d - pow(normFeatureProbabilities.product, 1d / numFeatures)
    } else {
      val powerMean = normFeatureProbabilities.map(pow(_, alpha)).sum / numFeatures
      1d - pow(powerMean, 1d / alpha)
    }
  }
  val theilLIndex: Double = generalizedEntropyIndex(0d)
  val theilTIndex: Double = generalizedEntropyIndex(1d)

  def generalizedEntropyIndex(alpha: Double): Double = {
    if (abs(alpha - 1d) < errorTolerance) {
      normFeatureProbabilities.map(x => x * log(x)).sum / numFeatures
    } else if (abs(alpha) < errorTolerance) {
      normFeatureProbabilities.map(-1 * log(_)).sum / numFeatures
    } else {
      normFeatureProbabilities.map(pow(_, alpha) - 1d).sum / (numFeatures * alpha * (alpha - 1d))
    }
  }
}

case class DistributionMetricsCalculator(refFeatureProbabilities: Array[Double],
                                         refFeatureCounts: Array[Double],
                                         obsFeatureProbabilities: Array[Double],
                                         obsFeatureCounts: Array[Double],
                                         numFeatures: Double) {
  val absDiffObsRef: Array[Double] = (obsFeatureProbabilities, refFeatureProbabilities).zipped.map((a, b) => abs(a - b))

  val klDivergence: Double = entropy(obsFeatureProbabilities, Some(refFeatureProbabilities))
  val jsDistance: Double = {
    val averageObsRef = (obsFeatureProbabilities, refFeatureProbabilities).zipped.map((a, b) => (a + b) / 2d)
    val entropyRefAvg = entropy(refFeatureProbabilities, Some(averageObsRef))
    val entropyObsAvg = entropy(obsFeatureProbabilities, Some(averageObsRef))
    sqrt((entropyRefAvg + entropyObsAvg) / 2d)
  }
  val infNormDistance: Double = absDiffObsRef.max
  val totalVariationDistance: Double = 0.5d * absDiffObsRef.sum
  val wassersteinDistance: Double = absDiffObsRef.sum / absDiffObsRef.length
  val chiSquaredTestStatistic: Double = (obsFeatureCounts, refFeatureCounts).zipped.map((a, b) => pow(a - b, 2) / b).sum
  implicit val rand: RandBasis = RandBasis.mt0
  val chiSquaredPValue: Double = chiSquaredTestStatistic match {
    // limit of CDF as x approaches +inf is 1 (https://en.wikipedia.org/wiki/Cumulative_distribution_function)
    case Double.PositiveInfinity => 1
    case _ => 1 - ChiSquared(numFeatures - 1).cdf(chiSquaredTestStatistic)
  }

  def entropy(distA: Array[Double], distB: Option[Array[Double]] = None): Double = {
    if (distB.isDefined) {
      (distA, distB.get).zipped.map((a, b) =>
        // Using cases from scipy.special.rel_entr, which scipy.stats.entropy directly calls
        // https://docs.scipy.org/doc/scipy/reference/generated/scipy.special.rel_entr.html
        if (a == 0.0 && b >= 0.0) 0.0
        else if (a > 0.0 && b > 0) a * log(a / b)
        else Double.PositiveInfinity
      ).sum
    } else {
      -1d * distA.map(x => x * log(x)).sum
    }
  }
}
