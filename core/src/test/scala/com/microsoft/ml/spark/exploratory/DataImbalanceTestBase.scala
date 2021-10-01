package com.microsoft.ml.spark.exploratory

import com.microsoft.ml.spark.core.test.base.TestBase
import org.apache.spark.sql.DataFrame

import scala.math.{abs, log, pow, sqrt}

trait DataImbalanceTestBase extends TestBase {

  import spark.implicits._

  lazy val errorTolerance: Double = 1e-12

  lazy val sensitiveFeaturesDf: DataFrame = Seq(
    (0, "Male", "Asian"),
    (0, "Male", "White"),
    (1, "Male", "Other"),
    (1, "Male", "Black"),
    (0, "Female", "White"),
    (0, "Female", "Black"),
    (1, "Female", "Black"),
    (0, "Other", "Asian"),
    (0, "Other", "White")
  ) toDF("Label", "Gender", "Ethnicity")

  case class GapCalculator(numRows: Double, pY: Double, pX1: Double, pX1andY: Double, pX2: Double, pX2andY: Double) {
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

  case class AggregateMeasureCalculator(benefits: Array[Double], epsilon: Double, errorTolerance: Double) {
    val numBenefits: Double = benefits.length
    val meanBenefits: Double = benefits.sum / numBenefits
    val normBenefits: Array[Double] = benefits.map(_ / meanBenefits)

    val atkinsonIndex: Double = {
      val alpha = 1d - epsilon
      if (abs(alpha) < errorTolerance) {
        1d - pow(normBenefits.product, 1d / numBenefits)
      } else {
        val powerMean = normBenefits.map(pow(_, alpha)).sum / numBenefits
        1d - pow(powerMean, 1d / alpha)
      }
    }
    val theilLIndex: Double = generalizedEntropyIndex(0d)
    val theilTIndex: Double = generalizedEntropyIndex(1d)

    def generalizedEntropyIndex(alpha: Double): Double = {
      if (abs(alpha - 1d) < errorTolerance) {
        normBenefits.map(x => x * log(x)).sum / numBenefits
      } else if (abs(alpha) < errorTolerance) {
        normBenefits.map(-1 * log(_)).sum / numBenefits
      } else {
        normBenefits.map(pow(_, alpha) - 1d).sum / (numBenefits * alpha * (alpha - 1d))
      }
    }
  }

  case class DistributionMeasureCalculator(observedProbabilities: Array[Double], numRows: Double) {
    val numObservations: Double = observedProbabilities.length
    val referenceNumObservations: Double = numRows / numObservations
    val referenceProbabilities: Array[Double] = Array.fill(numObservations.toInt)(1d / numObservations)

    val absDiffObsRef: Array[Double] = (observedProbabilities, referenceProbabilities).zipped.map((a, b) => abs(a - b))

    val klDivergence: Double = entropy(observedProbabilities, Some(referenceProbabilities))
    val jsDistance: Double = {
      val averageObsRef = (observedProbabilities, referenceProbabilities).zipped.map((a, b) => (a + b) / 2d)
      val entropyRefAvg = entropy(referenceProbabilities, Some(averageObsRef))
      val entropyObsAvg = entropy(observedProbabilities, Some(averageObsRef))
      sqrt((entropyRefAvg + entropyObsAvg) / 2d)
    }
    val infNormDistance: Double = absDiffObsRef.max
    val totalVariationDistance: Double = 0.5d * absDiffObsRef.sum
    //    val wassersteinDistance: Double = ???
    val chiSqTestStatistic: Double = pow(numObservations - referenceNumObservations, 2) / referenceNumObservations
    //    val chiSqPValue: Double = ???

    def entropy(distA: Array[Double], distB: Option[Array[Double]] = None): Double = {
      if (distB.isDefined) {
        val logQuotient = (distA, distB.get).zipped.map((a, b) => log(a / b))
        (distA, logQuotient).zipped.map(_ * _).sum
      } else {
        -1d * distA.map(x => x * log(x)).sum
      }
    }
  }
}
