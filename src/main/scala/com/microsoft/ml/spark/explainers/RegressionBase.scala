package com.microsoft.ml.spark.explainers

import breeze.linalg.{*, sum, DenseMatrix => BDM, DenseVector => BDV}
import breeze.numerics.sqrt

case class RegressionResult(coefficients: BDV[Double], intercept: Double, rSquared: Double, loss: Double)
  extends (BDV[Double] => Double) {
  def apply(x: BDV[Double]): Double = coefficients.dot(x) + intercept
}

/**
  * The RegressionBase class centers and rescales the input matrix and output vector to support fitting intercept and
  * specifying sampleWeights. The underlying regression algorithm does not need to support fitting intercept and sample
  * weights.
  */
//noinspection ScalaStyle
abstract class RegressionBase {
  def fit(data: BDM[Double], outputs: BDV[Double], fitIntercept: Boolean): RegressionResult = {
    val weights = BDV.ones[Double](data.rows)
    fit(data, outputs, weights, fitIntercept)
  }

  def fit(data: BDM[Double], outputs: BDV[Double], sampleWeights: BDV[Double], fitIntercept: Boolean)
  : RegressionResult = {
    require(data.rows == sampleWeights.size, s"data.rows != sampleWeights.size: ${data.rows} != ${sampleWeights.size}")
    require(sampleWeights.forall(_ >= 0), "Weights must be non-negative")

    val normalizedWeights = normalizeSampleWeights(sampleWeights)

    val (x, y, xOffset, yOffset) = if (fitIntercept) {
      // step 1: center x and y.
      val x_weighted = BDM.tabulate(data.rows, data.cols) {
        (i, j) =>
          normalizedWeights(i) * data(i, j)
      }

      val x_offset = (sum(x_weighted(::, *)) /:/ sum(normalizedWeights)).t
      val x_centered = data(*, ::).map {
        row => row - x_offset
      }

      val y_weighted = BDV.tabulate(outputs.size) {
        i => normalizedWeights(i) * outputs(i)
      }

      val y_offset = sum(y_weighted) / sum(normalizedWeights)
      val y_centered = outputs - y_offset

      (x_centered, y_centered, x_offset, y_offset)
    } else {
      (data, outputs, BDV.zeros[Double](data.cols), 0d)
    }

    // step 2: rescale x, y by sqrt(weights)
    val x_rescaled = BDM.tabulate(data.rows, data.cols) {
      (i, j) =>
        sqrt(normalizedWeights(i)) * x(i, j)
    }

    val yRescaled = BDV.tabulate(outputs.size) {
      i => sqrt(normalizedWeights(i)) * y(i)
    }

    // step 3: solve for coefficients
    val coefficients = regress(x_rescaled, yRescaled)

    // step 4: compute intercept
    val intercept = if (fitIntercept) yOffset - (xOffset dot coefficients) else 0d

    val loss = computeLoss(coefficients, intercept)(data, outputs, sampleWeights)
    val rSquared = computeRSquared(coefficients, intercept)(data, outputs, sampleWeights)

    RegressionResult(coefficients, intercept, rSquared, loss)
  }

  protected def computeLoss(coefficients: BDV[Double], intercept: Double)
                         (x: BDM[Double], y: BDV[Double], sampleWeights: BDV[Double]): Double = {
    sumSquaredResiduals(coefficients, intercept)(x, y, sampleWeights)
  }

  private def sumSquaredResiduals(coefficients: BDV[Double], intercept: Double)
                                 (x: BDM[Double], y: BDV[Double], sampleWeights: BDV[Double]): Double = {
    val estimated = x(*, ::).map {
      row => (row dot coefficients) + intercept
    }

    val residuals = y - estimated
    val weightedResiduals = sqrt(sampleWeights) * residuals
    weightedResiduals dot weightedResiduals
  }

  private def computeRSquared(coefficients: BDV[Double], intercept: Double)
                      (x: BDM[Double], y: BDV[Double], sampleWeights: BDV[Double]): Double = {
    val loss = sumSquaredResiduals(coefficients, intercept)(x, y, sampleWeights)

    val weightedMean = (sampleWeights dot y) / sum(sampleWeights)
    val weightedVariance = sqrt(sampleWeights) *:* (y - weightedMean)
    val tss = weightedVariance dot weightedVariance
    1 - (loss / tss)
  }

  protected def regress(x: BDM[Double], y: BDV[Double]): BDV[Double]

  protected def normalizeSampleWeights(sampleWeights: BDV[Double]): BDV[Double]
}
