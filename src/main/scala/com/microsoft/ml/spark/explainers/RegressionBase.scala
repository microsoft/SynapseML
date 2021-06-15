// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers

import breeze.linalg.{*, sum, DenseMatrix => BDM, DenseVector => BDV, Vector => BV, Matrix => BM}
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
  def fit(data: BM[Double], outputs: BV[Double], fitIntercept: Boolean): RegressionResult = {
    val weights = BDV.ones[Double](data.rows)
    fit(data, outputs, weights, fitIntercept)
  }

  def fit(data: BM[Double], outputs: BV[Double], sampleWeights: BV[Double], fitIntercept: Boolean)
  : RegressionResult = {
    require(data.rows == sampleWeights.size, s"data.rows != sampleWeights.size: ${data.rows} != ${sampleWeights.size}")
    require(sampleWeights.forall(_ >= 0), "Weights must be non-negative")

    val (dataDense, outputsDense, sampleWeightsDense) = (
      data.toDenseMatrix,
      outputs.toDenseVector,
      sampleWeights.toDenseVector
    )

    val normalizedWeights = normalizeSampleWeights(sampleWeightsDense)

    val (x, y, xOffset, yOffset) = if (fitIntercept) {
      // step 1: center x and y.
      val xWeighted = BDM.tabulate(dataDense.rows, dataDense.cols) {
        (i, j) =>
          normalizedWeights(i) * dataDense(i, j)
      }

      val xOffset = (sum(xWeighted(::, *)) /:/ sum(normalizedWeights)).t
      val xCentered = dataDense(*, ::).map {
        row => row - xOffset
      }

      val yWeighted = BDV.tabulate(outputsDense.size) {
        i => normalizedWeights(i) * outputsDense(i)
      }

      val yOffset = sum(yWeighted) / sum(normalizedWeights)
      val yCentered = outputsDense - yOffset

      (xCentered, yCentered, xOffset, yOffset)
    } else {
      (dataDense, outputsDense, BDV.zeros[Double](dataDense.cols), 0d)
    }

    // step 2: rescale x, y by sqrt(weights)
    val xRescaled = BDM.tabulate(dataDense.rows, dataDense.cols) {
      (i, j) =>
        sqrt(normalizedWeights(i)) * x(i, j)
    }

    val yRescaled = BDV.tabulate(outputsDense.size) {
      i => sqrt(normalizedWeights(i)) * y(i)
    }

    // step 3: solve for coefficients
    val coefficients = regress(xRescaled, yRescaled)

    // step 4: compute intercept
    val intercept = if (fitIntercept) yOffset - (xOffset dot coefficients) else 0d

    val loss = computeLoss(coefficients, intercept)(dataDense, outputsDense, sampleWeightsDense)
    val rSquared = computeRSquared(coefficients, intercept)(dataDense, outputsDense, sampleWeightsDense)

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
