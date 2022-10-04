// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, Matrix => BM, Vector => BV, _}
import breeze.numerics.sqrt
import breeze.storage.Zero

import scala.reflect.ClassTag
import scala.util.Try

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

  /**
    * Provides an implementation for sum operation of BroadcastedColumns in breeze.
    * Spark 3.0.* and 3.1.* depends on breeze 1.0 and Spark 3.2.* depends on breeze 1.2, and there is
    * a breaking change in the way the implicit sum implementation is provided.
    * In breeze 1.0, the implementation is constructed via
    *   `sum.vectorizeCols_Double(ClassTag[Double], Zero.DoubleZero, sum.helper_Double)`,
    * while in breeze 1.2, it's constructed via
    *   `sum.vectorizeCols_Double(sum.helper_Double)`
    * If our code is compiled against Spark 3.2.0/breeze 1.0, the scala compiler implicitly constructs
    * the implementation via
    *   `sum.vectorizeCols_Double(ClassTag[Double], Zero.DoubleZero, sum.helper_Double)`,
    * which does not exist in breeze 1.2, thus causing `java.lang.NoSuchMethodError` when running on Spark 3.2.0.
    * Conversely, if our code is compiled against Spark 3.2.0/breeze 1.2, it will cause `java.lang.NoSuchMethodError`
    * when running on Spark 3.0.* and 3.1.*.
    * Workaround: use reflection to construct the implementation.
    */
  //TODO: Check for spark 3.3.0
  implicit lazy val sumImpl: sum.Impl[BroadcastedColumns[BDM[Double], BDV[Double]], Transpose[BDV[Double]]] = {
    Try {
      // This works for breeze 1.2
      sum
        .getClass
        .getMethod("vectorizeCols_Double", classOf[sum.VectorizeHelper[_]])
        .invoke(sum, sum.helper_Double)
    }.getOrElse(
      // This works for breeze 1.0
      sum
        .getClass
        .getMethod("vectorizeCols_Double", classOf[ClassTag[_]], classOf[Zero[_]], classOf[sum.VectorizeHelper[_]])
        .invoke(sum, ClassTag.Double, Zero.DoubleZero, sum.helper_Double)
    ).asInstanceOf[sum.Impl[BroadcastedColumns[BDM[Double], BDV[Double]], Transpose[BDV[Double]]]]
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
