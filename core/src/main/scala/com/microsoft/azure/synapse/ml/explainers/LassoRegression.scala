// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers

import breeze.linalg.{isClose, sum, DenseMatrix => BDM, DenseVector => BDV}
import breeze.numerics.abs

import scala.annotation.tailrec

private[ml] case class CoordinateDescentLasso(alpha: Double, maxIterations: Int, tol: Double) {
  require(maxIterations >= 1)
  require(tol >= 0)

  def softThresholdingOp(x: Double, lambda: Double): Double = {
    if (x > 0 && lambda < math.abs(x))
      x - lambda
    else if (x < 0 && lambda < math.abs(x))
      x + lambda
    else
      0d
  }

  private def fitIteration(x: BDM[Double], y: BDV[Double])(currBeta: BDV[Double]): BDV[Double] = {
    val newBeta = currBeta.copy

    (0 until currBeta.length) foreach {
      j =>
        newBeta(j) = 0d
        val xj = x(::, j)
        val squaredLength = xj dot xj
        newBeta(j) = if (squaredLength == 0d) {
          // Since x_j is centered, zero squared magnitude indicates that x_j has single value.
          // Setting the coefficient to zero in this case.
          0d
        } else {
          val r = y - (x * newBeta)
          val arg1 = xj dot r
          val arg2 = this.alpha * x.rows
          softThresholdingOp(arg1, arg2) / squaredLength
        }
    }

    newBeta
  }

  private def converged(tol: Double)(oldBeta: BDV[Double], newBeta: BDV[Double]): Boolean = {
    isClose(oldBeta, newBeta, tol)
  }

  @tailrec
  private def recurse[T](iterateFunc: T => T, convergeFunc: (T, T) => Boolean, iteration: Int, value: T): T = {
    if (iteration > 0) {
      val newVal = iterateFunc(value)

      if (convergeFunc(value, newVal)) {
        newVal
      } else {
        recurse(iterateFunc, convergeFunc, iteration - 1, newVal)
      }
    } else {
      value
    }
  }

  def fit(x: BDM[Double], y: BDV[Double]): BDV[Double] = {
    val initialBeta = BDV.zeros[Double](x.cols)
    val fitFunc = fitIteration(x, y) _
    val convergeFunc = converged(tol = tol) _
    recurse(fitFunc, convergeFunc, maxIterations, initialBeta)
  }
}

//noinspection ScalaStyle
final class LassoRegression(alpha: Double,
                            maxIterations: Int = 1000,  //scalastyle:ignore magic.number
                            tol: Double = 1E-5) extends RegressionBase {
  override protected def normalizeSampleWeights(sampleWeights: BDV[Double]): BDV[Double] = {
    sampleWeights * (sampleWeights.size / sum(sampleWeights))
  }

  override protected def regress(x: BDM[Double], y: BDV[Double]): BDV[Double] = {
    CoordinateDescentLasso(alpha, maxIterations, tol).fit(x, y)
  }

  override protected def computeLoss(coefficients: BDV[Double], intercept: Double)
                                    (x: BDM[Double], y: BDV[Double], sampleWeights: BDV[Double]): Double = {
    super.computeLoss(coefficients, intercept)(x, y, sampleWeights) + alpha * sum(abs(coefficients))
  }
}
