// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers

import breeze.linalg.{DenseMatrix, DenseVector, sum}
import breeze.stats.regression.leastSquares

final class LeastSquaresRegression extends RegressionBase {
  override protected def normalizeSampleWeights(sampleWeights: DenseVector[Double]): DenseVector[Double] = {
    sampleWeights / sum(sampleWeights)
  }

  override protected def regress(x: DenseMatrix[Double], y: DenseVector[Double]): DenseVector[Double] = {
    require(x.rows > x.cols + 1, s"x's rows must be greater than x's cols + 1, but got ${(x.rows, x.cols)}")
    leastSquares(x, y).coefficients
  }
}
