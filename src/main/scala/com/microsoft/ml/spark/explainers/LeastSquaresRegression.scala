package com.microsoft.ml.spark.explainers
import breeze.linalg.{DenseMatrix, DenseVector, sum}
import breeze.stats.regression.leastSquares

final class LeastSquaresRegression extends RegressionBase {
  override protected def normalizeSampleWeights(sampleWeights: DenseVector[Double]): DenseVector[Double] = {
    sampleWeights / sum(sampleWeights)
  }

  override protected def regress(x: DenseMatrix[Double], y: DenseVector[Double]): DenseVector[Double] = {
    leastSquares(x, y).coefficients
  }
}
