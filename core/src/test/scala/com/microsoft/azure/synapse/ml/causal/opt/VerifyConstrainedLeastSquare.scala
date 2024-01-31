// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal.opt

import breeze.stats.distributions.RandBasis
import breeze.linalg.{sum, DenseMatrix => BDM, DenseVector => BDV}
import com.microsoft.azure.synapse.ml.causal._
import com.microsoft.azure.synapse.ml.causal.linalg._
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.log4j.{Level, Logger}
import org.scalactic.{Equality, TolerantNumerics}

class VerifyConstrainedLeastSquare extends TestBase {
  private val matrixA = BDM.rand(100, 50, RandBasis.withSeed(47).uniform)
  private val vectorB = BDV.rand(100, RandBasis.withSeed(59).uniform)
  private implicit val matrixOps: MatrixOps[BDM[Double], BDV[Double]] = BzMatrixOps
  private implicit val vectorOps: VectorOps[BDV[Double]] = BzVectorOps
  private implicit val cacheOps: CacheOps[BDV[Double]] = BDVCacheOps
  private implicit val equalityDouble: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1E-8)

  spark.sparkContext.setLogLevel("INFO")
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  test("Fit CLS without intercept") {
    val cls = new ConstrainedLeastSquare(0.5, 1000)
    val solution = cls.solve(matrixA, vectorB)
    assert(solution._2 === 0d)
    assert(sum(solution._1) === 1.0)
    assert(solution._1.forall(0 <= _ && _ <= 1))
    val lossFunc = cls.getLossFunc(matrixA, vectorB, 0d)
    assert(lossFunc(solution._1) === 6.087082814745372)
  }

  test("Fit CLS without intercept distributed") {
    val mA = matrixA.toDMatrix
    val vB = vectorB.toDVector

    implicit val dMatrixOps: MatrixOps[DMatrix, DVector] = DMatrixOps
    implicit val dVectorOps: VectorOps[DVector] = DVectorOps
    implicit val dCacheOps: CacheOps[DVector] = DVectorCacheOps

    val cls = new ConstrainedLeastSquare[DMatrix, DVector](0.5, 10)

    val solution = cls.solve(mA, vB)
    assert(solution._2 === 0d)
    val weights = solution._1.toBreeze
    assert(sum(weights) === 1.0)
    assert(weights.forall(0 <= _ && _ <= 1))
    val lossFunc = cls.getLossFunc(mA, vB, 0d)
    assert(lossFunc(solution._1) === 6.672597868965775)
  }

  test("Fit CLS with intercept") {
    val cls = new ConstrainedLeastSquare(1.5, 1000)
    val solution = cls.solve(matrixA, vectorB, fitIntercept = true)
    assert(solution._2 === 0.025647456560875026)
    assert(sum(solution._1) === 1.0)
    assert(solution._1.forall(0 <= _ && _ <= 1))

    val error = vectorOps.axpy(
      vectorB,
      Some(matrixOps.gemv(matrixA, solution._1, Some(BDV.fill(vectorB.size)(solution._2)))),
      -1
    )

    val loss = math.pow(vectorOps.nrm2(error), 2)
    assert(loss === 6.027794680382836)
  }

  test("Fit CLS with L2 regularization") {
    val cls = new ConstrainedLeastSquare(1.5, 1000)
    val solution = cls.solve(matrixA, vectorB, lambda = 1.5)
    assert(solution._2 === 0d)
    assert(sum(solution._1) === 1.0)
    assert(solution._1.forall(0 <= _ && _ <= 1))
    val lossFunc = cls.getLossFunc(matrixA, vectorB, 1.5)
    assert(lossFunc(solution._1) === 6.219635205398321)
  }
}
