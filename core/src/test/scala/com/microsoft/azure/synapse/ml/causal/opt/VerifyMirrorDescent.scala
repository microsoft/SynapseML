// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal.opt

import breeze.linalg.{sum, DenseMatrix => BDM, DenseVector => BDV}
import breeze.optimize.DiffFunction
import breeze.stats.distributions.RandBasis
import com.microsoft.azure.synapse.ml.causal._
import com.microsoft.azure.synapse.ml.causal.linalg._
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.scalactic.{Equality, TolerantNumerics}

class VerifyMirrorDescent extends TestBase {
  private val matrixA = BDM.rand(100, 50, RandBasis.withSeed(47).uniform)
  private val vectorB = BDV.rand(100, RandBasis.withSeed(59).uniform)
  private implicit val matrixOps: MatrixOps[BDM[Double], BDV[Double]] = BzMatrixOps
  private implicit val vectorOps: VectorOps[BDV[Double]] = BzVectorOps
  private implicit val equalityDouble: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1E-8)

  private def getLossFunc(A: BDM[Double], b: BDV[Double])
  : DiffFunction[BDV[Double]] = {
    (x: BDV[Double]) => {
      val error = matrixOps.gemv(A, x, Some(b), beta = -1)
      val value = math.pow(vectorOps.nrm2(error), 2)

      val grad = matrixOps.gemv(
        A,
        x = error,
        alpha = 2,
        aTranspose = true
      ) // 2 * A.t * (A*x - b)

      (value, grad)
    }
  }

  private val loss = getLossFunc(matrixA, vectorB)

  test("MirrorDescent finds optimal constrained solution") {
    implicit val cacheOps: CacheOps[BDV[Double]] = BDVCacheOps
    val md = new MirrorDescent(loss, 0.5, 1000)

    val initial = BDV.fill(50, 1d/50)
    val solution = md.solve(initial)
    assert(sum(solution) === 1.0)
    assert(solution.forall(0 <= _ && _ <= 1))
    assert(loss(solution) === 6.087082814745372)
    val lossHistory = md.history.map(_.valueAt)
    assert(lossHistory.length === 1001)
  }

  test("MirrorDescent finds constrained solution with early stopping") {
    implicit val cacheOps: CacheOps[BDV[Double]] = BDVCacheOps
    val md = new MirrorDescent(loss, 0.5, 1000, Some(5), tol = 1E-2)
    val initial = BDV.fill(50, 1d/50)
    val solution = md.solve(initial)
    assert(sum(solution) === 1.0)
    assert(solution.forall(0 <= _ && _ <= 1))
    assert(loss(solution) === 6.186123997006668)
    val lossHistory = md.history.map(_.valueAt)
    assert(lossHistory.length === 55)
  }
}
