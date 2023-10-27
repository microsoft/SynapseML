// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal.linalg

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import breeze.stats.distributions.RandBasis
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.scalactic.Equality

class VerifyMatrixOps extends TestBase {
  spark

  private lazy val testBzVector1 = BDV.rand(20, RandBasis.withSeed(47).uniform)
  private lazy val testDVector1 = testBzVector1.toDVector

  private lazy val testBzVector2 = BDV.rand(10, RandBasis.withSeed(61).gaussian)
  private lazy val testDVector2 = testBzVector2.toDVector

  private lazy val testBzMatrix = BDM.rand(10, 20, RandBasis.withSeed(79).gaussian)
  private lazy val testDMatrix = testBzMatrix.toDMatrix

  implicit val equalityBDV: Equality[BDV[Double]] = breezeVectorEq(1E-8)
  implicit val equalityBDM: Equality[BDM[Double]] = breezeMatrixEq(1E-8)

  test("MatrixOps.size computes correctly") {
    assert(DMatrixOps.size(testDMatrix) === BzMatrixOps.size(testBzMatrix))
  }

  test("MatrixOps.transpose computes correctly") {
    implicit val matrixOps: MatrixOps[DMatrix, DVector] = DMatrixOps
    assert(DMatrixOps.transpose(testDMatrix).toBreeze === BzMatrixOps.transpose(testBzMatrix))
  }

  test("MatrixOps.colMean computes correctly") {
    implicit val vectorOps: VectorOps[DVector] = DVectorOps
    assert(DMatrixOps.colMean(testDMatrix).toBreeze === BzMatrixOps.colMean(testBzMatrix))
  }

  test("MatrixOps.centerColumns computes correctly") {
    implicit val matrixOps: MatrixOps[DMatrix, DVector] = DMatrixOps
    assert(DMatrixOps.centerColumns(testDMatrix).toBreeze === BzMatrixOps.centerColumns(testBzMatrix))
  }

  test("MatrixOps.gemv computes correctly") {
    implicit val vectorOps: VectorOps[DVector] = DVectorOps
    val result1 = DMatrixOps.gemv(testDMatrix, testDVector1, Some(testDVector2), 2.0, 3.0).toBreeze
    val result2 = BzMatrixOps.gemv(testBzMatrix, testBzVector1, Some(testBzVector2), 2.0, 3.0)
    assert(result1 === result2)
  }
}
