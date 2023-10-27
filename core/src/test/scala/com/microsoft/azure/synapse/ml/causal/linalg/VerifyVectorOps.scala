// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal.linalg

import breeze.linalg.{DenseVector => BDV}
import breeze.stats.distributions.RandBasis
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.scalactic.{Equality, TolerantNumerics}

class VerifyVectorOps extends TestBase {
  spark

  private lazy val testBzVector1 = BDV.rand(20, RandBasis.withSeed(47).uniform)
  private lazy val testDVector1 = testBzVector1.toDVector

  private lazy val testBzVector2 = BDV.rand(20, RandBasis.withSeed(47).gaussian)
  private lazy val testDVector2 = testBzVector2.toDVector

  implicit val equalityDouble: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1E-8)
  implicit val equalityBDV: Equality[BDV[Double]] = breezeVectorEq(1E-8)


  test("VectorOps.nrm2 computes correctly") {
    assert(DVectorOps.nrm2(testDVector1) === BzVectorOps.nrm2(testBzVector1))
  }

  test("VectorOps.sum computes correctly") {
    assert(DVectorOps.sum(testDVector1) === BzVectorOps.sum(testBzVector1))
  }

  test("VectorOps.mean computes correctly") {
    assert(DVectorOps.mean(testDVector1) === BzVectorOps.mean(testBzVector1))
  }

  test("VectorOps.dot computes correctly") {
    assert(DVectorOps.dot(testDVector1, testDVector2) === BzVectorOps.dot(testBzVector1, testBzVector2))
  }

  test("VectorOps.maxAbs computes correctly") {
    assert(DVectorOps.maxAbs(testDVector2) === BzVectorOps.maxAbs(testBzVector2))
  }

  test("VectorOps.axpy computes correctly") {
    implicit val vectorOps: VectorOps[DVector] = DVectorOps
    val result = DVectorOps.axpy(testDVector1, Some(testDVector2), 2.5)
    assert(result.toBreeze === BzVectorOps.axpy(testBzVector1, Some(testBzVector2), 2.5))
  }

  test("VectorOps.center computes correctly") {
    implicit val vectorOps: VectorOps[DVector] = DVectorOps
    val result = DVectorOps.center(testDVector1)
    assert(result.toBreeze === BzVectorOps.center(testBzVector1))
  }

  test("VectorOps.elementwiseProduct computes correctly") {
    implicit val vectorOps: VectorOps[DVector] = DVectorOps
    val result = DVectorOps.elementwiseProduct(testDVector1, testDVector2)
    assert(result.toBreeze === BzVectorOps.elementwiseProduct(testBzVector1, testBzVector2))
  }

  test("VectorOps.exp computes correctly") {
    implicit val vectorOps: VectorOps[DVector] = DVectorOps
    val result = DVectorOps.exp(testDVector1)
    assert(result.toBreeze === BzVectorOps.exp(testBzVector1))
  }

  test("VectorOps.uniformRandom computes correctly") {
    implicit val vectorOps: VectorOps[DVector] = DVectorOps
    val result1 = DVectorOps.make(20, 1d/20).toBreeze
    val result2 = BzVectorOps.make(20, 1d/20)
    assert(result1 === result2)
    assert(result1.forall(0 <= _ && _ <= 1))
    assert(result2.forall(0 <= _ && _ <= 1))
  }
}
