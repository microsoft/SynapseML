// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.utils

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV}
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.utils.BreezeUtils._
import org.apache.spark.ml.linalg.{Matrices, Vectors}

class VerifyBreezeUtils extends TestBase {

  test("Spark dense vector to Breeze and back roundtrip") {
    val sparkVec = Vectors.dense(1.0, 2.0, 3.0, 4.0)
    val breezeVec = sparkVec.toBreeze
    val result = breezeVec.toSpark
    assert(result === sparkVec)
  }

  test("Spark sparse vector conversion through Breeze dense vector") {
    val sparseVec = Vectors.sparse(5, Array(0, 2, 4), Array(1.0, 3.0, 5.0))
    val breezeVec = sparseVec.toBreeze
    assert(breezeVec.isInstanceOf[BDV[Double]])
    assert(breezeVec.length === 5)
    assert(breezeVec(0) === 1.0)
    assert(breezeVec(1) === 0.0)
    assert(breezeVec(2) === 3.0)
    assert(breezeVec(3) === 0.0)
    assert(breezeVec(4) === 5.0)
  }

  test("Breeze sparse vector to Spark") {
    val bsv = new BSV[Double](Array(0, 3), Array(10.0, 40.0), 5)
    val sparkVec = bsv.toSpark
    assert(sparkVec.size === 5)
    assert(sparkVec(0) === 10.0)
    assert(sparkVec(1) === 0.0)
    assert(sparkVec(2) === 0.0)
    assert(sparkVec(3) === 40.0)
    assert(sparkVec(4) === 0.0)
  }

  test("Spark matrix to Breeze and back roundtrip") {
    val sparkMat = Matrices.dense(2, 3, Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
    val breezeMat = sparkMat.toBreeze
    val result = breezeMat.toSpark
    assert(result === sparkMat)
  }

  test("Single-element vector conversion") {
    val sparkVec = Vectors.dense(42.0)
    val breezeVec = sparkVec.toBreeze
    assert(breezeVec.length === 1)
    assert(breezeVec(0) === 42.0)
    val result = breezeVec.toSpark
    assert(result === sparkVec)
  }

  test("Vector values are preserved accurately") {
    val values = Array(0.0, -1.5, Double.MaxValue, Double.MinValue, 1e-10)
    val sparkVec = Vectors.dense(values)
    val roundtripped = sparkVec.toBreeze.toSpark
    values.indices.foreach { i =>
      assert(roundtripped(i) === values(i))
    }
  }

  test("Matrix dimensions preserved through roundtrip") {
    val sparkMat = Matrices.dense(3, 4, Array.tabulate(12)(_.toDouble))
    val breezeMat = sparkMat.toBreeze
    assert(breezeMat.rows === 3)
    assert(breezeMat.cols === 4)
    val result = breezeMat.toSpark
    assert(result.numRows === 3)
    assert(result.numCols === 4)
  }
}
