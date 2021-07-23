// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV, DenseMatrix => BDM}
import org.apache.spark.ml.linalg.{Vector, Vectors, Matrix, Matrices}

object BreezeUtils {
  implicit class SparkVectorCanConvertToBreeze(sv: Vector) {
    def toBreeze: BDV[Double] = {
      BDV(sv.toArray)
    }
  }

  implicit class SparkMatrixCanConvertToBreeze(mat: Matrix) {
    def toBreeze: BDM[Double] = {
      BDM(mat.rowIter.map(_.toBreeze).toArray: _*)
    }
  }

  implicit class BreezeVectorCanConvertToSpark(bv: BV[Double]) {
    def toSpark: Vector = {
      bv match {
        case v: BDV[Double] => Vectors.dense(v.toArray)
        case v: BSV[Double] => Vectors.sparse(v.size, v.activeIterator.toSeq).compressed
      }
    }
  }

  implicit class BreezeMatrixCanConvertToSpark(bm: BDM[Double]) {
    def toSpark: Matrix = {
      Matrices.dense(bm.rows, bm.cols, bm.data)
    }
  }
}
