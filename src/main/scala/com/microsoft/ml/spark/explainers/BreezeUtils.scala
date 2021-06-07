// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV, DenseMatrix => BDM}
import org.apache.spark.ml.linalg.{Vector => SV, Vectors => SVS, Matrix => SM, Matrices => SMS}

object BreezeUtils {
  implicit class SparkVectorCanConvertToBreeze(sv: SV) {
    def toBreeze: BDV[Double] = {
      BDV(sv.toArray)
    }
  }

  implicit class SparkMatrixCanConvertToBreeze(sm: SM) {
    def toBreeze: BDM[Double] = {
      BDM(sm.rowIter.map(_.toBreeze).toArray: _*)
    }
  }

  implicit class BreezeVectorCanConvertToSpark(bv: BV[Double]) {
    def toSpark: SV = {
      bv match {
        case v: BDV[Double] => SVS.dense(v.toArray)
        case v: BSV[Double] => SVS.sparse(v.size, v.activeIterator.toSeq).compressed
      }
    }
  }

  implicit class BreezeMatrixCanConvertToSpark(bm: BDM[Double]) {
    def toSpark: SM = {
      SMS.dense(bm.rows, bm.cols, bm.data)
    }
  }
}