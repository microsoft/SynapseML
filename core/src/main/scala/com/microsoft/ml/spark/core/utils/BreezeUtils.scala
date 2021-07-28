package com.microsoft.ml.spark.core.utils

import org.apache.spark.ml.linalg.{Matrices, Matrix, Vector, Vectors}
import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM, SparseVector => BSV, Vector => BV}

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
