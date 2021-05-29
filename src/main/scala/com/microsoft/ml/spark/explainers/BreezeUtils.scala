package com.microsoft.ml.spark.explainers

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import org.apache.spark.ml.linalg.{Vector => SV, Vectors => SVS}

object BreezeUtils {
  implicit class SparkVectorCanConvertToBreeze(sv: SV) {
    def toBreeze: BDV[Double] = {
      BDV(sv.toArray)
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
}
