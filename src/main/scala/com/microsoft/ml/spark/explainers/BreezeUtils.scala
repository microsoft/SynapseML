package com.microsoft.ml.spark.explainers

import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.ml.linalg.{Vector => SV}

object BreezeUtils {
  implicit class SparkVectorCanConvertToBreeze(sv: SV) {
    def toBreeze: BDV[Double] = {
      BDV(sv.toArray)
    }
  }
}
