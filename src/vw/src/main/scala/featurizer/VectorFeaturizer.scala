// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.featurizer

import org.apache.spark.sql.Row
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}

import scala.collection.mutable.ArrayBuilder

class VectorFeaturizer(override val fieldIdx: Int, val mask: Int)
  extends Featurizer(fieldIdx) {

  override def featurize(row: Row, indices: ArrayBuilder[Int], values: ArrayBuilder[Double]): Unit = {

    row.getAs[Vector](fieldIdx) match {
      case v:DenseVector => {
        // check if we need to hash
        if (v.size < mask + 1)
          indices ++= 0 until v.size
        else
          indices ++= (0 until v.size).map { mask & _ }

        values ++= v.values
      }
      case v:SparseVector => {
        // check if we need to hash
        if (v.size < mask + 1)
          indices ++= v.indices
        else
          indices ++= v.indices.map { mask & _ }

        values ++= v.values
      }
    }

    ()
  }
}
