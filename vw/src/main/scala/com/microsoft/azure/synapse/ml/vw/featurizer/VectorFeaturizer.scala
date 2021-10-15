// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw.featurizer

import org.apache.spark.sql.Row
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}

import scala.collection.mutable

/**
  * Featurize sparse and dense vector into native VW structure. (hash(s(0)):value, hash(s(1)):value, ...)
  * @param fieldIdx input field index.
  * @param mask bit mask applied to final hash.
  */
private[ml] class VectorFeaturizer(override val fieldIdx: Int, override val columnName: String, val mask: Int)
  extends Featurizer(fieldIdx) {

  /**
    * Featurize a single row.
    * @param row input row.
    * @param indices output indices.
    * @param values output values.
    * @note this interface isn't very Scala-esce, but it avoids lots of allocation.
    *       Also due to SparseVector limitations we don't support 64bit indices (e.g. indices are signed 32bit ints)
    */
  override def featurize(row: Row,
                         indices: mutable.ArrayBuilder[Int],
                         values: mutable.ArrayBuilder[Double]): Unit = {

    row.getAs[Vector](fieldIdx) match {
      case v: DenseVector =>
        // check if we need to hash
        if (v.size < mask + 1)
          indices ++= 0 until v.size
        else
          indices ++= (0 until v.size).map { mask & _ }

        values ++= v.values
      case v: SparseVector =>
        // check if we need to hash
        if (v.size < mask + 1)
          indices ++= v.indices
        else
          indices ++= v.indices.map { mask & _ }

        values ++= v.values
    }
    ()
  }
}
