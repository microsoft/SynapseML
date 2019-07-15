// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.featurizer

import org.apache.spark.sql.Row
import org.vowpalwabbit.spark.VowpalWabbitMurmur

import scala.collection.mutable.{ArrayBuilder}

/**
  * Featurize boolean value into native VW structure. (True = hash(feature name):1, False ignored).
  * @param fieldIdx input field index.
  * @param columnName used as feature name.
  * @param namespaceHash pre-hashed namespace.
  * @param mask bit mask applied to final hash.
  */
class BooleanFeaturizer(override val fieldIdx: Int, columnName: String, namespaceHash: Int, mask: Int)
  extends Featurizer(fieldIdx) {

  /**
    * Pre-hashed feature index.
    */
  val featureIdx = mask & VowpalWabbitMurmur.hash(columnName, namespaceHash)

  /**
    * Featurize a single row.
    * @param row input row.
    * @param indices output indices.
    * @param values output values.
    * @note this interface isn't very Scala-esce, but it avoids lots of allocation.
    *       Also due to SparseVector limitations we don't support 64bit indices (e.g. indices are signed 32bit ints)
    */
  override def featurize(row: Row, indices: ArrayBuilder[Int], values: ArrayBuilder[Double]): Unit = {
    if (row.getBoolean(fieldIdx)) {
        indices += featureIdx
        values += 1.0
    }

    ()
  }
}

