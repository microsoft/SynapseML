// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.vw.featurizer

import org.apache.spark.sql.Row

import scala.collection.mutable

/**
  * Featurize string into native VW structure. (hash(column name + value):1)
  * @param fieldIdx input field index.
  * @param columnName used as feature name prefix.
  * @param namespaceHash pre-hashed namespace.
  * @param mask bit mask applied to final hash.
  */
class StringFeaturizer(override val fieldIdx: Int,
                       override val columnName: String,
                       val namespaceHash: Int,
                       val mask: Int)
  extends Featurizer(fieldIdx) {

  /**
    * Featurize a single row.
    * @param row input row.
    * @param indices output indices.
    * @param values output values.
    * @note this interface isn't very Scala-esce, but it avoids lots of allocation.
    *       Also due to SparseVector limitations we don't support 64bit indices (e.g. indices are signed 32bit ints)
    */
  override def featurize(row: Row, indices: mutable.ArrayBuilder[Int], values: mutable.ArrayBuilder[Double]): Unit = {
    indices += mask & hasher.hash(row.getString(fieldIdx), namespaceHash)
    values += 1.0

    ()
  }
}
