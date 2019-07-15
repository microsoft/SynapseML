// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.featurizer

import com.microsoft.ml.spark.VowpalWabbitMurmurWithPrefix
import org.apache.spark.sql.Row

import scala.collection.mutable.ArrayBuilder

/**
  * Featurize string into native VW structure. (hash(column name + value):1)
  * @param fieldIdx input field index.
  * @param columnName used as feature name prefix.
  * @param namespaceHash pre-hashed namespace.
  * @param mask bit mask applied to final hash.
  */
class StringFeaturizer(override val fieldIdx: Int, val columnName: String, val namespaceHash: Int, val mask: Int)
  extends Featurizer(fieldIdx) {

  /**
    * Pre-hashed feature index.
    */
  val hasher = new VowpalWabbitMurmurWithPrefix(columnName)

  /**
    * Featurize a single row.
    * @param row input row.
    * @param indices output indices.
    * @param values output values.
    * @note this interface isn't very Scala-esce, but it avoids lots of allocation.
    *       Also due to SparseVector limitations we don't support 64bit indices (e.g. indices are signed 32bit ints)
    */
  override def featurize(row: Row, indices: ArrayBuilder[Int], values: ArrayBuilder[Double]): Unit = {
    indices += mask & hasher.hash(row.getString(fieldIdx), namespaceHash)
    values += 1.0

    ()
  }
}
