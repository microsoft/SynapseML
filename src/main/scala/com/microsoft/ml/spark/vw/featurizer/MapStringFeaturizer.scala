// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.vw.featurizer

import com.microsoft.ml.spark.vw.VowpalWabbitMurmurWithPrefix
import org.apache.spark.sql.Row

import scala.collection.mutable.ArrayBuilder

/**
  * Featurize map strings into native VW structure. (hash(column name + k + v):1)
  * @param fieldIdx input field index.
  * @param columnName ignored.
  * @param namespaceHash pre-hashed namespace.
  * @param mask bit mask applied to final hash.
  */
class MapStringFeaturizer(override val fieldIdx: Int, val columnName: String, namespaceHash: Int, val mask: Int)
  extends Featurizer(fieldIdx) {

  /**
    * Initialize hasher that already pre-hashes the column prefix.
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
    for ((k,v) <- row.getMap[String, String](fieldIdx).iterator) {
      indices += mask & hasher.hash(k + v, namespaceHash)
      values += 1.0
    }
  }
}

