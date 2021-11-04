// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw.featurizer

import org.apache.spark.sql.Row
import org.vowpalwabbit.spark.VowpalWabbitMurmur

import scala.collection.mutable

/**
  * Featurize map of type T into native VW structure. (hash(column name + k):value)
  * @param fieldIdx input field index.
  * @param columnName used as feature name prefix.
  * @param namespaceHash pre-hashed namespace.
  * @param mask bit mask applied to final hash.
  * @param valueFeaturizer featurizer for value type.
  * @tparam T value type.
  */
private[ml] class MapFeaturizer[T](override val fieldIdx: Int, override val columnName: String, val namespaceHash: Int,
                       val mask: Int, val valueFeaturizer: T => Double)
  extends Featurizer(fieldIdx) {

  /**
    * Featurize a single row.
    * @param row input row.
    * @param indices output indices.
    * @param values output values.
    * @note this interface isn't very Scala idiomatic, but it avoids lots of allocation.
    *       Also due to SparseVector limitations we don't support 64bit indices (e.g. indices are signed 32bit ints)
    */
  override def featurize(row: Row,
                         indices: mutable.ArrayBuilder[Int],
                         values: mutable.ArrayBuilder[Double]): Unit = {
    for ((k,v) <- row.getMap[String, T](fieldIdx).iterator) {
      val value = valueFeaturizer(v)

      // Note: 0 valued features are always filtered.
      if (value != 0) {
        indices += mask & VowpalWabbitMurmur.hash(columnName + k, namespaceHash)
        values += value
      }
    }
  }
}
