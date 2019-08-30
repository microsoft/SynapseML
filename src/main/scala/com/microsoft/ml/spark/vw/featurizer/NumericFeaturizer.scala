// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.vw.featurizer

import org.apache.spark.sql.Row
import org.vowpalwabbit.spark.VowpalWabbitMurmur

import scala.collection.mutable

/**
  * Featurize numeric values into native VW structure. ((hash(column name):value)
  * @param fieldIdx input field index.
  * @param columnName used as feature name prefix.
  * @param namespaceHash pre-hashed namespace.
  * @param mask bit mask applied to final hash.
  * @param getFieldValue lambda to unify the cast/conversion to double.
  */
class NumericFeaturizer(override val fieldIdx: Int, override val columnName: String, namespaceHash: Int,
                        mask: Int, val getFieldValue: Row => Double)
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
  override def featurize(row: Row,
                         indices: mutable.ArrayBuilder[Int],
                         values: mutable.ArrayBuilder[Double]): Unit = {
    val value = getFieldValue(row)

    // Note: 0 valued features are always filtered.
    if (value != 0) {
        indices += featureIdx
        values += value
    }
    ()
  }
}
