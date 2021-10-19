// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw.featurizer

import org.apache.spark.sql.Row
import org.vowpalwabbit.spark.VowpalWabbitMurmur

import scala.collection.mutable

/**
  * Featurize numeric values into native VW structure. ((hash(column name):value)
  * @param fieldIdx input field index.
  * @param columnName used as feature name prefix.
  * @param namespaceHash pre-hashed namespace.
  * @param mask bit mask applied to final hash.
  */
private[ml] class NumericFeaturizer[T: Numeric](override val fieldIdx: Int,
                                 override val columnName: String,
                                 val namespaceHash: Int,
                                 val mask: Int,
                                 val zero: Numeric[T])
  extends Featurizer(fieldIdx) with ElementFeaturizer[T] {

  /**
    * Pre-hashed feature index.
    */
  val featureIdx: Int = mask & VowpalWabbitMurmur.hash(columnName, namespaceHash)

  override def featurize(row: Row,
                         indices: mutable.ArrayBuilder[Int],
                         values: mutable.ArrayBuilder[Double]): Unit = {
      featurize(0, row.getAs[T](fieldIdx), indices, values)
  }

  def featurize(idx: Int,
                value: T,
                indices: mutable.ArrayBuilder[Int],
                values: mutable.ArrayBuilder[Double]): Unit = {
    // Note: 0 valued features are always filtered.
    if (value != zero.zero) {
      indices += featureIdx + idx
      // This is weird but zero is a numeric typeclass that is used to convert the generic T to a double.
      values += zero.toDouble(value)
    }
    ()
  }
}

class NullableNumericFeaturizer[T: Numeric](override val fieldIdx: Int,
                           override val columnName: String,
                           override val namespaceHash: Int,
                           override val mask: Int,
                           override val zero: Numeric[T])
  extends NumericFeaturizer[T](fieldIdx, columnName, namespaceHash, mask, zero) {
  override def featurize(row: Row,
                         indices: mutable.ArrayBuilder[Int],
                         values: mutable.ArrayBuilder[Double]): Unit =
    if (!row.isNullAt(fieldIdx))
      super.featurize(row, indices, values)
}
