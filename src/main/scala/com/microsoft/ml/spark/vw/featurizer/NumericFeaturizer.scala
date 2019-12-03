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
  */
class NumericFeaturizer[T <: AnyVal{ def toDouble:Double }](override val fieldIdx: Int,
                                 override val columnName: String,
                                 val namespaceHash: Int,
                                 val mask: Int)
  extends Featurizer(fieldIdx) with ElementFeaturizer[T] {

  /**
    * Pre-hashed feature index.
    */
  val featureIdx = mask & VowpalWabbitMurmur.hash(columnName, namespaceHash)

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
    if (value != 0) {
      indices += featureIdx + idx
      values += value.toDouble
    }
    ()
  }
}

class NullableNumericFeaturizer[T <: AnyVal{ def toDouble:Double }](override val fieldIdx: Int,
                           override val columnName: String,
                           override val namespaceHash: Int,
                           override val mask: Int)
  extends NumericFeaturizer[T](fieldIdx, columnName, namespaceHash, mask) {
  override def featurize(row: Row,
                         indices: mutable.ArrayBuilder[Int],
                         values: mutable.ArrayBuilder[Double]): Unit =
    if (!row.isNullAt(fieldIdx))
      super.featurize(row, indices, values)
}