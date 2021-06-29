// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.vw.featurizer

import org.apache.spark.sql.Row
import org.vowpalwabbit.spark.VowpalWabbitMurmur

import scala.collection.mutable

/**
  * Featurize numeric values into native VW structure. ((hash(column name):value)
  * @param fieldIdx input field index.
  */
private[ml] class StructFeaturizer(override val fieldIdx: Int,
                       override val columnName: String,
                       fieldFeaturizer: Seq[Featurizer])
  extends Featurizer(fieldIdx) with ElementFeaturizer[Row] {

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
                         values: mutable.ArrayBuilder[Double]): Unit =
    featurize(fieldIdx, row.getStruct(fieldIdx), indices, values)

  def featurize(idx: Int,
                value: Row,
                indices: mutable.ArrayBuilder[Int],
                values: mutable.ArrayBuilder[Double]): Unit = {

    for (f <- fieldFeaturizer)
      f.featurize(value, indices, values)
  }
}

private[ml] class NullableStructFeaturizer(override val fieldIdx: Int,
                               override val columnName: String,
                               fieldFeaturizer: Seq[Featurizer])
 extends StructFeaturizer(fieldIdx, columnName, fieldFeaturizer) {

  override def featurize(row: Row,
                         indices: mutable.ArrayBuilder[Int],
                         values: mutable.ArrayBuilder[Double]): Unit =
    if (!row.isNullAt(fieldIdx))
      super.featurize(row, indices, values)
}
