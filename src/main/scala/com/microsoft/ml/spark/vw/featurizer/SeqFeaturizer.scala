// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.vw.featurizer

import org.apache.spark.sql.Row

import scala.collection.mutable

/**
  * Featurize array of strings into native VW structure. (hash(column name + k):1)
  * @param fieldIdx input field index.
  * @param columnName used as feature name prefix.
  */
class SeqFeaturizer[E](override val fieldIdx: Int,
                       override val columnName: String,
                       val featurizer: Featurizer)
  extends Featurizer(fieldIdx) with ElementFeaturizer[Seq[E]] {

  private val elementFeaturizer = featurizer.asInstanceOf[ElementFeaturizer[E]]

  /**
    * Featurize a single row.
    * @param row input row.
    * @param indices output indices.
    * @param values output values.
    * @note this interface isn't very Scala-esce, but it avoids lots of allocation.
    *       Also due to SparseVector limitations we don't support 64bit indices (e.g. indices are signed 32bit ints)
    */
  override def featurize(row: Row, indices: mutable.ArrayBuilder[Int], values: mutable.ArrayBuilder[Double]): Unit = {
    // loop over sequence and pass the offset
    featurize(0, row.getSeq[E](fieldIdx), indices, values)
  }

  def featurize(idx: Int,
                value: Seq[E],
                indices: mutable.ArrayBuilder[Int],
                values: mutable.ArrayBuilder[Double]): Unit = {
    for((v, i) <- value.view.zipWithIndex)
      elementFeaturizer.featurize(i, v, indices, values)
  }
}