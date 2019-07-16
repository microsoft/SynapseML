// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.vw.featurizer

import org.apache.spark.sql.Row

import scala.collection.mutable.ArrayBuilder

abstract class Featurizer(val fieldIdx: Int) extends Serializable {

  /**
    * Featurize a single row.
    * @param row input row.
    * @param indices output indices.
    * @param values output values.
    * @note this interface isn't very Scala-esce, but it avoids lots of allocation.
    *       Also due to SparseVector limitations we don't support 64bit indices (e.g. indices are signed 32bit ints)
    */
  def featurize(row: Row, indices: ArrayBuilder[Int], values: ArrayBuilder[Double]): Unit
}
