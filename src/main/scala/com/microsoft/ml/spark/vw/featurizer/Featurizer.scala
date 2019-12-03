// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.vw.featurizer

import com.microsoft.ml.spark.vw.VowpalWabbitMurmurWithPrefix
import org.apache.spark.sql.Row

import scala.collection.mutable

abstract class Featurizer(val fieldIdx: Int) extends Serializable {

  val columnName: String

  /**
    * Initialize hasher that already pre-hashes the column prefix.
    */
  protected lazy val hasher: VowpalWabbitMurmurWithPrefix = new VowpalWabbitMurmurWithPrefix(columnName)

  /**
    * Featurize a single row.
    * @param row input row.
    * @param indices output indices.
    * @param values output values.
    * @note this interface isn't very Scala idiomatic, but it avoids lots of allocation.
    *       Also due to SparseVector limitations we don't support 64bit indices (e.g. indices are signed 32bit ints)
    */
  def featurize(row: Row,
                indices: mutable.ArrayBuilder[Int],
                values: mutable.ArrayBuilder[Double]): Unit
}