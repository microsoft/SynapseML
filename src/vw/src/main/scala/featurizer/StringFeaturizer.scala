// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.featurizer

import com.microsoft.ml.spark.VowpalWabbitMurmurWithPrefix
import org.apache.spark.sql.Row

import scala.collection.mutable.ArrayBuilder

class StringFeaturizer(override val fieldIdx: Int, val columnName: String, val namespaceHash: Int, val mask: Int)
  extends Featurizer(fieldIdx) {
  val hasher = new VowpalWabbitMurmurWithPrefix(columnName)

    override def featurize(row: Row, indices: ArrayBuilder[Int], values: ArrayBuilder[Double]): Unit = {
      indices += mask & hasher.hash(row.getString(fieldIdx), namespaceHash)
      values += 1.0

      ()
    }
}
