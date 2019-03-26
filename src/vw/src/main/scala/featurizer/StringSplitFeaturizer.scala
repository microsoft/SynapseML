// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.featurizer

import com.microsoft.ml.spark.VowpalWabbitMurmurWithPrefix
import org.apache.spark.sql.Row
import org.vowpalwabbit.bare.VowpalWabbitMurmur

import scala.collection.mutable.ArrayBuilder

class StringSplitFeaturizer(override val fieldIdx: Int, val columnName: String, val namespaceHash: Int)
  extends Featurizer(fieldIdx) {

  val nonWhiteSpaces = "\\S+".r
  val hasher = new VowpalWabbitMurmurWithPrefix(columnName)

  override def featurize(row: Row, indices: ArrayBuilder[Int], values: ArrayBuilder[Double]): Unit = {

    val s = row.getString(fieldIdx)

    for (e <- nonWhiteSpaces.findAllMatchIn(s)) {
      // Note: since the hasher access the chars directly it avoids any allocation
      indices += Featurizer.maxIndexMask & hasher.hash(s, e.start, e.end, namespaceHash)
      values += 1.0
    }
  }
}
