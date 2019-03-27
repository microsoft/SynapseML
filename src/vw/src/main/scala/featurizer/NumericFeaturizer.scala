// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.featurizer

import org.apache.spark.sql.Row
import org.vowpalwabbit.bare.VowpalWabbitMurmur

import scala.collection.mutable.{ArrayBuffer, ArrayBuilder}

class NumericFeaturizer(override val fieldIdx: Int, columnName: String, namespaceHash: Int,
                        mask: Int, val getFieldValue: (Row) => Double)
  extends Featurizer(fieldIdx) {

    val featureIdx = mask & VowpalWabbitMurmur.hash(columnName, namespaceHash)

    override def featurize(row: Row, indices: ArrayBuilder[Int], values: ArrayBuilder[Double]): Unit = {
      val value = getFieldValue(row)
      if (value != 0) {
          indices += featureIdx
          values += value
      }
      ()
    }
}
