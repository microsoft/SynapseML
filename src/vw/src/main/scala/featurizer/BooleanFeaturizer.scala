// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.featurizer

import org.apache.spark.sql.Row
import org.vowpalwabbit.bare.VowpalWabbitMurmur

import scala.collection.mutable.{ArrayBuilder}

class BooleanFeaturizer(override val fieldIdx: Int, columnName: String, namespaceHash: Int)
  extends Featurizer(fieldIdx) {

    val featureIdx = Featurizer.maxIndexMask & VowpalWabbitMurmur.hash(columnName, namespaceHash)

    override def featurize(row: Row, indices: ArrayBuilder[Int], values: ArrayBuilder[Double]): Unit = {
      if (row.getBoolean(fieldIdx)) {
          indices += featureIdx
          values += 1.0
      }

      ()
    }
}

