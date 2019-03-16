// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.featurizer

import org.apache.spark.sql.Row
import org.vowpalwabbit.bare.VowpalWabbitMurmur

import scala.collection.mutable.ArrayBuffer

class BooleanFeaturizer(override val fieldIdx:Int, columnName:String, namespaceHash:Int)
  extends Featurizer(fieldIdx) {

    val featureIdx = Featurizer.maxIndexMask & VowpalWabbitMurmur.hash(columnName, namespaceHash)

    override def featurize(row: Row, indices:ArrayBuffer[Int], values:ArrayBuffer[Double]) = {
      if (row.getBoolean(fieldIdx)) {
          indices += featureIdx
          values += 1.0
      }

      ()
    }
}

