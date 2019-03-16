// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.featurizer

import org.apache.spark.sql.Row
import scala.collection.mutable.ArrayBuffer

object Featurizer {
  val maxIndexMask: Int = ((1 << 31) - 1)
}

abstract class Featurizer(val fieldIdx:Int) extends Serializable {

  // TODO: review this, but due to SparseVector limitations we don't support large indices

  // this interface isn't very Scala-esce, but it avoids lots of allocation
  // def featurize(row:Row): Seq[Feature] but this requires multiple allocations for each invocation
  def featurize(row:Row, indices:ArrayBuffer[Int], values:ArrayBuffer[Double])
}