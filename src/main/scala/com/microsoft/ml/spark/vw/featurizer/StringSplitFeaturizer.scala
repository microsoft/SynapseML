// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.vw.featurizer

import com.microsoft.ml.spark.vw.VowpalWabbitMurmurWithPrefix
import org.apache.spark.sql.Row

import scala.collection.mutable
import scala.util.matching.Regex

/**
  * Featurize strings by splitting into native VW structure. (hash(s(0)):value, hash(s(1)):value, ...)
  * @param fieldIdx input field index.
  * @param columnName used as feature name prefix.
  * @param namespaceHash pre-hashed namespace.
  * @param mask bit mask applied to final hash.
  */
private[ml] class StringSplitFeaturizer(override val fieldIdx: Int,
                            override val columnName: String,
                            val namespaceHash: Int,
                            val mask: Int)
  extends Featurizer(fieldIdx) {

  /**
    * (?U) makes \w unicode aware
    * https://stackoverflow.com/questions/4304928/unicode-equivalents-for-w-and-b-in-java-regular-expressions
    * we could follow
    * https://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.CountVectorizer.html
    * but that strips single character words...
    *
    * TODO: expose as user configurable parameter
    */
  val nonWhiteSpaces: Regex = "(?U)\\w+".r

  /**
    * Featurize a single row.
    * @param row input row.
    * @param indices output indices.
    * @param values output values.
    * @note this interface isn't very Scala-esce, but it avoids lots of allocation.
    *       Also due to SparseVector limitations we don't support 64bit indices (e.g. indices are signed 32bit ints)
    */
  override def featurize(row: Row, indices: mutable.ArrayBuilder[Int], values: mutable.ArrayBuilder[Double]): Unit = {
    val s = row.getString(fieldIdx)

    for (e <- nonWhiteSpaces.findAllMatchIn(s)) {
      // Note: since the hasher access the chars directly. this avoids allocation.
      indices +=  mask & hasher.hash(s, e.start, e.end, namespaceHash)

      values += 1.0
    }
  }
}
