// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.util.MLReadable

class RankingAdapterSpec extends RankingTestBase with EstimatorFuzzing[RankingAdapter] {
  override def testObjects(): Seq[TestObject[RankingAdapter]] = {
    List(new TestObject(adapter, transformedDf))
  }

  override def reader: MLReadable[_] = RankingAdapter

  override def modelReader: MLReadable[_] = RankingAdapterModel
}

class RankingAdapterModelSpec extends RankingTestBase with TransformerFuzzing[RankingAdapterModel] {
  override def testObjects(): Seq[TestObject[RankingAdapterModel]] = {
    val df = transformedDf
    List(new TestObject(adapter.fit(df), df))
  }

  override def reader: MLReadable[_] = RankingAdapterModel
}
