// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

import com.microsoft.ml.spark._
import org.apache.spark.ml.util.MLReadable

class RankingAdapterSpec extends RankingTestBase with EstimatorFuzzing[RankingAdapter] {
  override def testObjects(): Seq[TestObject[RankingAdapter]] = {
    val est = new RankingAdapter()
      .setMode("allUsers") //allItems does not work, not sure if it would be used
      .setK(evaluator.getK)
      .setRecommender(als)
      .setUserCol(als.getUserCol)
      .setRatingCol(als.getRatingCol)
      .setItemCol(als.getItemCol)

    List(new TestObject(est, transformedDf))
  }

  override def reader: MLReadable[_] = RankingAdapter

  override def modelReader: MLReadable[_] = RankingAdapterModel
}
