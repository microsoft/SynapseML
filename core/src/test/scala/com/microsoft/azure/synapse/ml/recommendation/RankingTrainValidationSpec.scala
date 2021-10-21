// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.recommendation

import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject, TransformerFuzzing}
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.ml.util.MLReadable

class RankingTrainValidationSplitSpec extends RankingTestBase with EstimatorFuzzing[RankingTrainValidationSplit] {

  test("testALS") {

    val tvRecommendationSplit = new RankingTrainValidationSplit()
      .setEstimator(als)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)
      .setUserCol(recommendationIndexer.getUserOutputCol)
      .setItemCol(recommendationIndexer.getItemOutputCol)
      .setRatingCol("rating")

    val tvModel = tvRecommendationSplit.fit(transformedDf)

    val model = tvModel.getBestModel.asInstanceOf[ALSModel]

    val items = model.recommendForAllUsers(3)
    assert(items.collect()(0)(0) == 1)

    val users = model.recommendForAllItems(3)
    assert(users.collect()(0)(0) == 4)

  }

  override def testObjects(): Seq[TestObject[RankingTrainValidationSplit]] = {
    List(new TestObject(rankingTrainValidationSplit, transformedDf))
  }

  override def reader: MLReadable[_] = RankingTrainValidationSplit

  override def modelReader: MLReadable[_] = RankingTrainValidationSplitModel
}

class RankingTrainValidationSplitModelSpec extends RankingTestBase with
  TransformerFuzzing[RankingTrainValidationSplitModel] {
  override def testObjects(): Seq[TestObject[RankingTrainValidationSplitModel]] = {
    List(new TestObject(rankingTrainValidationSplit.fit(transformedDf), transformedDf))
  }

  override def reader: MLReadable[_] = RankingTrainValidationSplitModel
}
