// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.recommendation

import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject, TransformerFuzzing}
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.types.{DoubleType, StringType}

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

  test("splitDF preserves string identifiers with and without ratings") {
    import spark.implicits._
    val stringRatings = Seq(
      ("user-a", "item-a", 1.0),
      ("user-a", "item-b", 2.0),
      ("user-b", "item-a", 3.0),
      ("user-b", "item-c", 4.0)
    ).toDF("user", "item", "rating")
    val splitter = new RankingTrainValidationSplit()
      .setUserCol("user")
      .setItemCol("item")
      .setRatingCol("rating")
      .setTrainRatio(0.5)

    val ratedParts = splitter.splitDF(stringRatings)
    ratedParts.foreach(part => {
      assert(part.schema("user").dataType == StringType)
      assert(part.schema("item").dataType == StringType)
      assert(part.schema("rating").dataType == DoubleType)
      assert(part.filter(part("item").isNull).count() == 0)
    })
    val recombinedRatings = ratedParts.reduce(_.unionByName(_))
    assert(recombinedRatings.exceptAll(stringRatings).count() == 0)
    assert(stringRatings.exceptAll(recombinedRatings).count() == 0)

    val stringInteractions = stringRatings.select("user", "item")
    val interactionParts = splitter.splitDF(stringInteractions)
    interactionParts.foreach(part => {
      assert(part.schema("user").dataType == StringType)
      assert(part.schema("item").dataType == StringType)
      assert(part.filter(part("item").isNull).count() == 0)
    })
    val recombinedInteractions = interactionParts.reduce(_.unionByName(_))
    assert(recombinedInteractions.exceptAll(stringInteractions).count() == 0)
    assert(stringInteractions.exceptAll(recombinedInteractions).count() == 0)
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
