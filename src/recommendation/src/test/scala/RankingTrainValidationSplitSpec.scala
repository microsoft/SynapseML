// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.recommendation.HasRecommenderCols
import org.apache.spark.ml.tuning.{TrainValidationSplit, TrainValidationSplitModel}
import org.apache.spark.sql.{DataFrame, RankingDataSet}

class RankingTrainValidationSplitSpec extends RankingTestBase {

  test("testALSSparkTVS") {

    import scala.language.implicitConversions

    val rankingAdapter = new RankingAdapter()
      .setMode("allUsers")
      .setK(evaluator.getK)
      .setRecommender(als)
      .setUserCol(als.getUserCol)
      .setItemCol(als.getItemCol)
      .setRatingCol(als.getRatingCol)

    val trainValidationSplit = (new TrainValidationSplit() with HasRecommenderCols)
      .setEstimator(rankingAdapter)
      .setEstimatorParamMaps(paramGrid)
      .setEvaluator(evaluator)
      .setTrainRatio(0.8)
      .setUserCol(als.getUserCol)
      .setItemCol(als.getItemCol)
      .setRatingCol(als.getRatingCol)

    val df = pipeline.fit(ratings).transform(ratings)

    val rankingDF = RankingDataSet.toRankingDataSet[Any](df)
      .setUserCol(als.getUserCol)
      .setItemCol(als.getItemCol)
      .setRatingCol(als.getRatingCol)

    val model = trainValidationSplit.fit(rankingDF)

    class RankingTrainValidationSplitModel(trainValidationSplitModel: TrainValidationSplitModel) {
      def recommendForAllUsers(k: Int): DataFrame = trainValidationSplitModel
        .bestModel
        .asInstanceOf[RankingAdapterModel]
        .recommendForAllUsers(k)
    }

    implicit def tvsm2RTVSM(trainValidationSplitModel: TrainValidationSplitModel): RankingTrainValidationSplitModel =
      new RankingTrainValidationSplitModel(trainValidationSplitModel)

    val items = model.recommendForAllUsers(3)
    print(items)
  }

}
