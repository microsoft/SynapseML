// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.DataFrame

import scala.language.existentials

class RankingTrainValidationSpec
  extends RankingTestBase {

  test("testALS") {

    val tvRecommendationSplit = new RankingTrainValidationSplit()
      .setEstimator(als)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)
      .setUserCol(customerIndex.getOutputCol)
      .setItemCol(itemIndex.getOutputCol)
      .setRatingCol("rating")

    val tvModel = tvRecommendationSplit.fit(transformedDf)

    val model = tvModel.getBestModel.asInstanceOf[ALSModel]

    val items = model.recommendForAllUsers(3)
    val users = model.recommendForAllItems(3)

//    evaluator.setSaveAll(true)
//    tvRecommendationSplit.fit(transformedDf)
//    evaluator.printMetrics()
  }

}
