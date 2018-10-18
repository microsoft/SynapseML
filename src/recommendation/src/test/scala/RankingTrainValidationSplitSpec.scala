// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame

import scala.language.existentials

class RankingTrainValidationSplitSpec extends TestBase with EstimatorFuzzing[RankingTrainValidationSplit] {

  test("testALS") {

    val userCol = "customerIDOrg"
    val itemCol = "itemIDOrg"
    val ratingCol = "rating"

    val userColIndex = "customerID"
    val itemColIndex = "itemID"

    val dfRaw2: DataFrame = session
      .createDataFrame(Seq(
        ("11", "Movie 01", 4),
        ("11", "Movie 03", 1),
        ("11", "Movie 04", 5),
        ("11", "Movie 05", 3),
        ("11", "Movie 06", 4),
        ("11", "Movie 07", 1),
        ("11", "Movie 08", 5),
        ("11", "Movie 09", 3),
        ("22", "Movie 01", 4),
        ("22", "Movie 02", 5),
        ("22", "Movie 03", 1),
        ("22", "Movie 05", 3),
        ("22", "Movie 06", 4),
        ("22", "Movie 07", 5),
        ("22", "Movie 08", 1),
        ("22", "Movie 10", 3),
        ("33", "Movie 01", 4),
        ("33", "Movie 03", 1),
        ("33", "Movie 04", 5),
        ("33", "Movie 05", 3),
        ("33", "Movie 06", 4),
        ("33", "Movie 08", 1),
        ("33", "Movie 09", 5),
        ("33", "Movie 10", 3),
        ("44", "Movie 01", 4),
        ("44", "Movie 02", 5),
        ("44", "Movie 03", 1),
        ("44", "Movie 05", 3),
        ("44", "Movie 06", 4),
        ("44", "Movie 07", 5),
        ("44", "Movie 08", 1),
        ("44", "Movie 10", 3)))
      .toDF(userCol, itemCol, ratingCol)

    val ratings = dfRaw2.dropDuplicates()

    val customerIndex = new StringIndexer()
      .setInputCol(userCol)
      .setOutputCol(userColIndex)

    val itemIndex = new StringIndexer()
      .setInputCol(itemCol)
      .setOutputCol(itemColIndex)

    val pipeline = new Pipeline()
      .setStages(Array(customerIndex, itemIndex))

    val transformedDf = pipeline.fit(ratings).transform(ratings)

    val als = new ALS()
      .setUserCol(customerIndex.getOutputCol)
      .setRatingCol(ratingCol)
      .setItemCol(itemIndex.getOutputCol)

    val paramGrid = new ParamGridBuilder()
      .addGrid(als.regParam, Array(1.0))
      .build()

    val evaluator = new RankingEvaluator()
      .setK(3)
      .setNItems(10)

    val rankingTrainValidationSplit = new RankingTrainValidationSplit()
      .setEstimator(als)
      .setEstimatorParamMaps(paramGrid)
      .setEvaluator(evaluator)
      .setTrainRatio(0.8)
      .setCollectSubMetrics(true)

    val model = rankingTrainValidationSplit.fit(transformedDf)

    val items = model.recommendForAllUsers(3)
    val users = model.recommendForAllItems(3)
    model.subMetrics.foreach(println(_))
  }

  override def testObjects(): Seq[TestObject[RankingTrainValidationSplit]] = {
    val userCol = "customerIDOrg"
    val itemCol = "itemIDOrg"
    val ratingCol = "rating"

    val userColIndex = "customerID"
    val itemColIndex = "itemID"

    val dfRaw2: DataFrame = session
      .createDataFrame(Seq(
        ("11", "Movie 01", 4),
        ("11", "Movie 03", 1),
        ("22", "Movie 01", 4),
        ("22", "Movie 02", 5),
        ("33", "Movie 01", 4),
        ("33", "Movie 03", 1),
        ("44", "Movie 01", 4),
        ("44", "Movie 02", 5),
        ("44", "Movie 03", 1)))
      .toDF(userCol, itemCol, ratingCol)

    val ratings = dfRaw2.dropDuplicates()

    val customerIndex = new StringIndexer()
      .setInputCol(userCol)
      .setOutputCol(userColIndex)

    val itemIndex = new StringIndexer()
      .setInputCol(itemCol)
      .setOutputCol(itemColIndex)

    val pipeline = new Pipeline()
      .setStages(Array(customerIndex, itemIndex))

    val transformedDf = pipeline.fit(ratings).transform(ratings)

    val als = new ALS()
      .setUserCol(customerIndex.getOutputCol)
      .setRatingCol(ratingCol)
      .setItemCol(itemIndex.getOutputCol)

    val paramGrid = new ParamGridBuilder()
      .addGrid(als.regParam, Array(1.0))
      .build()

    val evaluator = new RankingEvaluator()
      .setK(1)
      .setNItems(3)

    val rankingTrainValidationSplit = new RankingTrainValidationSplit()
      .setEstimator(als)
      .setEstimatorParamMaps(paramGrid)
      .setEvaluator(evaluator)
      .setTrainRatio(0.8)

    List(new TestObject(rankingTrainValidationSplit, transformedDf))
  }

  override def reader: MLReadable[_] = RankingTrainValidationSplit

  override def modelReader: MLReadable[_] = RankingTrainValidationSplitModel
}
