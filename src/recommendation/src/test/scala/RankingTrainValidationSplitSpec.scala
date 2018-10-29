// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.{Estimator, Model, Pipeline}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.tuning.{CrossValidator, _}
import org.apache.spark.ml.util.{Identifiable, MLReadable}
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.language.existentials

class RankingTrainValidationSplitSpec extends RankingTestBase
  with EstimatorFuzzing[RankingTrainValidationSplit] {

  test("testALS2") {
    lazy val adapter: RankingAdapter = new RankingAdapter()
      .setMode("allUsers") //allItems does not work, not sure if it would be used
      .setK(evaluator.getK)
      .setRecommender(als)

    val model = rankingTrainValidationSplit.fit(transformedDf)

    val items = model.recommendForAllUsers(3)
    val users = model.recommendForAllItems(3)
    model.subMetrics.foreach(println(_))
  }

  test("testALS") {
    val model = rankingTrainValidationSplit.fit(transformedDf)

    val items = model.recommendForAllUsers(3)
    val users = model.recommendForAllItems(3)
    model.subMetrics.foreach(println(_))
  }

  test("testALSSparkTVS") {

    import scala.language.implicitConversions
    implicit def df2RDF(rankingDataFrame: Dataset[_]): RankingDataFrame =
      new RankingDataFrame("rdf", rankingDataFrame)

    val rankingAdapter = new RankingAdapter()
      .setMode("allUsers") //allItems does not work, not sure if it would be used
      .setK(evaluator.getK)
      .setRecommender(als)
      .setUserCol(als.getUserCol)
      .setItemCol(als.getItemCol)
      .setRatingCol(als.getRatingCol)

    val trainValidationSplit = new TrainValidationSplit() with HasRecommenderCols
    trainValidationSplit
      .setEstimator(rankingAdapter)
      .setEstimatorParamMaps(paramGrid)
      .setEvaluator(evaluator)
      .setTrainRatio(0.8)
      .setUserCol(als.getUserCol)
      .setItemCol(als.getItemCol)
      .setRatingCol(als.getRatingCol)

    val df = pipeline.fit(ratings).transform(ratings) //RankingDataFrame.randomSplit is not getting passed into this
    // as hoped for

    df.setUserCol(als.getUserCol)
      .setItemCol(als.getItemCol)
      .setRatingCol(als.getRatingCol)

    val model = trainValidationSplit.fit(df)

    class RankingTrainValidationSplitModel(trainValidationSplitModel: TrainValidationSplitModel) {
      def recommendForAllUsers(k: Int): DataFrame = trainValidationSplitModel.bestModel
        .asInstanceOf[RankingAdapterModel]
        .recommendForAllUsers(k)
    }

    implicit def tvsm2RTVSM(trainValidationSplitModel: TrainValidationSplitModel): RankingTrainValidationSplitModel =
      new RankingTrainValidationSplitModel(trainValidationSplitModel)

    val items = model.recommendForAllUsers(3)
    print(items)
  }

  //todo: dedup with SparkTVS test
  test("testALSSparkCV") {
    import scala.language.implicitConversions
    implicit def df2RDF(rankingDataFrame: Dataset[_]): RankingDataFrame =
      new RankingDataFrame("rdf", rankingDataFrame)

    implicit def tvsm2RTVSM(crossValidatorSplitModel: CrossValidatorModel): RankingCrossValidatorModel =
      new RankingCrossValidatorModel(crossValidatorSplitModel)

    val rankingAdapter = new RankingAdapter()
      .setMode("allUsers") //allItems does not work, not sure if it would be used
      .setK(evaluator.getK)
      .setRecommender(als)
      .setUserCol(als.getUserCol)
      .setItemCol(als.getItemCol)
      .setRatingCol(als.getRatingCol)

    val crossValidator = (new CrossValidator() with HasRecommenderCols)
      .setEstimator(rankingAdapter)
      .setEstimatorParamMaps(paramGrid)
      .setEvaluator(evaluator)
      .setUserCol(als.getUserCol)
      .setItemCol(als.getItemCol)
      .setRatingCol(als.getRatingCol)

    val df = pipeline.fit(ratings).transform(ratings)

    df.setUserCol(als.getUserCol)
      .setItemCol(als.getItemCol)
      .setRatingCol(als.getRatingCol)

    val model = crossValidator.fit(df)

    val items = model.recommendForAllUsers(3)
    print(items)
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
    val df = transformedDf
    List(new TestObject(rankingTrainValidationSplit.fit(df), df))
  }

  override def reader: MLReadable[_] = RankingTrainValidationSplitModel
}

trait RankingTestBase extends TestBase {
  lazy val userCol = "customerIDOrg"
  lazy val itemCol = "itemIDOrg"
  lazy val ratingCol = "rating"

  lazy val userColIndex = "customerID"
  lazy val itemColIndex = "itemID"

  lazy val ratings: DataFrame = session
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
    .dropDuplicates()

  lazy val customerIndex: StringIndexer = new StringIndexer()
    .setInputCol(userCol)
    .setOutputCol(userColIndex)

  lazy val itemIndex: StringIndexer = new StringIndexer()
    .setInputCol(itemCol)
    .setOutputCol(itemColIndex)

  lazy val pipeline: Pipeline = new Pipeline()
    .setStages(Array(customerIndex, itemIndex))

  val als = new ALS()
  als.setUserCol(customerIndex.getOutputCol)
    .setItemCol(itemIndex.getOutputCol)
    .setRatingCol(ratingCol)

  lazy val paramGrid: Array[ParamMap] = new ParamGridBuilder()
    .addGrid(als.regParam, Array(1.0))
    .build()

  lazy val evaluator: RankingEvaluator = new RankingEvaluator()
    .setK(3)
    .setNItems(10)

  lazy val rankingTrainValidationSplit: RankingTrainValidationSplit = new RankingTrainValidationSplit()
    .setEstimator(als)
    .setEstimatorParamMaps(paramGrid)
    .setEvaluator(evaluator)
    .setTrainRatio(0.8)
    .setCollectSubMetrics(true)

  lazy val transformedDf: DataFrame = pipeline.fit(ratings).transform(ratings)

  lazy val adapter: RankingAdapter = new RankingAdapter()
    .setMode("allUsers") //allItems does not work, not sure if it would be used
    .setK(evaluator.getK)
    .setRecommender(als)
    .setUserCol(customerIndex.getOutputCol)
    .setItemCol(itemIndex.getOutputCol)
    .setRatingCol(ratingCol)

}
