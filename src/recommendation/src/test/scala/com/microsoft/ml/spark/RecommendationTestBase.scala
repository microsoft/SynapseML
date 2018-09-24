// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.util.Random

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.linalg.{Vectors => mlVectors}
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait RecommendationTestBase extends TestBase {
  private[spark] val conf: SparkConf = new SparkConf()
    .setAppName("Testing Recommendation Model")
    .setMaster("local[*]")
    .set("spark.driver.memory", "60g")
    .set("spark.driver.maxResultSize", "0")

  override lazy val session: SparkSession = SparkSession.builder()
    .config(conf)
    .getOrCreate()
  session.sparkContext.setLogLevel("WARN")

  private[spark] val sc2 = session.sqlContext

  override lazy val sc: SparkContext = session.sparkContext

  private[spark] lazy val df = {
    session.createDataFrame(Seq(
      ("11", "Movie 01", 4, 4),
      ("11", "Movie 03", 1, 1),
      ("11", "Movie 04", 5, 5),
      ("11", "Movie 05", 3, 3),
      ("11", "Movie 07", 3, 3),
      ("11", "Movie 09", 3, 3),
      ("11", "Movie 10", 3, 3),
      ("22", "Movie 01", 4, 4),
      ("22", "Movie 02", 5, 5),
      ("22", "Movie 03", 1, 1),
      ("22", "Movie 06", 4, 4),
      ("22", "Movie 07", 5, 5),
      ("22", "Movie 08", 1, 1),
      ("22", "Movie 10", 3, 3),
      ("33", "Movie 01", 4, 4),
      ("33", "Movie 02", 1, 1),
      ("33", "Movie 03", 1, 1),
      ("33", "Movie 04", 5, 5),
      ("33", "Movie 05", 3, 3),
      ("33", "Movie 06", 4, 4),
      ("33", "Movie 08", 1, 1),
      ("33", "Movie 09", 5, 5),
      ("33", "Movie 10", 3, 3),
      ("44", "Movie 02", 5, 5),
      ("44", "Movie 03", 1, 1),
      ("44", "Movie 04", 5, 5),
      ("44", "Movie 05", 3, 3),
      ("44", "Movie 06", 4, 4),
      ("44", "Movie 07", 5, 5),
      ("44", "Movie 08", 1, 1),
      ("44", "Movie 09", 5, 5),
      ("44", "Movie 10", 3, 3))).toDF("userId", "movieId", "rating", "timeOld")
  }

  //noinspection AccessorLikeMethodIsEmptyParen
  private[spark] def getColdTestData(): (DataFrame, DataFrame) = {
    val df: DataFrame = session.createDataFrame(Seq(
      ("11", "Movie 09", 3, 3),
      ("11", "Movie 07", 3, 3),
      ("11", "Movie 01", 4, 4),
      ("11", "Movie 03", 1, 1),
      ("11", "Movie 04", 5, 5),
      ("11", "Movie 05", 3, 3),
      ("11", "Movie 10", 3, 3),
      ("22", "Movie 01", 4, 4),
      ("22", "Movie 02", 5, 5),
      ("22", "Movie 03", 1, 1),
      ("22", "Movie 06", 4, 4),
      ("22", "Movie 07", 5, 5),
      ("22", "Movie 08", 1, 1),
      ("22", "Movie 10", 3, 3),
      ("33", "Movie 10", 3, 3),
      ("33", "Movie 09", 5, 5),
      ("33", "Movie 08", 1, 1),
      ("33", "Movie 04", 5, 5),
      ("33", "Movie 05", 3, 3),
      ("33", "Movie 06", 4, 4),
      ("33", "Movie 03", 1, 1),
      ("33", "Movie 02", 1, 1),
      ("33", "Movie 01", 4, 4),
      ("44", "Movie 07", 5, 5),
      ("44", "Movie 10", 3, 3),
      ("44", "Movie 03", 1, 1),
      ("44", "Movie 09", 5, 5),
      ("44", "Movie 08", 1, 1),
      ("44", "Movie 06", 4, 4),
      ("44", "Movie 05", 3, 3),
      ("44", "Movie 04", 5, 5),
      ("44", "Movie 02", 5, 5)
    )).toDF("customerIDOrg", "itemIDOrg", "rating", "timeOld")
    val itemFeaturesDF: DataFrame = session.createDataFrame(Seq(
      ("Movie 01", 1, .25),
      ("Movie 01", 0, .50),
      ("Movie 02", 1, .25),
      ("Movie 02", 0, .50),
      ("Movie 03", 1, .25),
      ("Movie 03", 0, .50),
      ("Movie 04", 1, .25),
      ("Movie 04", 0, .50),
      ("Movie 05", 1, .25),
      ("Movie 05", 0, .50),
      ("Movie 06", 1, .25),
      ("Movie 06", 0, .50),
      ("Movie 07", 1, .25),
      ("Movie 07", 0, .50),
      ("Movie 08", 1, .25),
      ("Movie 08", 0, .50),
      ("Movie 09", 1, .25),
      ("Movie 09", 0, .50),
      ("Movie 10", 1, .25),
      ("Movie 10", 0, .50))).toDF("itemIDOrg", "tagID", "relevance")
    (df, itemFeaturesDF)
  }

  private[spark] lazy val movieLensSmall: DataFrame = session.read
    .option("header", "true") //reading the headers
    .option("inferSchema", "true") //reading the headers
    .csv("/mnt/ml-latest-small/ratings.csv").na.drop

  private[spark] lazy val movieLensLarge: DataFrame = session.read
    .option("header", "true") //reading the headers
    .option("inferSchema", "true") //reading the headers
    .csv("/mnt/ml-20m/ratings.csv").na.drop

  private[spark] def test_eval_metrics_cold_items(ratings: DataFrame, itemRatings: DataFrame, customerId: String =
  "userId", itemId: String = "movieId", rating: String = "rating") = {
    session.sparkContext.setLogLevel("WARN")

    val k = 10

    ratings.cache()
    val customerIndex = new StringIndexer()
      .setInputCol(customerId)
      .setOutputCol("customerID")

    val ratingsIndex = new StringIndexer()
      .setInputCol(itemId)
      .setOutputCol("itemID")

    val pipeline = new Pipeline()
      .setStages(Array(customerIndex, ratingsIndex))

    val hashModel = pipeline.fit(ratings)
    val transformedDf = hashModel.transform(ratings)
    val itemTransformedDF = hashModel.transform(itemRatings)
    transformedDf.cache().count
    //    ratings.unpersist()
    val sar = new SAR()
      .setUserCol(customerIndex.getOutputCol)
      .setItemCol(ratingsIndex.getOutputCol)
      .setRatingCol("rating")
      .setItemFeatures(itemTransformedDF)
      .setSupportThreshold(2)

    val paramGrid = new ParamGridBuilder()
      .build()

    val evaluator = new MsftRecommendationEvaluator()
      .setK(k)
      .setSaveAll(true)

    val helper = new TrainValidRecommendSplit()
      .setEstimator(sar)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)
      .setUserCol(customerIndex.getOutputCol)
      .setRatingCol("rating")
      .setItemCol(ratingsIndex.getOutputCol)
      .setMinRatingsI(0)
      .setMinRatingsU(0)

    val filtered = helper.filterRatings(transformedDf)
    filtered.cache().count()
    //    transformedDf.unpersist()
    val Array(train, test) = helper.splitDF(filtered)
    train.cache().count
    test.cache().count
    //    filtered.unpersist()

    val model = sar.fit(train)

    val users = model.recommendForAllUsers(k)
    users.cache.count
    //    train.unpersist

    val testData = helper.prepareTestData(test, users, k)
    testData.cache.count
    //    test.unpersist
    //    users.unpersist

    evaluator.setNItems(transformedDf.rdd.map(r => r(5)).distinct().count())
    evaluator.evaluate(testData)
    evaluator.printMetrics()

    assert(evaluator.getMetricsList.head.getOrElse("map", 0.0) > 0)
    assert(evaluator.getMetricsList.head.getOrElse("ndcgAt", 0.0) > 0)
    assert(evaluator.getMetricsList.head.getOrElse("precisionAtk", 0.0) > 0)
    assert(evaluator.getMetricsList.head.getOrElse("recallAtK", 0.0) > 0)
    assert(evaluator.getMetricsList.head.getOrElse("diversityAtK", 0.0) > 0)
    assert(evaluator.getMetricsList.head.getOrElse("maxDiversity", 0.0) > 0)
    assert(evaluator.getMetricsList.head.getOrElse("map", 0.0) <= 1.0)
    assert(evaluator.getMetricsList.head.getOrElse("ndcgAt", 0.0) <= 1.0)
    assert(evaluator.getMetricsList.head.getOrElse("precisionAtk", 0.0) <= 1.0)
    assert(evaluator.getMetricsList.head.getOrElse("recallAtK", 0.0) <= 1.0)
    assert(evaluator.getMetricsList.head.getOrElse("diversityAtK", 0.0) <= 1.0)
    assert(evaluator.getMetricsList.head.getOrElse("maxDiversity", 0.0) <= 1.0)

  }

  private[spark] def test_recommendations(ratings: DataFrame, customerId: String = "userId", itemId: String = "movieId",
                                          rating: String = "rating", similarityFunction: String = "jacccard"): Unit = {
    session.sparkContext.setLogLevel("WARN")

    ratings.cache()
    val customerIndex = new StringIndexer()
      .setInputCol(customerId)
      .setOutputCol("customerID")

    val ratingsIndex = new StringIndexer()
      .setInputCol(itemId)
      .setOutputCol("itemID")

    val pipeline = new Pipeline()
      .setStages(Array(customerIndex, ratingsIndex))

    val transformedDf = pipeline.fit(ratings).transform(ratings)
    transformedDf.cache().count

    //    ratings.unpersist()
    val sar = new SAR()
      .setUserCol(customerIndex.getOutputCol)
      .setItemCol(ratingsIndex.getOutputCol)
      .setRatingCol("rating")
      .setSimilarityFunction(similarityFunction)
      .setSupportThreshold(2)
      .setActivityTimeFormat("EEE MMM dd HH:mm:ss Z yyyy")

    val model: SARModel = sar.fit(transformedDf)
    val recs = model.recommendForAllUsers(10)
    println(recs.count)
  }

  private[spark] def test_recommendations_without_indexer(ratings: DataFrame, customerId: String = "userId",
                                                          itemId: String = "movieId", rating: String = "rating",
                                                          similarityFunction: String = "jacccard"): Unit = {
    session.sparkContext.setLogLevel("WARN")

    ratings.cache()

    val sar = new SAR()
      .setUserCol(customerId)
      .setItemCol(itemId)
      .setRatingCol("rating")
      .setSimilarityFunction(similarityFunction)
      .setSupportThreshold(2)
      .setActivityTimeFormat("EEE MMM dd HH:mm:ss Z yyyy")
      .setAutoIndex(true)

    val model: SARModel = sar.fit(ratings)
    val recs = model.recommendForAllUsers(10)
    assert(recs.count > 0)
    println(recs.take(5)(0))
    println(recs.take(5)(0).getString(0))
  }

  private[spark] def test_eval_metrics_no_ratings(ratings: DataFrame, customerId: String = "userId",
                                                  itemId: String = "movieId", rating: String = "rating",
                                                  similarityFunction: String = "jacccard", seed: Boolean = true) = {
    session.sparkContext.setLogLevel("WARN")

    val k = 10

    ratings.cache()
    val customerIndex = new StringIndexer()
      .setInputCol(customerId)
      .setOutputCol("customerID")

    val ratingsIndex = new StringIndexer()
      .setInputCol(itemId)
      .setOutputCol("itemID")

    val pipeline = new Pipeline()
      .setStages(Array(customerIndex, ratingsIndex))

    val transformedDf = pipeline.fit(ratings).transform(ratings)
    transformedDf.cache().count

    //    ratings.unpersist()
    val sar = new SAR()
      .setUserCol(customerIndex.getOutputCol)
      .setItemCol(ratingsIndex.getOutputCol)
      .setSimilarityFunction(similarityFunction)
      .setSupportThreshold(2)
      .setActivityTimeFormat("EEE MMM dd HH:mm:ss Z yyyy")
      .setAllowSeedItemsInRecommendations(seed)

    val paramGrid = new ParamGridBuilder()
      .build()

    val evaluator = new MsftRecommendationEvaluator()
      .setK(k)
      .setSaveAll(true)

    val helper = new TrainValidRecommendSplit()
      .setEstimator(sar)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)
      .setUserCol(customerIndex.getOutputCol)
      .setItemCol(ratingsIndex.getOutputCol)

    val filtered = helper.filterRatings(transformedDf)
    filtered.cache().count()
    //    transformedDf.unpersist()
    val Array(train, test) = helper.splitDF(filtered)
    train.cache().count
    test.cache().count
    //    filtered.unpersist()

    val model = sar.fit(train)

    val users = model.recommendForAllUsers(k)
    assert(users.count > 0)
  }

  private[spark] def test_train(ratings: DataFrame, customerId: String = "userId", itemId: String = "movieId",
                                rating: String = "rating", similarityFunction: String = "jacccard",
                                seed: Boolean = true) = {
    session.sparkContext.setLogLevel("WARN")

    val k = 10

    val sar = new SAR()
      .setUserCol(customerId)
      .setItemCol(itemId)
      .setRatingCol("rating")
      .setSimilarityFunction(similarityFunction)
      .setSupportThreshold(2)
      .setActivityTimeFormat("EEE MMM dd HH:mm:ss Z yyyy")
      .setAllowSeedItemsInRecommendations(seed)

    sar.fit(ratings)
  }

  private[spark] def test_eval_metrics(ratings: DataFrame, customerId: String = "userId", itemId: String = "movieId",
                                       rating: String = "rating", similarityFunction: String = "jacccard",
                                       seed: Boolean = true) = {
    session.sparkContext.setLogLevel("WARN")

    val k = 10

    ratings.cache()
    val customerIndex = new StringIndexer()
      .setInputCol(customerId)
      .setOutputCol("customerID")

    val ratingsIndex = new StringIndexer()
      .setInputCol(itemId)
      .setOutputCol("itemID")

    val pipeline = new Pipeline()
      .setStages(Array(customerIndex, ratingsIndex))

    val transformedDf = pipeline.fit(ratings).transform(ratings)
    transformedDf.cache().count

    //    ratings.unpersist()
    val sar = new SAR()
      .setUserCol(customerIndex.getOutputCol)
      .setItemCol(ratingsIndex.getOutputCol)
      .setRatingCol("rating")
      .setSimilarityFunction(similarityFunction)
      .setSupportThreshold(2)
      .setActivityTimeFormat("EEE MMM dd HH:mm:ss Z yyyy")
      .setAllowSeedItemsInRecommendations(seed)
      .setAutoIndex(false)

    val paramGrid = new ParamGridBuilder()
      .build()

    val evaluator = new MsftRecommendationEvaluator()
      .setK(k)
      .setSaveAll(true)

    val helper = new TrainValidRecommendSplit()
      .setEstimator(sar)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)
      .setUserCol(customerIndex.getOutputCol)
      .setRatingCol("rating")
      .setItemCol(ratingsIndex.getOutputCol)

    val filtered = helper.filterRatings(transformedDf)
    filtered.cache().count()
    //    transformedDf.unpersist()
    val Array(train, test) = helper.splitDF(filtered)
    train.cache().count
    test.cache().count
    //    filtered.unpersist()

    val model = sar.fit(train)

    val users = model.recommendForAllUsers(k)
    users.cache.count
    //    train.unpersist

    val testData = helper.prepareTestData(test, users, k)
    testData.cache.count
    //    test.unpersist
    //    users.unpersist

    evaluator.setNItems(transformedDf.rdd.map(r => r(5)).distinct().count())
    evaluator.evaluate(testData)
    evaluator.printMetrics()

    assert(evaluator.getMetricsList.head.getOrElse("map", 0.0) > 0)
    assert(evaluator.getMetricsList.head.getOrElse("ndcgAt", 0.0) > 0)
    assert(evaluator.getMetricsList.head.getOrElse("precisionAtk", 0.0) > 0)
    assert(evaluator.getMetricsList.head.getOrElse("recallAtK", 0.0) > 0)
    assert(evaluator.getMetricsList.head.getOrElse("diversityAtK", 0.0) > 0)
    assert(evaluator.getMetricsList.head.getOrElse("maxDiversity", 0.0) > 0)
    assert(evaluator.getMetricsList.head.getOrElse("map", 0.0) <= 1.0)
    assert(evaluator.getMetricsList.head.getOrElse("ndcgAt", 0.0) <= 1.0)
    assert(evaluator.getMetricsList.head.getOrElse("precisionAtk", 0.0) <= 1.0)
    assert(evaluator.getMetricsList.head.getOrElse("recallAtK", 0.0) <= 1.0)
    assert(evaluator.getMetricsList.head.getOrElse("diversityAtK", 0.0) <= 1.0)
    assert(evaluator.getMetricsList.head.getOrElse("maxDiversity", 0.0) <= 1.0)

  }

  private[spark] def test_eval_metrics_without_indexer(ratings: DataFrame, customerId: String = "userId",
                                                       itemId: String = "movieId",
                                                       rating: String = "rating", similarityFunction: String =
                                                       "jacccard",
                                                       seed: Boolean = true) = {
    session.sparkContext.setLogLevel("WARN")

    val k = 10

    ratings.cache()

    val sar = new SAR()
      .setUserCol(customerId)
      .setItemCol(itemId)
      .setRatingCol("rating")
      .setSimilarityFunction(similarityFunction)
      .setSupportThreshold(2)
      .setActivityTimeFormat("EEE MMM dd HH:mm:ss Z yyyy")
      .setAllowSeedItemsInRecommendations(seed)

    val paramGrid = new ParamGridBuilder()
      .build()

    val evaluator = new MsftRecommendationEvaluator()
      .setK(k)
      .setSaveAll(true)

    val helper = new TrainValidRecommendSplit()
      .setEstimator(sar)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)
      .setUserCol(customerId)
      .setRatingCol("rating")
      .setItemCol(itemId)

    val filtered = helper.filterRatings(ratings)
    filtered.cache().count()
    //    transformedDf.unpersist()
    val Array(train, test) = helper.splitDF(filtered)
    train.cache().count
    test.cache().count
    //    filtered.unpersist()

    val model = sar.fit(train)

    val users = model.recommendForAllUsers(k)
    users.cache.count
    //    train.unpersist

    val testData = helper.prepareTestData(test, users, k)
    testData.cache.count
    //    test.unpersist
    //    users.unpersist

    evaluator.setNItems(ratings.rdd.map(r => r(5)).distinct().count())
    evaluator.evaluate(testData)
    evaluator.printMetrics()

    assert(evaluator.getMetricsList.head.getOrElse("map", 0.0) > 0)
    assert(evaluator.getMetricsList.head.getOrElse("ndcgAt", 0.0) > 0)
    assert(evaluator.getMetricsList.head.getOrElse("precisionAtk", 0.0) > 0)
    assert(evaluator.getMetricsList.head.getOrElse("recallAtK", 0.0) > 0)
    assert(evaluator.getMetricsList.head.getOrElse("diversityAtK", 0.0) > 0)
    assert(evaluator.getMetricsList.head.getOrElse("maxDiversity", 0.0) > 0)
    assert(evaluator.getMetricsList.head.getOrElse("map", 0.0) <= 1.0)
    assert(evaluator.getMetricsList.head.getOrElse("ndcgAt", 0.0) <= 1.0)
    assert(evaluator.getMetricsList.head.getOrElse("precisionAtk", 0.0) <= 1.0)
    assert(evaluator.getMetricsList.head.getOrElse("recallAtK", 0.0) <= 1.0)
    assert(evaluator.getMetricsList.head.getOrElse("diversityAtK", 0.0) <= 1.0)
    assert(evaluator.getMetricsList.head.getOrElse("maxDiversity", 0.0) <= 1.0)

  }

  private[spark] def test_hyper_param_sweep(ratings: DataFrame, customerId: String = "userId",
                                            itemId: String = "movieId", rating: String = "rating") = {
    session.sparkContext.setLogLevel("WARN")

    val k = 5

    ratings.cache()
    val customerIndex = new StringIndexer()
      .setInputCol(customerId)
      .setOutputCol("customerID")

    val ratingsIndex = new StringIndexer()
      .setInputCol(itemId)
      .setOutputCol("itemID")

    val pipeline = new Pipeline()
      .setStages(Array(customerIndex, ratingsIndex))

    val transformedDf = pipeline.fit(ratings).transform(ratings)
    transformedDf.cache().count

    val sar = new SAR()
      .setUserCol(customerIndex.getOutputCol)
      .setItemCol(ratingsIndex.getOutputCol)
      .setRatingCol("rating")

    val paramGrid = new ParamGridBuilder()
      .addGrid(sar.supportThreshold, Array(5, 8, 13, 24, 37))
      .build()

    val evaluator = new MsftRecommendationEvaluator()
      .setK(k)
      .setSaveAll(true)
      .setMetricName("map")

    val helper = new TrainValidRecommendSplit()
      .setEstimator(sar)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)
      .setUserCol(customerIndex.getOutputCol)
      .setRatingCol("rating")
      .setItemCol(ratingsIndex.getOutputCol)

    val tvModel = helper.fit(transformedDf)

    val model = tvModel.getBestModel.asInstanceOf[SARModel]

    val users = model.recommendForAllUsers(k)
    users.cache.count
    evaluator.printMetrics()

    assert(evaluator.getMetricsList.head.getOrElse("map", 0.0) > 0)
    assert(evaluator.getMetricsList.head.getOrElse("ndcgAt", 0.0) > 0)
    assert(evaluator.getMetricsList.head.getOrElse("precisionAtk", 0.0) > 0)
    assert(evaluator.getMetricsList.head.getOrElse("recallAtK", 0.0) > 0)
    assert(evaluator.getMetricsList.head.getOrElse("diversityAtK", 0.0) > 0)
    assert(evaluator.getMetricsList.head.getOrElse("maxDiversity", 0.0) > 0)
    assert(evaluator.getMetricsList.head.getOrElse("map", 0.0) <= 1.0)
    assert(evaluator.getMetricsList.head.getOrElse("ndcgAt", 0.0) <= 1.0)
    assert(evaluator.getMetricsList.head.getOrElse("precisionAtk", 0.0) <= 1.0)
    assert(evaluator.getMetricsList.head.getOrElse("recallAtK", 0.0) <= 1.0)
    assert(evaluator.getMetricsList.head.getOrElse("diversityAtK", 0.0) <= 1.0)
    assert(evaluator.getMetricsList.head.getOrElse("maxDiversity", 0.0) <= 1.0)

  }

  val recommender = new SAR()

  def testRecommender(
                       training: RDD[Rating[Int]],
                       test: RDD[Rating[Int]],
                       rank: Int,
                       regParam: Double,
                       numUserBlocks: Int = 2,
                       numItemBlocks: Int = 3,
                       targetRMSE: Double = 0.05,
                       recommender: SAR): Unit = {
    val spark = this.session
    import spark.implicits._
    val sar = recommender
      .setUserCol("user")
      .setItemCol("item")
      .setRatingCol("rating")

    val alpha = sar.getAlpha
    val model = sar.fit(training.toDF())
    val predictions = model.transform(test.toDF()).select("rating", "prediction").rdd.map {
      case Row(rating: Float, prediction: Float) =>
        (rating.toDouble, prediction.toDouble)
    }
    val implicitPrefs = false
    val rmse =
      if (implicitPrefs) {
        // TODO: Use a better (rank-based?) evaluation metric for implicit feedback.
        // We limit the ratings and the predictions to interval [0, 1] and compute the weighted RMSE
        // with the confidence scores as weights.
        val (totalWeight, weightedSumSq) = predictions.map { case (rating, prediction) =>
          val confidence = 1.0 + alpha * math.abs(rating)
          val rating01 = math.max(math.min(rating, 1.0), 0.0)
          val prediction01 = math.max(math.min(prediction, 1.0), 0.0)
          val err = prediction01 - rating01
          (confidence, confidence * err * err)
        }.reduce { case ((c0, e0), (c1, e1)) =>
          (c0 + c1, e0 + e1)
        }
        math.sqrt(weightedSumSq / totalWeight)
      } else {
        val mse = predictions.map { case (rating, prediction) =>
          val err = rating - prediction
          err * err
        }.mean()
        math.sqrt(mse)
      }
    assert(rmse < targetRMSE)
    println(rmse)
    ()
  }

  def genExplicitTestData(
                           numUsers: Int,
                           numItems: Int,
                           rank: Int,
                           noiseStd: Double = 0.0,
                           seed: Long = 11L): (RDD[Rating[Int]], RDD[Rating[Int]]) = {
    val trainingFraction = 0.6
    val testFraction = 0.3
    val totalFraction = trainingFraction + testFraction
    val random = new Random(seed)
    val userFactors = genFactors(numUsers, rank, random)
    val itemFactors = genFactors(numItems, rank, random)
    val training = ArrayBuffer.empty[Rating[Int]]
    val test = ArrayBuffer.empty[Rating[Int]]
    for ((userId, userFactor) <- userFactors; (itemId, itemFactor) <- itemFactors) {
      val x = random.nextDouble()
      if (x < totalFraction) {
        val rating = blas.sdot(rank, userFactor, 1, itemFactor, 1)
        if (x < trainingFraction) {
          val noise = noiseStd * random.nextGaussian()
          training += Rating(userId, itemId, rating + noise.toFloat)
        } else {
          test += Rating(userId, itemId, rating)
        }
      }
    }
    (sc.parallelize(training, 2), sc.parallelize(test, 2))
  }

  private def genFactors(
                          size: Int,
                          rank: Int,
                          random: Random,
                          a: Float = -1.0f,
                          b: Float = 1.0f): Seq[(Int, Array[Float])] = {
    require(size > 0 && size < Int.MaxValue / 3)
    require(b > a)
    val ids = mutable.Set.empty[Int]
    while (ids.size < size) {
      ids += random.nextInt(1000)
    }
    val width = b - a
    ids.toSeq.sorted.map(id => (id, Array.fill(rank)(a + random.nextFloat() * width)))
  }
}

