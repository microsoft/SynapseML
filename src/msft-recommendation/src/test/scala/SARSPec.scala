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

import scala.collection.mutable.ArrayBuffer
import scala.collection.{immutable, mutable}
import scala.language.existentials

class SARSPec extends TestBase with EstimatorFuzzing[SAR] {
  val conf: SparkConf = new SparkConf()
    //      .setAppName("Testing Custom Model")
    .setMaster("local[*]")
    .set("spark.driver.memory", "60g")
    .set("spark.driver.maxResultSize", "0")

  override lazy val session = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  override lazy val sc: SparkContext = session.sparkContext

  test("testSAR") {
    val df: DataFrame = session.createDataFrame(Seq(
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
      ("44", "Movie 10", 3, 3))).toDF("customerIDOrg", "itemIDOrg", "rating", "timeOld")

    evalTest(df, "customerIDOrg", "itemIDOrg", "rating")
    //    evalTest2(df, "customerIDOrg", "itemIDOrg", "rating")

    //    val converted = converter.transform(items).select("customerIDrestored", "item_1", "item_2", "item_3")

    //    converted.take(4).foreach(println(_))
  }

  test("testSAR_movie_lens_tiny_eval") {

    //    val customerIndex = new StringIndexer()
    //      .setInputCol("originalCustomerID")
    //      .setOutputCol("customerID")
    //
    //    val ratingsIndex = new StringIndexer()
    //      .setInputCol("newCategoryID")
    //      .setOutputCol("itemID")
    //
    //    val pipeline = new Pipeline()
    //      .setStages(Array(customerIndex, ratingsIndex))
    //
    //    val model1: PipelineModel = pipeline.fit(df)
    //
    //    val transformedDf = model1.transform(df)
    //
    //    val sar = new SAR()
    //      .setUserCol(customerIndex.getOutputCol)
    //      .setItemCol(ratingsIndex.getOutputCol)
    //      .setRatingCol("rating_total")
    //
    //    val tvModel = sar.fit(transformedDf)
    //
    //    val model = tvModel.asInstanceOf[SARModel]
    //
    //    val k = 3
    //    var items = model.recommendForAllUsers(k)
    //
    //    val labelsbc = session.sparkContext.broadcast(model1.stages(1).asInstanceOf[StringIndexerModel].labels)
    //    val labelItem = udf((itemId: Double) => labelsbc.value(itemId.toInt))
    //    (0 until k).map(i => items = items.withColumn("item_" + (i + 1), labelItem(col("recommended")(i))))
    //
    //    val converter = new IndexToString()
    //      .setInputCol("customerID")
    //      .setOutputCol("customerIDrestored")
    //
    //
    //    val converted = converter.transform(items).select("customerIDrestored", "item_1", "item_2", "item_3")
    //
    //    converted.take(4).foreach(println(_))
    //
    //    val (transformedDf: DataFrame,
    //    alsWReg: MsftRecommendation,
    //    evaluator: MsftRecommendationEvaluator,
    //    helper: TrainValidRecommendSplit) = {
    //      (transformedDf, alsWReg, evaluator, helper)
    //    }
    {
      val ratings: DataFrame = session.read
        .option("header", "true") //reading the headers
        .option("inferSchema", "true") //reading the headers
        .csv("/mnt/ml-latest-small/ratings.csv").na.drop

      evalTest(ratings, "userId", "movieId", "rating")
    }
    //3.53
  }

  ignore("testSAR_movie_lens_all_eval") {
    //Map(map -> 0.0, maxDiversity -> 0.7083964327780582, diversityAtK -> 0.005216220763923944, recallAtK -> 0.0,
    // ndcgAt -> 0.0, precisionAtk -> 0.0)

    //    val customerIndex = new StringIndexer()
    //      .setInputCol("originalCustomerID")
    //      .setOutputCol("customerID")
    //
    //    val ratingsIndex = new StringIndexer()
    //      .setInputCol("newCategoryID")
    //      .setOutputCol("itemID")
    //
    //    val pipeline = new Pipeline()
    //      .setStages(Array(customerIndex, ratingsIndex))
    //
    //    val model1: PipelineModel = pipeline.fit(df)
    //
    //    val transformedDf = model1.transform(df)
    //
    //    val sar = new SAR()
    //      .setUserCol(customerIndex.getOutputCol)
    //      .setItemCol(ratingsIndex.getOutputCol)
    //      .setRatingCol("rating_total")
    //
    //    val tvModel = sar.fit(transformedDf)
    //
    //    val model = tvModel.asInstanceOf[SARModel]
    //
    //    val k = 3
    //    var items = model.recommendForAllUsers(k)
    //
    //    val labelsbc = session.sparkContext.broadcast(model1.stages(1).asInstanceOf[StringIndexerModel].labels)
    //    val labelItem = udf((itemId: Double) => labelsbc.value(itemId.toInt))
    //    (0 until k).map(i => items = items.withColumn("item_" + (i + 1), labelItem(col("recommended")(i))))
    //
    //    val converter = new IndexToString()
    //      .setInputCol("customerID")
    //      .setOutputCol("customerIDrestored")
    //
    //
    //    val converted = converter.transform(items).select("customerIDrestored", "item_1", "item_2", "item_3")
    //
    //    converted.take(4).foreach(println(_))
    //
    //    val (transformedDf: DataFrame,
    //    alsWReg: MsftRecommendation,
    //    evaluator: MsftRecommendationEvaluator,
    //    helper: TrainValidRecommendSplit) = {
    //      (transformedDf, alsWReg, evaluator, helper)
    //    }

    val ratings: DataFrame = session.read
      .option("header", "true") //reading the headers
      .option("inferSchema", "true") //reading the headers
      .csv("/mnt/ml-20m/parts/part*.csv").na.drop

    evalTest(ratings, "userId", "movieId", "rating")
  }

  private def evalTest(ratings: DataFrame, customerId: String, itemId: String, rating: String) = {
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

    val alsWReg = new MsftRecommendation()
      .setUserCol(customerIndex.getOutputCol)
      .setRatingCol("rating")
      .setItemCol(ratingsIndex.getOutputCol)

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
    val metrics = "ndcgAt|map|precisionAtk|recallAtK|diversityAtK|maxDiversity"

    assert(evaluator.getMetricsList(0).getOrElse("map", 0.0) > 0)
    assert(evaluator.getMetricsList(0).getOrElse("ndcgAt", 0.0) > 0)
    assert(evaluator.getMetricsList(0).getOrElse("precisionAtk", 0.0) > 0)
    assert(evaluator.getMetricsList(0).getOrElse("recallAtK", 0.0) > 0)
    assert(evaluator.getMetricsList(0).getOrElse("diversityAtK", 0.0) > 0)
    assert(evaluator.getMetricsList(0).getOrElse("maxDiversity", 0.0) > 0)
    assert(evaluator.getMetricsList(0).getOrElse("map", 0.0) <= 1.0)
    assert(evaluator.getMetricsList(0).getOrElse("ndcgAt", 0.0) <= 1.0)
    assert(evaluator.getMetricsList(0).getOrElse("precisionAtk", 0.0) <= 1.0)
    assert(evaluator.getMetricsList(0).getOrElse("recallAtK", 0.0) <= 1.0)
    assert(evaluator.getMetricsList(0).getOrElse("diversityAtK", 0.0) <= 1.0)
    assert(evaluator.getMetricsList(0).getOrElse("maxDiversity", 0.0) <= 1.0)

  }

  test("testItemFactorize") {
    val df: DataFrame = session
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
      .toDF("customerIDOrg", "itemIDOrg", "rating")
    val customerIndex = new StringIndexer()
      .setInputCol("customerIDOrg")
      .setOutputCol("customerID")

    val ratingsIndex = new StringIndexer()
      .setInputCol("itemIDOrg")
      .setOutputCol("itemID")

    val pipeline = new Pipeline()
      .setStages(Array(customerIndex, ratingsIndex))

    val transformedDf = pipeline.fit(df).transform(df)

    val sar = new SAR()
      .setUserCol(customerIndex.getOutputCol)
      .setItemCol(ratingsIndex.getOutputCol)
      .setRatingCol("rating")

    type MapType = Map[Double, Map[Double, Double]]

    val itemMatrixDF = sar.itemFeatures(transformedDf)

    val lists = itemMatrixDF.collect()
    //    val itemMap: MapType = itemMatrix.take(1)(0)(0).asInstanceOf[Map[Double, Map[Double, Double]]]
    //
    //    assert(itemMap.get(0.0).get(0.0) == 32.0)
    //    assert(itemMap.get(0.0).get(5.0) == 21.0)
    //    assert(itemMap.get(0.0).get(1.0) == 25.0)
    //    print(itemMap.prettyPrint)
  }

  implicit class PrettyPrintMap[K, V](val map: Map[K, V]) {
    def prettyPrint: PrettyPrintMap[K, V] = this

    override def toString: String = {
      val valuesString = toStringLines.mkString("\n")

      "Map (\n" + valuesString + "\n)"
    }

    def toStringLines: immutable.Iterable[String] = {
      map
        .flatMap { case (k, v) => keyValueToString(k, v) }
        .map(indentLine(_))
    }

    def keyValueToString(key: K, value: V): Iterable[String] = {
      value match {
        case v: Map[_, _] => Iterable(key + " -> Map (") ++ v.prettyPrint.toStringLines ++ Iterable(")")
        case x => Iterable(key + " -> " + x.toString)
      }
    }

    def indentLine(line: String): String = {
      "\t" + line
    }
  }

  val recommender = new SAR()

  test("exact rank-1 matrix") {
    val (training, test) = genExplicitTestData(numUsers = 20, numItems = 40, rank = 1)

    testRecommender(training, test, rank = 1, regParam = 1e-5, targetRMSE = 0.25, recommender =
      recommender)
    testRecommender(training, test, rank = 2, regParam = 1e-5, targetRMSE = 0.25, recommender =
      recommender)
  }

  test("approximate rank-1 matrix") {
    val (training, test) =
      genExplicitTestData(numUsers = 20, numItems = 40, rank = 1, noiseStd = 0.01)
    testRecommender(training, test, rank = 1, regParam = 0.01, targetRMSE = 0.25, recommender =
      recommender)
    testRecommender(training, test, rank = 2, regParam = 0.01, targetRMSE = 0.25, recommender =
      recommender)
  }

  test("approximate rank-2 matrix") {
    val (training, test) =
      genExplicitTestData(numUsers = 20, numItems = 40, rank = 2, noiseStd = 0.01)
    testRecommender(training, test, rank = 2, regParam = 0.01, targetRMSE = 0.45, recommender =
      recommender)
    testRecommender(training, test, rank = 3, regParam = 0.01, targetRMSE = 0.45, recommender =
      recommender)
  }

  test("different block settings") {
    val (training, test) =
      genExplicitTestData(numUsers = 20, numItems = 40, rank = 2, noiseStd = 0.01)
    for ((numUserBlocks, numItemBlocks) <- Seq((1, 1), (1, 2), (2, 1), (2, 2))) {
      testRecommender(training, test, rank = 3, regParam = 0.01, targetRMSE = 0.45,
        numUserBlocks = numUserBlocks, numItemBlocks = numItemBlocks, recommender = recommender)
    }
  }

  test("more blocks than ratings") {
    val (training, test) =
      genExplicitTestData(numUsers = 4, numItems = 4, rank = 1)
    testRecommender(training, test, rank = 1, regParam = 1e-4, targetRMSE = 0.25,
      numItemBlocks = 5, numUserBlocks = 5, recommender = recommender)
  }

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
    val als = recommender
      .setRank(rank)
      .setUserCol("user")
      .setItemCol("item")
      .setRatingCol("rating")

    val alpha = als.getAlpha
    val model = als.fit(training.toDF())
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

  override def testObjects(): List[TestObject[SAR]] = {
    val df: DataFrame = session.createDataFrame(Seq(
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
      ("44", "Movie 10", 3, 3))).toDF("customerIDOrg", "itemIDOrg", "rating", "timeOld")

    evalTest(df, "customerIDOrg", "itemIDOrg", "rating")

    val customerIndex = new StringIndexer()
      .setInputCol("customerIDOrg")
      .setOutputCol("customerID")

    val ratingsIndex = new StringIndexer()
      .setInputCol("itemIDOrg")
      .setOutputCol("itemID")

    val pipeline = new Pipeline()
      .setStages(Array(customerIndex, ratingsIndex))

    val transformedDf = pipeline.fit(df).transform(df)

    List(
      new TestObject(new SAR()
        .setUserCol(customerIndex.getOutputCol)
        .setItemCol(ratingsIndex.getOutputCol)
        .setRatingCol("rating"), transformedDf)
    )
  }

  override def reader: SAR.type = SAR

  override def modelReader: SARModel.type = SARModel
}
