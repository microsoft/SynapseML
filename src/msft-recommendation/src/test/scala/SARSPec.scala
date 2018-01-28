// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.spark.ml.recommendations

import com.microsoft.ml.spark.{MsftRecommendationEvaluator, TrainValidRecommendSplit}
import com.microsoft.spark.ml.recommendations.SAR.processRow
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, StringIndexerModel}
import org.apache.spark.ml.linalg.{Vectors => mlVectors}
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSuite

import scala.collection.mutable

class SARSPec extends FunSuite {
  val conf = new SparkConf()
    //      .setAppName("Testing Custom Model")
    .setMaster("local[*]")
    .set("spark.driver.memory", "60g")
    .set("spark.driver.maxResultSize", "0")

  private val session = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  test("testSARMatrix") {
    val df: DataFrame = session
      .createDataFrame(Seq(
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
        ("44", "Movie 10", 3, 3)))
      .toDF("customerIDOrg", "itemIDOrg", "rating", "time")
    val customerIndex = new StringIndexer()
      .setInputCol("customerIDOrg")
      .setOutputCol("customerID")

    val ratingsIndex = new StringIndexer()
      .setInputCol("itemIDOrg")
      .setOutputCol("itemID")

    val pipeline = new Pipeline()
      .setStages(Array(customerIndex, ratingsIndex))

    val model1: PipelineModel = pipeline.fit(df)

    val transformedDf = model1.transform(df)
    import org.apache.spark.sql.functions._
    val rdd = transformedDf
      .select(col(customerIndex.getOutputCol).cast(LongType)).rdd
      .map(r => MatrixEntry(r.getLong(0), r.getLong(0), 1.0))
    val matrix = new CoordinateMatrix(rdd).toBlockMatrix().cache()
    val ata = matrix.transpose.multiply(matrix)

    print(ata.toString)
    print("Done")
  }

  test("testSARMatrix_tiny") {
    testNewMatrixMutiply(tinydf)
  }

  test("testSARMatrix_tiny_old") {
    oldMatrixMadeByMap(tinydf)
  }

  test("testSARMatrix_big") {
    testNewMatrixMutiply(bigdf)
  }

  test("testSARMatrix_big_old") {
    oldMatrixMadeByMap(bigdf)
  }

  test("testSARMatrix_all") {
    testNewMatrixMutiply(alldf)
  }

  test("testSARMatrix_all_old") {
    oldMatrixMadeByMap(alldf)
  }

  private lazy val alldf: DataFrame = session.read
    .option("header", "true") //reading the headers
    .option("inferSchema", "true") //reading the headers
    .csv("/mnt/rating.csv").na.drop

  private lazy val bigdf: DataFrame = session.read
    .option("header", "true") //reading the headers
    .option("inferSchema", "true") //reading the headers
    .csv("/mnt/rating_big.csv").na.drop

  private lazy val tinydf: DataFrame = session.read
    .option("header", "true") //reading the headers
    .option("inferSchema", "true") //reading the headers
    .csv("/mnt/rating_tiny.csv").na.drop

  private def oldMatrixMadeByMap(df: DataFrame) = {
    val customerIndex = new StringIndexer()
      .setInputCol("originalCustomerID")
      .setOutputCol("customerID")

    val ratingsIndex = new StringIndexer()
      .setInputCol("newCategoryID")
      .setOutputCol("itemID")

    val pipeline = new Pipeline()
      .setStages(Array(customerIndex, ratingsIndex))

    val model1: PipelineModel = pipeline.fit(df)
    val transformedDf = model1.transform(df)

    val userColumn = customerIndex.getOutputCol
    val itemColumn = ratingsIndex.getOutputCol

    import org.apache.spark.sql.functions._
    val userToItemsList = transformedDf.groupBy(userColumn)
      .agg(collect_list(col(itemColumn)))
    userToItemsList.cache

    val countedItems = userToItemsList.withColumn("counted", processRow(col("collect_list(" + itemColumn + ")")))

    val func = new CountTableUDAF
    val itemMatrix = countedItems.agg(func(col("counted")))

    print(itemMatrix.count)
    print("Done")
  }

  private def testNewMatrixMutiply(df: DataFrame) = {
    val customerIndex = new StringIndexer()
      .setInputCol("originalCustomerID")
      .setOutputCol("customerID")

    val ratingsIndex = new StringIndexer()
      .setInputCol("newCategoryID")
      .setOutputCol("itemID")

    val pipeline = new Pipeline()
      .setStages(Array(customerIndex, ratingsIndex))

    val model1: PipelineModel = pipeline.fit(df)

    val transformedDf = model1.transform(df)
    import org.apache.spark.sql.functions._
    val rdd = transformedDf
      .select(col(customerIndex.getOutputCol).cast(LongType), col(ratingsIndex.getOutputCol).cast(LongType)).rdd
      .map(r => MatrixEntry(r.getLong(0), r.getLong(1), 1.0))
    val matrix = new CoordinateMatrix(rdd).toBlockMatrix().cache()
    val ata = matrix.transpose.multiply(matrix)
    val localMatrix = ata.toLocalMatrix().asML
    val broadcastMatrix = session.sparkContext.broadcast(localMatrix)

    val distinctItems = transformedDf.select("itemID").distinct().cache
    val items = distinctItems.collect()
    val broadcastItems = session.sparkContext.broadcast(items)
    val jaccardColumn = udf((i: Double) => {
      val countI = broadcastMatrix.value.apply(i.toInt, i.toInt)
      broadcastItems.value.map(j => {
        val countJ = broadcastMatrix.value.apply(j.getDouble(0).toInt, j.getDouble(0).toInt)
        val cooco = broadcastMatrix.value.apply(i.toInt, j.getDouble(0).toInt)
        val lift = cooco / (countI * countJ)
        val jaccard = cooco / (countI + countJ - cooco)
        //        MatrixEntry(i.toInt, j.getDouble(0).toInt, jaccard)
        Array(i, j.getDouble(0), countI, countJ, cooco, lift, jaccard)
      })
    })

    val jaccard = distinctItems
      .withColumn("jaccard", jaccardColumn(col("itemID"))).collect()

    var map2: Map[Double, Map[Double, Double]] = Map()
    jaccard.foreach(row => {
      row.getAs[mutable.WrappedArray[mutable.WrappedArray[Double]]]("jaccard").foreach(pair => {
        var littleMap = map2.getOrElse(pair(0), Map())
        val value: Double = littleMap.getOrElse(pair(1), 0.0)
        littleMap = littleMap + (pair(1) -> value)
        map2 = map2 + (pair(0) -> littleMap)
      })
    })
    map2.prettyPrint

    println(map2.toString())

    println(ata.toString())
    println("Done")
  }

  test("testSAR") {
    val df: DataFrame = session
      .createDataFrame(Seq(
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
        ("44", "Movie 10", 3, 3)))
      .toDF("customerIDOrg", "itemIDOrg", "rating", "timeOld")
    //    val customerIndex = new StringIndexer()
    //      .setInputCol("customerIDOrg")
    //      .setOutputCol("customerID")
    //
    //    val ratingsIndex = new StringIndexer()
    //      .setInputCol("itemIDOrg")
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
    //      .setRatingCol("rating")
    //
    //    val tvModel = sar.fit(transformedDf)
    //
    //    val model = tvModel.asInstanceOf[SARModel]
    //
    //    val k = 3
    //    var items = model.recommendForAllUsers(k)
    //    println(items.count)
    //    val labelsbc = session.sparkContext.broadcast(model1.stages(1).asInstanceOf[StringIndexerModel].labels)
    //    val labelItem = udf((itemId: Double) => labelsbc.value(itemId.toInt))
    //    (0 until k).map(i => items = items.withColumn("item_" + (i + 1), labelItem(col("recommendations.itemID")(i))))
    //
    //    val converter = new IndexToString()
    //      .setInputCol("customerID")
    //      .setOutputCol("customerIDrestored")

    evalTest(df, "customerIDOrg", "itemIDOrg", "rating")


    //    val converted = converter.transform(items).select("customerIDrestored", "item_1", "item_2", "item_3")

    //    converted.take(4).foreach(println(_))
  }

  test("testSAR_movies_tiny") {
    val df: DataFrame = session.read
      .option("header", "true") //reading the headers
      .option("inferSchema", "true") //reading the headers
      .csv("/mnt/rating_tiny.csv").na.drop

    val customerIndex = new StringIndexer()
      .setInputCol("originalCustomerID")
      .setOutputCol("customerID")

    val ratingsIndex = new StringIndexer()
      .setInputCol("newCategoryID")
      .setOutputCol("itemID")

    val pipeline = new Pipeline()
      .setStages(Array(customerIndex, ratingsIndex))

    val model1: PipelineModel = pipeline.fit(df)

    val transformedDf = model1.transform(df)

    val sar = new SAR()
      .setUserCol(customerIndex.getOutputCol)
      .setItemCol(ratingsIndex.getOutputCol)
      .setRatingCol("rating_total")
      .setTimeCol("session_startdt")

    val tvModel = sar.fit(transformedDf)

    val model = tvModel.asInstanceOf[SARModel]

    val k = 3
    var items = model.recommendForAllUsers(k)

    val labelsbc = session.sparkContext.broadcast(model1.stages(1).asInstanceOf[StringIndexerModel].labels)
    val labelItem = udf((itemId: Double) => labelsbc.value(itemId.toInt))
    (0 until k).map(i => items = items.withColumn("item_" + (i + 1), labelItem(col("recommended")(i))))

    val converter = new IndexToString()
      .setInputCol("customerID")
      .setOutputCol("customerIDrestored")

    val converted = converter.transform(items).select("customerIDrestored", "item_1", "item_2", "item_3")

    converted.take(4).foreach(println(_))
  }

  test("testSAR_movies_big") {
    val df: DataFrame = session.read
      .option("header", "true") //reading the headers
      .option("inferSchema", "true") //reading the headers
      .csv("/mnt/rating_big.csv").na.drop

    val customerIndex = new StringIndexer()
      .setInputCol("originalCustomerID")
      .setOutputCol("customerID")

    val ratingsIndex = new StringIndexer()
      .setInputCol("newCategoryID")
      .setOutputCol("itemID")

    val pipeline = new Pipeline()
      .setStages(Array(customerIndex, ratingsIndex))

    val model1: PipelineModel = pipeline.fit(df)

    val transformedDf = model1.transform(df)

    val sar = new SAR()
      .setUserCol(customerIndex.getOutputCol)
      .setItemCol(ratingsIndex.getOutputCol)
      .setRatingCol("rating_total")

    val tvModel = sar.fit(transformedDf)

    val model = tvModel.asInstanceOf[SARModel]

    val k = 3
    var items = model.recommendForAllUsers(k)

    val labelsbc = session.sparkContext.broadcast(model1.stages(1).asInstanceOf[StringIndexerModel].labels)
    val labelItem = udf((itemId: Double) => labelsbc.value(itemId.toInt))
    (0 until k).map(i => items = items.withColumn("item_" + (i + 1), labelItem(col("recommended")(i))))

    val converter = new IndexToString()
      .setInputCol("customerID")
      .setOutputCol("customerIDrestored")

    val converted = converter.transform(items).select("customerIDrestored", "item_1", "item_2", "item_3")

    converted.take(4).foreach(println(_))
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

    val ratings: DataFrame = session.read
      .option("header", "true") //reading the headers
      .option("inferSchema", "true") //reading the headers
      .csv("/mnt/ml-latest-small/ratings.csv").na.drop

    evalTest(ratings, "userId", "movieId", "rating")
    //3.53
  }

  test("testSAR_movie_lens_all_eval") {

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
      .csv("/mnt/ml-10m/parts/part*.csv").na.drop

    evalTest(ratings, "userId", "movieId", "rating")
  }

  private def evalTest(ratings: DataFrame, customerId: String, itemId: String, rating: String) = {
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
    ratings.unpersist()
    val sar = new SAR()
      .setUserCol(customerIndex.getOutputCol)
      .setItemCol(ratingsIndex.getOutputCol)
      .setRatingCol("rating")

    val paramGrid = new ParamGridBuilder()
      .build()

    val evaluator = new MsftRecommendationEvaluator()
      .setK(10)
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
    transformedDf.unpersist()
    val Array(train, test) = helper.splitDF(filtered)
    train.cache().count
    test.cache().count
    filtered.unpersist()

    val model = sar.fit(train)

    val users = model.recommendForAllUsers(10)
    users.cache.count
    train.unpersist

    val testData = helper.prepareTestData(test, users, 10)
    testData.cache.count
    test.unpersist
    users.unpersist

    evaluator.setNItems(test.rdd.map(r => r(1)).distinct().count())
    evaluator.evaluate(testData)
    evaluator.printMetrics()
    val metrics = "ndcgAt|map|precisionAtk|recallAtK|diversityAtK|maxDiversity"

    assert(evaluator.getMetricsList(0).getOrElse("map", 0.0) > 0)
    assert(evaluator.getMetricsList(0).getOrElse("ndcgAt", 0.0) > 0)
    assert(evaluator.getMetricsList(0).getOrElse("precisionAtk", 0.0) > 0)
    assert(evaluator.getMetricsList(0).getOrElse("recallAtK", 0.0) > 0)
    assert(evaluator.getMetricsList(0).getOrElse("diversityAtK", 0.0) > 0)
    assert(evaluator.getMetricsList(0).getOrElse("maxDiversity", 0.0) > 0)
  }

  test("testSAR_movies_big_eval") {

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

    val conf = new SparkConf()
      //      .setAppName("Testing Custom Model")
      .setMaster("local[*]")
      .set("spark.driver.memory", "80g")
      .set("spark.driver.maxResultSize", "5g")
    //      .set("spark.logConf", "true")
    //      .set("spark.sql.warehouse.dir", localWarehousePath)
    //      .set("spark.executor.extraJavaOptions", "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC
    // -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThread=20")
    val sess = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val ratings: DataFrame = sess.read
      .option("header", "true") //reading the headers
      .option("inferSchema", "true") //reading the headers
      .csv("/mnt/rating_big.csv").na.drop.cache()

    val customerIndex = new StringIndexer()
      .setInputCol("originalCustomerID")
      .setOutputCol("customerID")

    val ratingsIndex = new StringIndexer()
      .setInputCol("newCategoryID")
      .setOutputCol("itemID")

    val pipeline = new Pipeline()
      .setStages(Array(customerIndex, ratingsIndex))

    val transformedDf = pipeline.fit(ratings).transform(ratings).cache()

    val sar = new SAR()
      .setUserCol(customerIndex.getOutputCol)
      .setItemCol(ratingsIndex.getOutputCol)
      .setRatingCol("rating_total")

    val paramGrid = new ParamGridBuilder()
      .build()

    val evaluator = new MsftRecommendationEvaluator()
      .setK(3)
      .setSaveAll(true)

    val helper = new TrainValidRecommendSplit()
      .setEstimator(sar)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)
      .setUserCol(customerIndex.getOutputCol)
      .setRatingCol("rating_total")
      .setItemCol(ratingsIndex.getOutputCol)

    val filtered = helper.filterRatings(transformedDf).cache()
    val Array(train, test) = helper.splitDF(filtered)
    train.cache()
    test.cache()
    val model = sar.fit(train)

    val users = model.recommendForAllUsers(3).cache()
    users.count() //force users

    val testData = helper.prepareTestData(test, users, 3).cache()
    evaluator.setNItems(test.rdd.map(r => r(1)).distinct().count())
    evaluator.evaluate(testData)
    evaluator.printMetrics()
  }

  test("testSAR_movies_all_eval") {

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

    val conf = new SparkConf()
      //      .setAppName("Testing Custom Model")
      .setMaster("local[*]")
      .set("spark.driver.memory", "60G")
    //      .set("spark.logConf", "true")
    //      .set("spark.sql.warehouse.dir", localWarehousePath)
    //      .set("spark.executor.extraJavaOptions", "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC
    // -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThread=20")
    val sess = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val ratings: DataFrame = sess.read
      .option("header", "true") //reading the headers
      .option("inferSchema", "true") //reading the headers
      .csv("/mnt/rating.csv").na.drop.cache()

    val customerIndex = new StringIndexer()
      .setInputCol("originalCustomerID")
      .setOutputCol("customerID")

    val ratingsIndex = new StringIndexer()
      .setInputCol("newCategoryID")
      .setOutputCol("itemID")

    val pipeline = new Pipeline()
      .setStages(Array(customerIndex, ratingsIndex))

    val transformedDf = pipeline.fit(ratings).transform(ratings).cache()

    val sar = new SAR()
      .setUserCol(customerIndex.getOutputCol)
      .setItemCol(ratingsIndex.getOutputCol)
      .setRatingCol("rating_total")

    val paramGrid = new ParamGridBuilder()
      .build()

    val evaluator = new MsftRecommendationEvaluator()
      .setK(10)
      .setSaveAll(true)

    val helper = new TrainValidRecommendSplit()
      .setEstimator(sar)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)
      .setUserCol(customerIndex.getOutputCol)
      .setRatingCol("rating_total")
      .setItemCol(ratingsIndex.getOutputCol)

    val filtered = helper.filterRatings(transformedDf).cache()
    filtered.count
    val Array(train, test) = helper.splitDF(filtered)
    train.count
    test.count
    train.cache()
    test.cache()
    val model = sar.fit(train)

    val users = model.recommendForAllUsers(10).cache()
    users.count
    users.write.csv("/mnt/recommendations.csv")
    train.unpersist()

    val testData = helper.prepareTestData(test, users, 3).cache()
    test.unpersist()
    testData.count
    evaluator.setNItems(test.rdd.map(r => r(1)).distinct().count())
    evaluator.evaluate(testData)
    evaluator.printMetrics()
  }

  test("testJaccard") {
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

    val (itemMatrix, itemMatrixDF) = sar.itemFactorize(transformedDf)
    val itemMap = itemMatrix.take(1)(0)(0).asInstanceOf[Map[Double, Map[Double, Double]]]

    type MapType = Map[Double, Map[Double, Double]]
    val liftMap = sar.calcJaccard(itemMap)

    print(liftMap.toString)
  }

  test("testLift") {
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
      .setSupportThreshold(4)

    val (itemMatrix, itemMatrixDF) = sar.itemFactorize(transformedDf)
    val itemMap = itemMatrix.take(1)(0)(0).asInstanceOf[Map[Double, Map[Double, Double]]]

    type MapType = Map[Double, Map[Double, Double]]
    val liftMap = sar.calcLift(itemMap)

    print(liftMap.toString)
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

    val (itemMatrix, itemMatrixDF) = sar.itemFactorize(transformedDf)

    val lists = itemMatrixDF.collect()
    //    val itemMap: MapType = itemMatrix.take(1)(0)(0).asInstanceOf[Map[Double, Map[Double, Double]]]
    //
    //    assert(itemMap.get(0.0).get(0.0) == 32.0)
    //    assert(itemMap.get(0.0).get(5.0) == 21.0)
    //    assert(itemMap.get(0.0).get(1.0) == 25.0)
    //    print(itemMap.prettyPrint)
  }

  //  test("testItemFactorize2") {
  //    val df: DataFrame = session
  //      .createDataFrame(Seq(
  //        ("11", "Movie 01", 4),
  //        ("11", "Movie 03", 1),
  //        ("11", "Movie 04", 5),
  //        ("11", "Movie 05", 3),
  //        ("11", "Movie 06", 4),
  //        ("11", "Movie 07", 1),
  //        ("11", "Movie 08", 5),
  //        ("11", "Movie 09", 3),
  //        ("22", "Movie 01", 4),
  //        ("22", "Movie 02", 5),
  //        ("22", "Movie 03", 1),
  //        ("22", "Movie 05", 3),
  //        ("22", "Movie 06", 4),
  //        ("22", "Movie 07", 5),
  //        ("22", "Movie 08", 1),
  //        ("22", "Movie 10", 3),
  //        ("33", "Movie 01", 4),
  //        ("33", "Movie 03", 1),
  //        ("33", "Movie 04", 5),
  //        ("33", "Movie 05", 3),
  //        ("33", "Movie 06", 4),
  //        ("33", "Movie 08", 1),
  //        ("33", "Movie 09", 5),
  //        ("33", "Movie 10", 3),
  //        ("44", "Movie 01", 4),
  //        ("44", "Movie 02", 5),
  //        ("44", "Movie 03", 1),
  //        ("44", "Movie 05", 3),
  //        ("44", "Movie 06", 4),
  //        ("44", "Movie 07", 5),
  //        ("44", "Movie 08", 1),
  //        ("44", "Movie 10", 3)))
  //      .toDF("customerIDOrg", "itemIDOrg", "rating")
  //    val customerIndex = new StringIndexer()
  //      .setInputCol("customerIDOrg")
  //      .setOutputCol("customerID")
  //
  //    val ratingsIndex = new StringIndexer()
  //      .setInputCol("itemIDOrg")
  //      .setOutputCol("itemID")
  //
  //    val pipeline = new Pipeline()
  //      .setStages(Array(customerIndex, ratingsIndex))
  //
  //    val transformedDf = pipeline.fit(df).transform(df)
  //
  //    val sar = new SAR()
  //      .setUserCol(customerIndex.getOutputCol)
  //      .setItemCol(ratingsIndex.getOutputCol)
  //      .setRatingCol("rating")
  //
  //    type MapType = Map[(Double,Double), Double]
  //
  //    val itemMatrix = sar.itemFactorize(transformedDf)
  //    val itemMap: MapType = itemMatrix.take(1)(0)(0).asInstanceOf[Map[(Double,Double), Double]]
  //
  //    print(itemMap.prettyPrint)
  //    assert(itemMap.getOrElse((0.0,0.0),0.0) == 32.0)
  //    assert(itemMap.getOrElse((0.0,5.0),0.0) == 21.0)
  //    assert(itemMap.getOrElse((0.0,1.0),0.0) == 25.0)
  //  }

  implicit class PrettyPrintMap[K, V](val map: Map[K, V]) {
    def prettyPrint: PrettyPrintMap[K, V] = this

    override def toString: String = {
      val valuesString = toStringLines.mkString("\n")

      "Map (\n" + valuesString + "\n)"
    }

    def toStringLines = {
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

}
