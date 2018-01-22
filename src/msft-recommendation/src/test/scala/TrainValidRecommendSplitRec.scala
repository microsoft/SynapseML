// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.io.File

import com.microsoft.ml.spark.SparkSessionFactory.{currentDir, customNormalize}
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.language.existentials

class TrainValidRecommendSplitRec
  extends TestBase with EstimatorFuzzing[TrainValidRecommendSplit] {

  override def testObjects(): Seq[TestObject[TrainValidRecommendSplit]] = {
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

    val customerIndex: StringIndexer = new StringIndexer()
      .setInputCol("customerIDOrg")
      .setOutputCol("customerID")

    val ratingsIndex: StringIndexer = new StringIndexer()
      .setInputCol("itemIDOrg")
      .setOutputCol("itemID")

    val pipeline = new Pipeline()
      .setStages(Array(customerIndex, ratingsIndex))

    val transformedDf = pipeline.fit(df).transform(df)

    val alsWReg = new MsftRecommendation()
      .setUserCol(customerIndex.getOutputCol)
      .setRatingCol("rating")
      .setItemCol(ratingsIndex.getOutputCol)

    val paramGrid = new ParamGridBuilder()
      .addGrid(alsWReg.regParam, Array(1.0))
      .build()

    val evaluator = new MsftRecommendationEvaluator()
      .setK(3)
    val tvRecommendationSplit = new TrainValidRecommendSplit()
      .setEstimator(alsWReg)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)
      .setUserCol(customerIndex.getOutputCol)
      .setRatingCol("rating")
      .setItemCol(ratingsIndex.getOutputCol)
    //    val testDF = pipeline.fit(df).transform(df)

    List(new TestObject(tvRecommendationSplit, transformedDf))
  }

  override def reader: MLReadable[_] = TrainValidRecommendSplit

  override def modelReader: MLReadable[_] = TrainValidRecommendSplitModel

  test("testSplit") {

    val (transformedDf: DataFrame,
    alsWReg: MsftRecommendation,
    evaluator: MsftRecommendationEvaluator,
    helper: TrainValidRecommendSplit) = {
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
        .toDF("customerIDOrg", "itemIDOrg", "rating")

      val ratings = dfRaw2.dropDuplicates()

      val customerIndex = new StringIndexer()
        .setInputCol("customerIDOrg")
        .setOutputCol("customerID")

      val ratingsIndex = new StringIndexer()
        .setInputCol("itemIDOrg")
        .setOutputCol("itemID")

      val pipeline = new Pipeline()
        .setStages(Array(customerIndex, ratingsIndex))

      val transformedDf = pipeline.fit(ratings).transform(ratings)

      val alsWReg = new MsftRecommendation()
        .setUserCol(customerIndex.getOutputCol)
        .setRatingCol("rating")
        .setItemCol(ratingsIndex.getOutputCol)

      val paramGrid = new ParamGridBuilder()
        .addGrid(alsWReg.regParam, Array(1.0))
        .build()

      val evaluator = new MsftRecommendationEvaluator()
        .setK(3)
        .setSaveAll(true)

      val helper = new TrainValidRecommendSplit()
        .setEstimator(alsWReg)
        .setEvaluator(evaluator)
        .setEstimatorParamMaps(paramGrid)
        .setTrainRatio(0.8)
        .setUserCol(customerIndex.getOutputCol)
        .setRatingCol("rating")
        .setItemCol(ratingsIndex.getOutputCol)
      (transformedDf, alsWReg, evaluator, helper)
    }

    val filtered = helper.filterRatings(transformedDf)
    val Array(train, test) = helper.splitDF(filtered)
    val model = alsWReg.fit(train)

    val users = model.recommendForAllUsers(5)

    val testData = helper.prepareTestData(test, users, 5)
    evaluator.setNItems(test.rdd.map(r => r(1)).distinct().count())
    evaluator.evaluate(testData)
    evaluator.printMetrics()
  }

  test("testALS") {
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
      .toDF("customerIDOrg", "itemIDOrg", "rating")

    val ratings = dfRaw2.dropDuplicates()

    val customerIndex = new StringIndexer()
      .setInputCol("customerIDOrg")
      .setOutputCol("customerID")

    val ratingsIndex = new StringIndexer()
      .setInputCol("itemIDOrg")
      .setOutputCol("itemID")

    val pipeline = new Pipeline()
      .setStages(Array(customerIndex, ratingsIndex))

    val transformedDf = pipeline.fit(ratings).transform(ratings)

    val als = new ALS()
      .setUserCol(customerIndex.getOutputCol)
      .setRatingCol("rating")
      .setItemCol(ratingsIndex.getOutputCol)

    val paramGrid = new ParamGridBuilder()
      .addGrid(als.regParam, Array(1.0))
      .build()

    val evaluator = new MsftRecommendationEvaluator()
      .setK(3)

    val tvRecommendationSplit = new TrainValidRecommendSplit()
      .setEstimator(als)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)
      .setUserCol(customerIndex.getOutputCol)
      .setRatingCol("rating")
      .setItemCol(ratingsIndex.getOutputCol)

    val tvModel = tvRecommendationSplit.fit(transformedDf)

    val model = tvModel.bestModel.asInstanceOf[ALSModel]

    val items = model.recommendForAllUsers(3)
    val users = model.recommendForAllItems(3)

    evaluator.setSaveAll(true)
    tvRecommendationSplit.fit(transformedDf)
    evaluator.printMetrics()
  }

  test("testModel") {
    val defaultWarehouseDirName = "spark-warehouse"
    lazy val localWarehousePath =
      "file:" +
        customNormalize(new File(currentDir, defaultWarehouseDirName)
          .getAbsolutePath())

    val conf = new SparkConf()
      .setAppName("Testing Custom Model")
      .setMaster("local[*]")
      .set("spark.logConf", "true")
      .set("spark.sql.warehouse.dir", localWarehousePath)
    //      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //      .set("spark.driver.extraClassPath", "/home/dciborow/netlib.jar")
    //      .set("spark.driver.extraClassPath", "/home/dciborow/netlib-sources.jar")

    val sess = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    sess.sparkContext.setLogLevel("WARN")
    //    session.sparkContext.getConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //30s

    val dfRaw2: DataFrame = sess
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

    val ratings = dfRaw2.dropDuplicates()

    val customerIndex = new StringIndexer()
      .setInputCol("customerIDOrg")
      .setOutputCol("customerID")

    val ratingsIndex = new StringIndexer()
      .setInputCol("itemIDOrg")
      .setOutputCol("itemID")

    val pipeline = new Pipeline()
      .setStages(Array(customerIndex, ratingsIndex))

    val transformedDf = pipeline.fit(ratings).transform(ratings)

    val alsWReg = new MsftRecommendation()
      .setUserCol(customerIndex.getOutputCol)
      .setRatingCol("rating")
      .setItemCol(ratingsIndex.getOutputCol)

    val paramGrid = new ParamGridBuilder()
      .addGrid(alsWReg.regParam, Array(0.1))
      .addGrid(alsWReg.rank, Array(80))
      .build()

    val evaluator = new MsftRecommendationEvaluator()
      .setK(3)
      .setSaveAll(true)

    val tvRecommendationSplit = new TrainValidRecommendSplit()
      .setEstimator(alsWReg)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.75)
      .setUserCol(customerIndex.getOutputCol)
      .setRatingCol("rating")
      .setItemCol(ratingsIndex.getOutputCol)

    val tvModel = tvRecommendationSplit.fit(transformedDf)

    val model = tvModel.bestModel.asInstanceOf[MsftRecommendationModel]

    evaluator.printMetrics()
    val items = model.recommendForAllUsers(3)
    print("item count")
    print(items.count)

    val users = model.recommendForAllItems(3)
    print("user count")
    print(users.count)
  }

  test("fuzzing") {
    validateExperiments()
    testRoundTrip()
  }
  //  test("Smoke test to verify that evaluate can be run for recommendations pipelines") {
  //    val dfRaw2: DataFrame = session
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
  //
  //    val ratings = dfRaw2.dropDuplicates()
  //
  //    val customerIndex = new StringIndexer()
  //      .setInputCol("customerIDOrg")
  //      .setOutputCol("customerID")
  //
  //    val ratingsIndex = new StringIndexer()
  //      .setInputCol("itemIDOrg")
  //      .setOutputCol("itemID")
  //
  //    val alsWReg = new MsftRecommendation()
  //      .setUserCol(customerIndex.getOutputCol)
  //      .setRatingCol("rating")
  //      .setItemCol(ratingsIndex.getOutputCol)
  //
  //    val paramGrid = new ParamGridBuilder()
  //      .addGrid(alsWReg.regParam, Array(1.0))
  //      .build()
  //
  //    val evaluator = new MsftRecommendationEvaluator()
  //      .setK(3)
  //
  //    val pipeline = new Pipeline()
  //      .setStages(Array(customerIndex, ratingsIndex, alsWReg))
  //
  //    val tvAlsWRegModel = new TrainValidRecommendSplit()
  //      .setEstimator(pipeline)
  //      .setEvaluator(evaluator)
  //      .setEstimatorParamMaps(paramGrid)
  //      .setTrainRatio(0.8)
  //      .setUserCol(customerIndex.getInputCol)
  //      .setRatingCol("rating")
  //      .setItemCol(ratingsIndex.getInputCol)
  //      .fit(dfRaw2)
  //
  //    val ratings = dfRaw2.dropDuplicates()
  //
  //    val als = new ALS()
  //      .setUserCol(customerIndex.getOutputCol)
  //      .setRatingCol("rating")
  //      .setItemCol(ratingsIndex.getOutputCol)
  //
  //    val pipelineAls = new Pipeline()
  //      .setStages(Array(customerIndex, ratingsIndex, als))
  //
  //    val tvAlsModel = new TrainValidRecommendSplit()
  //      .setEstimator(pipelineAls)
  //      .setEvaluator(evaluator)
  //      .setEstimatorParamMaps(paramGrid)
  //      .setTrainRatio(0.8)
  //      .setUserCol(customerIndex.getInputCol)
  //      .setRatingCol("rating")
  //      .setItemCol(ratingsIndex.getInputCol)
  //      .fit(dfRaw2)
  //
  //    val findBestModel = new FindBestModel()
  //      .setModels(Array(tvAlsWRegModel.asInstanceOf[Transformer], tvAlsModel.asInstanceOf[Transformer]))
  //      .setEvaluationMetric(ComputeModelStatistics.RmseSparkMetric)
  //    val bestModel = findBestModel.fit(dfRaw2)
  //    bestModel.transform(dfRaw2)
  //  }
  //
  //  test("Smoke test to verify that evaluate can be run for recommendations") {
  //
  //    val dfRaw2: DataFrame = session
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
  //
  //    val ratings = dfRaw2.dropDuplicates()
  //
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
  //    val transformedDf = pipeline.fit(ratings).transform(ratings)
  //
  //    val alsWReg = new MsftRecommendation()
  //      .setUserCol(customerIndex.getOutputCol)
  //      .setRatingCol("rating")
  //      .setItemCol(ratingsIndex.getOutputCol)
  //      .fit(dfRaw2)
  //
  //    val alsModel = new ALS()
  //      .setUserCol(customerIndex.getOutputCol)
  //      .setRatingCol("rating")
  //      .setItemCol(ratingsIndex.getOutputCol)
  //      .fit(dfRaw2)
  //
  //    val findBestModel = new FindBestModel()
  //      .setModels(Array(alsWReg.asInstanceOf[Transformer], alsModel.asInstanceOf[Transformer]))
  //      .setEvaluationMetric(ComputeModelStatistics.RmseSparkMetric)
  //    val bestModel = findBestModel.fit(dfRaw2)
  //    bestModel.transform(dfRaw2)
  //  }
//  test("testSplitWData") {
//    val defaultWarehouseDirName = "spark-warehouse"
//    lazy val localWarehousePath =
//      "file:" +
//        customNormalize(new File(currentDir, defaultWarehouseDirName)
//          .getAbsolutePath())
//
//    val conf = new SparkConf()
//      .setAppName("Testing Custom Model")
//      .setMaster("local[*]")
//      .set("spark.driver.memory", "8es0G")
//      .set("spark.logConf", "true")
//      .set("spark.sql.warehouse.dir", localWarehousePath)
//      .set("spark.executor.extraJavaOptions", "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC
  // -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThread=20")
//    val sess = SparkSession.builder()
//      .config(conf)
//      .getOrCreate()
//    sess.sparkContext.setLogLevel("WARN")
//
//    //Implicit Map(map -> 0.020415022341847663, mapk -> 0.013762790449597308, maxDiversity
  // -> 0.6402010050251257, diversityAtK -> 0.16532663316582916, recallAtK -> 0.013762790449597311,
  // ndcgAt -> 0.04521755862022604)
//    //Map(map -> 0.025498697400195208, mapk -> 0.014721797157235497, maxDiversity -> 0.9623115577889447,
  // diversityAtK -> 0.48743718592964824, recallAtK -> 0.014721797157235495,
  // ndcgAt -> 0.04688129124679781)Alert ProvidedAL - Suite TrainValidRecommendSplitRec took 2415.748s
//    //40min
//    val k = 10
//
//    val (transformedDf: DataFrame,
//    alsWReg: MsftRecommendation,
//    evaluator: MsftRecommendationEvaluator,
//    helper: TrainValidRecommendSplit) = {
//
//      val dfRaw2: DataFrame = sess.read
//        .option("header", "true") //reading the headers
//        .option("inferSchema", "true") //reading the headers
//        .csv("/mnt/rating.csv").na.drop
//
//      val ratings = dfRaw2.dropDuplicates()
//
//      val rating = "rating_total"
//
//      val customerIndex = new StringIndexer()
//        .setInputCol("originalCustomerID")
//        .setOutputCol("customerID")
//
//      val ratingsIndex = new StringIndexer()
//        .setInputCol("newCategoryID")
//        .setOutputCol("itemID")
//
//      val pipeline = new Pipeline()
//        .setStages(Array(customerIndex, ratingsIndex))
//
//      val transformedDf = pipeline.fit(ratings).transform(ratings)
//
//      val alsWReg = new MsftRecommendation()
//        .setUserCol(customerIndex.getOutputCol)
//        .setRatingCol(rating)
//        .setItemCol(ratingsIndex.getOutputCol)
//        .setImplicitPrefs(true)
//        .setRank(80)
//        .setMaxIter(20)
//
//      val paramGrid = new ParamGridBuilder()
//        .addGrid(alsWReg.regParam, Array(0.1))
//        .build()
//
//      val evaluator = new MsftRecommendationEvaluator()
//        .setK(k)
//        .setSaveAll(true)
//
//      val helper = new TrainValidRecommendSplit()
//        .setEstimator(alsWReg)
//        .setEvaluator(evaluator)
//        .setEstimatorParamMaps(paramGrid)
//        .setTrainRatio(0.75)
//        .setUserCol(customerIndex.getOutputCol)
//        .setRatingCol(rating)
//        .setItemCol(ratingsIndex.getOutputCol)
//      (transformedDf, alsWReg, evaluator, helper)
//    }
//    transformedDf.cache()
//    val tvModel = helper.fit(transformedDf)
//    evaluator.printMetrics()
//    tvModel.save("./model")
//  }
}
