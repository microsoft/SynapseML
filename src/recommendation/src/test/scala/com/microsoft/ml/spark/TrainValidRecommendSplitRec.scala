// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.io.File

import com.microsoft.ml.spark.SparkSessionFactory.{currentDir, customNormalize}
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.language.existentials

class TrainValidRecommendSplitRec
  extends TestBase {
  ignore("testSplit") {

    val (transformedDf: DataFrame,
    alsWReg: MsftRecommendation,
    evaluator: MsftRecommendationEvaluator,
    helper: TrainValidRecommendSplit) = {
      session.conf.set("spark.driver.extraClassPath", "$SPARK_HOME/jars/hadoop-azure-2.7.4.jar,azure-storage-2.0.0.jar")

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

      val store_name = "bigdatadevlogs"
      val key = ""
      val container = "ms-sampledata"

      session.sparkContext.hadoopConfiguration.set(
        "fs.azure.account.key.bigdatadevlogs.blob.core.windows.net", key)
      session.conf.set(
        "fs.azure.account.key.bigdatadevlogs.blob.core.windows.net", key)

      val data_rating = "iwanttv/day1_sample/percentilerating_videoseen_20170901_20170228_tiny.csv"
      val wasb = "wasb://" + container + "@" + store_name + ".blob.core.windows.net/" + data_rating
      val ratings = session.read
        .option("header", "true") //reading the headers
        .option("inferSchema", "true")
        .csv(wasb).dropDuplicates()

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

  ignore("testALS") {
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

  ignore("testModel") {
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

    val sess = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    sess.sparkContext.setLogLevel("WARN")

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
}
