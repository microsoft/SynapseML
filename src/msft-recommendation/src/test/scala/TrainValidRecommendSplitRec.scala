// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.{Pipeline, PipelineModel, Transformer}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame

import scala.language.existentials
import scala.tools.nsc.interpreter.session

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

//  test("testALSPipeline") {
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
//    val als = new ALS()
//      .setUserCol(customerIndex.getOutputCol)
//      .setRatingCol("rating")
//      .setItemCol(ratingsIndex.getOutputCol)
//
//    val pipeline = new Pipeline()
//      .setStages(Array(customerIndex, ratingsIndex, als))
//
//    val paramGrid = new ParamGridBuilder()
//      .addGrid(als.regParam, Array(1.0))
//      .build()
//
//    val evaluator = new MsftRecommendationEvaluator()
//      .setK(3)
//
//    val tvRecommendationSplit = new TrainValidRecommendSplit()
//      .setEstimator(pipeline)
//      .setEvaluator(evaluator)
//      .setEstimatorParamMaps(paramGrid)
//      .setTrainRatio(0.8)
//      .setUserCol(customerIndex.getInputCol)
//      .setRatingCol("rating")
//      .setItemCol(ratingsIndex.getInputCol)
//
//    val tvModel = tvRecommendationSplit.fit(dfRaw2)
//
//    val model = tvModel.bestModel.asInstanceOf[PipelineModel].stages.last.asInstanceOf[ALSModel]
//
//    val items = model.recommendForAllUsers(3)
//    val users = model.recommendForAllItems(3)
//  }

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

    val tvRecommendationSplit = new TrainValidRecommendSplit()
      .setEstimator(alsWReg)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)
      .setUserCol(customerIndex.getOutputCol)
      .setRatingCol("rating")
      .setItemCol(ratingsIndex.getOutputCol)

    val tvModel = tvRecommendationSplit.fit(transformedDf)

    val model = tvModel.bestModel.asInstanceOf[MsftRecommendationModel]

    val items = model.recommendForAllUsers(3)
    val users = model.recommendForAllItems(3)
  }

//  test("testPipeline") {
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
//    val tvRecommendationSplit = new TrainValidRecommendSplit()
//      .setEstimator(pipeline)
//      .setEvaluator(evaluator)
//      .setEstimatorParamMaps(paramGrid)
//      .setTrainRatio(0.8)
//      .setUserCol(customerIndex.getInputCol)
//      .setRatingCol("rating")
//      .setItemCol(ratingsIndex.getInputCol)
//
//    val tvModel = tvRecommendationSplit.fit(ratings)
//
//    val model = tvModel.bestModel.asInstanceOf[PipelineModel].stages.last.asInstanceOf[MsftRecommendationModel]
//
//    val items = model.recommendForAllUsers(3)
//    val users = model.recommendForAllItems(3)
//  }

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
}
