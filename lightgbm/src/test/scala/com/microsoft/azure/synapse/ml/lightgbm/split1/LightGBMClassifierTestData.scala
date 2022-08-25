// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.core.test.benchmarks.{Benchmarks, DatasetUtils}
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import com.microsoft.azure.synapse.ml.lightgbm._
import com.microsoft.azure.synapse.ml.stages.MultiColumnAdapter
import org.apache.commons.io.FileUtils
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row}

import java.io.File
import java.nio.file.{Files, Path, Paths}

// scalastyle:off magic.number

/** Base class for tests for LightGBM Classifier. */
abstract class LightGBMClassifierTestData extends Benchmarks
    with EstimatorFuzzing[LightGBMClassifier] with LightGBMTestUtils {
  override def reader: MLReadable[_] = LightGBMClassifier
  override def modelReader: MLReadable[_] = LightGBMClassificationModel
  lazy val pimaDF: DataFrame = loadBinary("PimaIndian.csv", "Diabetes mellitus").cache()
  lazy val taskDF: DataFrame = loadBinary("task.train.csv", "TaskFailed10").cache()
  lazy val breastTissueDF: DataFrame = loadMulticlass("BreastTissue.csv", "Class").cache()
  lazy val au3DF: DataFrame = loadMulticlass("au3_25000.csv", "class").cache()
  lazy val unfeaturizedBankTrainDF: DataFrame = {
    val categoricalColumns = Array("job", "marital", "education", "default", "housing", "loan", "contact", "y")
    val newCategoricalColumns: Array[String] = categoricalColumns.map("c_" + _)
    val df = readCSV(DatasetUtils.binaryTrainFile("bank.train.csv").toString)
      .repartition(numPartitions)
    val df2 = new MultiColumnAdapter().setInputCols(categoricalColumns).setOutputCols(newCategoricalColumns)
      .setBaseStage(new StringIndexer())
      .fit(df)
      .transform(df).drop(categoricalColumns: _*)
      .withColumnRenamed("c_y", labelCol)
    df2
    }.cache()
  lazy val indexedBankTrainDF: DataFrame = {
    LightGBMUtils.getFeaturizer(unfeaturizedBankTrainDF, labelCol, featuresCol,
      oneHotEncodeCategoricals = false).transform(unfeaturizedBankTrainDF)
    }.cache()
  lazy val indexedTaskDF: DataFrame = {
    val categoricalColumns = Array("TaskNm", "QueueName")
    val newCategoricalColumns: Array[String] = categoricalColumns.map("c_" + _)
    val df = readCSV(DatasetUtils.binaryTrainFile("task.train.csv").toString).repartition(numPartitions)
    val df2 = new MultiColumnAdapter().setInputCols(categoricalColumns).setOutputCols(newCategoricalColumns)
      .setBaseStage(new StringIndexer())
      .fit(df)
      .transform(df).drop(categoricalColumns: _*)
      .withColumnRenamed("TaskFailed10", labelCol)
      .drop(Array("IsControl10", "RanAsSystem10", "IsDAAMachine10", "IsUx", "IsClient"): _*)
    val tdf = LightGBMUtils.getFeaturizer(df2, labelCol, featuresCol, oneHotEncodeCategoricals = false).transform(df2)
    tdf
  }.cache()
  lazy val bankTrainDF: DataFrame = {
    LightGBMUtils.getFeaturizer(unfeaturizedBankTrainDF, labelCol, featuresCol).transform(unfeaturizedBankTrainDF)
    }.cache()

  def binaryEvaluator: BinaryClassificationEvaluator = {
    new BinaryClassificationEvaluator()
      .setLabelCol(labelCol)
      .setRawPredictionCol(rawPredCol)
  }

  def multiclassEvaluator: MulticlassClassificationEvaluator = {
    new MulticlassClassificationEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol(predCol)
      .setMetricName("accuracy")
  }

  val verifyLearnerTitleTemplate: String = "Verify LightGBMClassifier can be trained and scored on %s %s, %s mode"
  val abaloneFile: String = "abalone.csv"
  val breastTissueFile: String = "BreastTissue.csv"
  val carEvaluationFile: String = "CarEvaluation.csv"
  val pimaIndianFile: String = "PimaIndian.csv"
  val banknoteFile: String = "data_banknote_authentication.csv"
  val taskFile: String = "task.train.csv"
  val breastCancerFile: String = "breast-cancer.train.csv"
  val randomForestFile: String = "random.forest.train.csv"
  val transfusionFile: String = "transfusion.csv"

  def baseModel: LightGBMClassifier = {
    new LightGBMClassifier()
      .setFeaturesCol(featuresCol)
      .setRawPredictionCol(rawPredCol)
      .setDefaultListenPort(getAndIncrementPort())
      .setNumLeaves(5)
      .setNumIterations(10)
      .setObjective(LightGBMConstants.BinaryObjective)
      .setLabelCol(labelCol)
      .setLeafPredictionCol(leafPredCol)
      .setFeaturesShapCol(featuresShapCol)
      .setExecutionMode(executionMode)
  }

  def assertBinaryImprovement(sdf1: DataFrame, sdf2: DataFrame): Unit = {
    assert(binaryEvaluator.evaluate(sdf1) < binaryEvaluator.evaluate(sdf2))
  }

  def assertBinaryEquality(sdf1: DataFrame, sdf2: DataFrame): Unit = {
    assert(Math.abs(binaryEvaluator.evaluate(sdf1) - binaryEvaluator.evaluate(sdf2)) < 1e-10)
  }

  def assertMulticlassImprovement(sdf1: DataFrame, sdf2: DataFrame): Unit = {
    assert(multiclassEvaluator.evaluate(sdf1) < multiclassEvaluator.evaluate(sdf2))
  }

  def assertBinaryImprovement(v1: LightGBMClassifier, train1: DataFrame, test1: DataFrame,
                              v2: LightGBMClassifier, train2: DataFrame, test2: DataFrame
                             ): Unit = {
    assertBinaryImprovement(v1.fit(train1).transform(test1), v2.fit(train2).transform(test2))
  }

  def verifyLearnerOnBinaryCsvFile(fileName: String,
                                   labelColumnName: String,
                                   decimals: Int): Unit = {
    boostingTypes.foreach { boostingType =>
      val df = loadBinary(fileName, labelColumnName)
      val model = baseModel
        .setBoostingType(boostingType)

      if (boostingType == "rf") {
        model.setBaggingFraction(0.9)
        model.setBaggingFreq(1)
      }

      val fitModel = model.fit(df)
      val tdf = fitModel.transform(df)
      assertProbabilities(tdf, model)

      assertImportanceLengths(fitModel, df)
      addBenchmark(s"LightGBMClassifier_${fileName}_$boostingType",
        binaryEvaluator.evaluate(fitModel.transform(df)), decimals)
    }
  }

  def verifyLearnerOnMulticlassCsvFile(fileName: String,
                                       labelColumnName: String,
                                       decimals: Int): Unit = {
    verifyLearnerOnMulticlassCsvFile(fileName, labelColumnName, scala.math.pow(10, -decimals.toDouble))
  }

  def verifyLearnerOnMulticlassCsvFile(fileName: String,
                                       labelColumnName: String,
                                       precision: Double): Unit = {
    lazy val df = loadMulticlass(fileName, labelColumnName).cache()
    boostingTypes.foreach { boostingType =>

      val model = baseModel
        .setObjective(LightGBMConstants.MulticlassObjective)
        .setBoostingType(boostingType)

      if (boostingType == "rf") {
        model.setBaggingFraction(0.9)
        model.setBaggingFreq(1)
      }

      val fitModel = model.fit(df)
      val tdf = fitModel.transform(df)
      assertProbabilities(tdf, model)

      assertImportanceLengths(fitModel, df)
      addBenchmark(s"LightGBMClassifier_${fileName}_$boostingType",
        multiclassEvaluator.evaluate(tdf), precision)
    }
    df.unpersist()
  }

  override def testObjects(): Seq[TestObject[LightGBMClassifier]] = {
    Seq(new TestObject(baseModel, pimaDF.coalesce(1)))
  }

  def verifySaveBooster(fileName: String,
                        outputFileName: String,
                        labelColumnName: String,
                        colsToVerify: Array[String]): Unit = {
    val model = baseModel
    val df = loadBinary(fileName, labelColumnName)
    val fitModel = model.fit(df)

    val targetDir: Path = Paths.get(getClass.getResource("/").toURI)
    val modelPath = targetDir.toString + "/" + outputFileName
    FileUtils.deleteDirectory(new File(modelPath))
    fitModel.saveNativeModel(modelPath, overwrite = true)
    val retrievedModelStr = fitModel.getNativeModel()
    assert(Files.exists(Paths.get(modelPath)), true)

    val oldModelString = fitModel.getModel.modelStr.get
    // Assert model string is equal when retrieved from booster and getNativeModel API
    assert(retrievedModelStr == oldModelString)

    // Verify model string contains some feature
    colsToVerify.foreach(col => oldModelString.contains(col))

    assertFitWithoutErrors(model.setModelString(oldModelString), df)

    // Verify can load model from file
    val resultsFromString = LightGBMClassificationModel
      .loadNativeModelFromString(oldModelString)
      .setFeaturesCol(featuresCol)
      .setRawPredictionCol(rawPredCol)
      .setLeafPredictionCol(leafPredCol)
      .setFeaturesShapCol(featuresShapCol)
      .transform(df)

    val resultsFromFile = LightGBMClassificationModel
      .loadNativeModelFromFile(modelPath)
      .setFeaturesCol(featuresCol)
      .setRawPredictionCol(rawPredCol)
      .setLeafPredictionCol(leafPredCol)
      .setFeaturesShapCol(featuresShapCol)
      .transform(df)

    val resultsOriginal = fitModel.transform(df)

    assert(resultsFromString === resultsOriginal)
    assert(resultsFromFile === resultsOriginal)
  }
}
