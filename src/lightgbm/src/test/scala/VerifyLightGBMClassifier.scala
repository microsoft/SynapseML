// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.io.File

import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import java.nio.file.{Files, Path, Paths}
import org.apache.commons.io.FileUtils

import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}

/** Tests to validate the functionality of LightGBM module. */
class VerifyLightGBMClassifier extends Benchmarks with EstimatorFuzzing[LightGBMClassifier] {
  lazy val moduleName = "lightgbm"
  var portIndex = 30
  val numPartitions = 2
  val objective = "binary"

  // TODO: Need to add multiclass param with objective function
  // verifyLearnerOnMulticlassCsvFile("abalone.csv",                  "Rings", 2)
  // verifyLearnerOnMulticlassCsvFile("BreastTissue.csv",             "Class", 2)
  // verifyLearnerOnMulticlassCsvFile("CarEvaluation.csv",            "Col7", 2)
  verifyLearnerOnBinaryCsvFile("PimaIndian.csv",                   "Diabetes mellitus", 1)
  verifyLearnerOnBinaryCsvFile("data_banknote_authentication.csv", "class", 1)
  verifyLearnerOnBinaryCsvFile("task.train.csv",                   "TaskFailed10", 1)
  verifyLearnerOnBinaryCsvFile("breast-cancer.train.csv",          "Label", 1)
  verifyLearnerOnBinaryCsvFile("random.forest.train.csv",          "#Malignant", 1)
  verifyLearnerOnBinaryCsvFile("transfusion.csv",                  "Donated", 1)

  verifySaveBooster(
    fileName = "PimaIndian.csv",
    labelColumnName = "Diabetes mellitus",
    outputFileName = "model.txt")

  test("Compare benchmark results file to generated file", TestBase.Extended) {
    verifyBenchmarks()
  }

  test("Verify LightGBM Classifier can be run with TrainValidationSplit") {
    // Increment port index
    portIndex += numPartitions
    val fileName = "PimaIndian.csv"
    val labelColumnName = "Diabetes mellitus"
    val fileLocation = DatasetUtils.binaryTrainFile(fileName).toString
    val dataset = readCSV(fileName, fileLocation).repartition(numPartitions)
    val featuresColumn = "_features"
    val rawPredCol = "rawPrediction"
    val lgbm = new LightGBMClassifier()
      .setLabelCol(labelColumnName)
      .setFeaturesCol(featuresColumn)
      .setRawPredictionCol(rawPredCol)
      .setDefaultListenPort(LightGBMConstants.defaultLocalListenPort + portIndex)
      .setNumLeaves(5)
      .setNumIterations(10)
      .setObjective(objective)

    val paramGrid = new ParamGridBuilder()
      .addGrid(lgbm.numLeaves, Array(5, 10))
      .addGrid(lgbm.numIterations, Array(10, 20))
      .build()

    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lgbm)
      .setEvaluator(new BinaryClassificationEvaluator().setLabelCol(labelColumnName))
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)
      .setParallelism(2)

    val featurizer = LightGBMUtils.featurizeData(dataset, labelColumnName, featuresColumn)
    val model = trainValidationSplit.fit(featurizer.transform(dataset))
    model.transform(featurizer.transform(dataset))
    assert(model != null)
  }

  /** Reads a CSV file given the file name and file location.
    * @param fileName The name of the csv file.
    * @param fileLocation The full path to the csv file.
    * @return A dataframe from read CSV file.
    */
  def readCSV(fileName: String, fileLocation: String): DataFrame = {
    session.read
      .option("header", "true").option("inferSchema", "true")
      .option("treatEmptyValuesAsNulls", "false")
      .option("delimiter", if (fileName.endsWith(".csv")) "," else "\t")
      .csv(fileLocation)
  }

  def verifyLearnerOnBinaryCsvFile(fileName: String,
                                   labelColumnName: String,
                                   decimals: Int): Unit = {
    test("Verify LightGBMClassifier can be trained and scored on " + fileName, TestBase.Extended) {
      // Increment port index
      portIndex += numPartitions
      val fileLocation = DatasetUtils.binaryTrainFile(fileName).toString
      val dataset = readCSV(fileName, fileLocation).repartition(numPartitions)
      val lgbm = new LightGBMClassifier()
      val featuresColumn = lgbm.uid + "_features"
      val featurizer = LightGBMUtils.featurizeData(dataset, labelColumnName, featuresColumn)
      val rawPredCol = "rawPred"
      val trainData = featurizer.transform(dataset)
      val model = lgbm.setLabelCol(labelColumnName)
        .setFeaturesCol(featuresColumn)
        .setRawPredictionCol(rawPredCol)
        .setDefaultListenPort(LightGBMConstants.defaultLocalListenPort + portIndex)
        .setNumLeaves(5)
        .setNumIterations(10)
        .setObjective(objective)
        .fit(trainData)
      val scoredResult = model.transform(trainData).drop(featuresColumn)
      val splitFeatureImportances = model.getFeatureImportances("split")
      val gainFeatureImportances = model.getFeatureImportances("gain")
      assert(splitFeatureImportances.length == gainFeatureImportances.length)
      val eval = new BinaryClassificationEvaluator()
        .setLabelCol(labelColumnName)
        .setRawPredictionCol(rawPredCol)
      val metric = eval.evaluate(scoredResult)
      addBenchmark(s"LightGBMClassifier_$fileName", metric, decimals)
    }
  }

  def verifyLearnerOnMulticlassCsvFile(fileName: String,
                                       labelColumnName: String,
                                       decimals: Int): Unit = {
    test("Verify LightGBMClassifier can be trained and scored on multiclass " + fileName, TestBase.Extended) {
      // Increment port index
      portIndex += numPartitions
      val fileLocation = DatasetUtils.multiclassTrainFile(fileName).toString
      val dataset = readCSV(fileName, fileLocation).repartition(numPartitions)
      val lgbm = new LightGBMClassifier()
      val featuresColumn = lgbm.uid + "_features"
      val featurizer = LightGBMUtils.featurizeData(dataset, labelColumnName, featuresColumn)
      val predCol = "pred"
      val trainData = featurizer.transform(dataset)
      val model = lgbm.setLabelCol(labelColumnName)
        .setFeaturesCol(featuresColumn)
        .setPredictionCol(predCol)
        .setDefaultListenPort(LightGBMConstants.defaultLocalListenPort + portIndex)
        .setNumLeaves(5)
        .setNumIterations(10)
        .setObjective(objective)
        .fit(trainData)
      val scoredResult = model.transform(trainData).drop(featuresColumn)
      val splitFeatureImportances = model.getFeatureImportances("split")
      val gainFeatureImportances = model.getFeatureImportances("gain")
      assert(splitFeatureImportances.length == gainFeatureImportances.length)
      val eval = new MulticlassClassificationEvaluator()
        .setLabelCol(labelColumnName)
        .setPredictionCol(predCol)
        .setMetricName("accuracy")
      val metric = eval.evaluate(scoredResult)
      addBenchmark(s"LightGBMClassifier_$fileName", metric, decimals)
    }
  }

  override def testObjects(): Seq[TestObject[LightGBMClassifier]] = {
    val fileName = "PimaIndian.csv"
    val labelCol = "Diabetes mellitus"
    val featuresCol = "feature"
    val fileLocation = DatasetUtils.binaryTrainFile(fileName).toString
    val dataset = readCSV(fileName, fileLocation)
    val featurizer = LightGBMUtils.featurizeData(dataset, labelCol, featuresCol)
    val train = featurizer.transform(dataset)

    Seq(new TestObject(
      new LightGBMClassifier()
        .setLabelCol(labelCol)
        .setFeaturesCol(featuresCol)
        .setNumLeaves(5)
        .setObjective(objective),
      train))
  }

  def verifySaveBooster(fileName: String,
                       outputFileName: String,
                       labelColumnName: String): Unit = {
    test("Verify LightGBMClassifier save booster to " + fileName) {
      // Increment port index
      portIndex += numPartitions
      val fileLocation = DatasetUtils.binaryTrainFile(fileName).toString
      val dataset = readCSV(fileName, fileLocation).repartition(numPartitions)
      val lgbm = new LightGBMClassifier()
      val featuresColumn = lgbm.uid + "_features"
      val featurizer = LightGBMUtils.featurizeData(dataset, labelColumnName, featuresColumn)
      val rawPredCol = "rawPred"
      val testData = featurizer.transform(dataset)
      val model = lgbm.setLabelCol(labelColumnName)
        .setFeaturesCol(featuresColumn)
        .setRawPredictionCol(rawPredCol)
        .setDefaultListenPort(LightGBMConstants.defaultLocalListenPort + portIndex)
        .setNumLeaves(5)
        .setNumIterations(10)
        .setObjective(objective)
        .fit(testData)

      val targetDir: Path = Paths.get(getClass.getResource("/").toURI)
      val modelPath = targetDir.toString() + "/" + outputFileName
      FileUtils.deleteDirectory(new File(modelPath))
      model.saveNativeModel(session, modelPath)
      assert(Files.exists(Paths.get(modelPath)), true)

      val oldModelString = model.getModel.model
      val newModel = lgbm.setLabelCol(labelColumnName)
        .setFeaturesCol(featuresColumn)
        .setRawPredictionCol(rawPredCol)
        .setDefaultListenPort(LightGBMConstants.defaultLocalListenPort + portIndex)
        .setNumLeaves(5)
        .setNumIterations(10)
        .setObjective(objective)
        .setModelString(oldModelString)
        .fit(testData)
    }
  }

  override def reader: MLReadable[_] = LightGBMClassifier

  override def modelReader: MLReadable[_] = LightGBMClassificationModel
}
