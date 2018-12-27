// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.io.File

import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import java.nio.file.{Files, Path, Paths}

import org.apache.commons.io.FileUtils
import org.apache.spark.ml.feature.{Binarizer, StringIndexer}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.linalg.Vector

/** Tests to validate the functionality of LightGBM module. */
class VerifyLightGBMClassifier extends Benchmarks with EstimatorFuzzing[LightGBMClassifier] {
  lazy val moduleName = "lightgbm"
  var portIndex = 30
  val numPartitions = 2
  val binaryObjective = "binary"
  val multiclassObject = "multiclass"

  // TODO: Look into error on abalone dataset
  // verifyLearnerOnMulticlassCsvFile("abalone.csv",                  "Rings", 2)
  verifyLearnerOnMulticlassCsvFile("BreastTissue.csv",             "Class", 2)
  verifyLearnerOnMulticlassCsvFile("CarEvaluation.csv",            "Col7", 2)
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
      .setObjective(binaryObjective)

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

  test("Verify LightGBM Classifier with weight column") {
    // Increment port index
    portIndex += numPartitions
    val fileName = "PimaIndian.csv"
    val labelColumnName = "Diabetes mellitus"
    val fileLocation = DatasetUtils.binaryTrainFile(fileName).toString
    val dataset = readCSV(fileName, fileLocation).repartition(numPartitions)
    val featuresColumn = "_features"
    val rawPredCol = "rawPrediction"
    val weightColName = "weight"
    val lgbm = new LightGBMClassifier()
      .setLabelCol(labelColumnName)
      .setFeaturesCol(featuresColumn)
      .setWeightCol(weightColName)
      .setRawPredictionCol(rawPredCol)
      .setDefaultListenPort(LightGBMConstants.defaultLocalListenPort + portIndex)
      .setNumLeaves(5)
      .setNumIterations(10)
      .setObjective(binaryObjective)

    val featurizer = LightGBMUtils.featurizeData(dataset, labelColumnName, featuresColumn)
    val transformedData = featurizer.transform(dataset)
    import org.apache.spark.sql.functions._
    val datasetWithNoWeight = transformedData.withColumn(weightColName, lit(1.0))
    val datasetWithWeight = transformedData.withColumn(weightColName,
      when(col(labelColumnName) >= 1, 100.0).otherwise(1.0))
    val labelOneCnt = lgbm.fit(datasetWithNoWeight)
      .transform(datasetWithNoWeight).select("prediction").filter(_.getDouble(0) == 1.0).count()
    val labelOneCntWeight = lgbm.fit(datasetWithWeight)
      .transform(datasetWithWeight).select("prediction").filter(_.getDouble(0) == 1.0).count()
    // Verify changing weight of one label significantly skews the results
    assert(labelOneCnt * 2 < labelOneCntWeight)
  }

  test("Verify LightGBM Classifier with unbalanced dataset") {
    // Increment port index
    portIndex += numPartitions
    val fileName = "task.train.csv"
    val labelColumnName = "TaskFailed10"
    val fileLocation = DatasetUtils.binaryTrainFile(fileName).toString
    val readDataset = readCSV(fileName, fileLocation).repartition(numPartitions)
    val seed = 42
    val data = readDataset.randomSplit(Array(0.8, 0.2), seed.toLong)
    val trainData = data(0)
    val testData = data(1)

    val featuresColumn = "_features"
    val rawPredCol = "rawPrediction"
    val lgbm = new LightGBMClassifier()
      .setLabelCol(labelColumnName)
      .setFeaturesCol(featuresColumn)
      .setRawPredictionCol(rawPredCol)
      .setDefaultListenPort(LightGBMConstants.defaultLocalListenPort + portIndex)
      .setNumLeaves(5)
      .setNumIterations(10)
      .setObjective(binaryObjective)

    val featurizer = LightGBMUtils.featurizeData(trainData, labelColumnName, featuresColumn)
    val transformedData = featurizer.transform(trainData)
    val scoredData = featurizer.transform(testData)
    val scoredWithoutIsUnbalance = lgbm.fit(transformedData)
      .transform(scoredData)
    val scoredWithIsUnbalance = lgbm.setIsUnbalance(true).fit(transformedData)
      .transform(scoredData)
    val eval = new BinaryClassificationEvaluator()
      .setLabelCol(labelColumnName)
      .setRawPredictionCol(rawPredCol)
    val metricWithoutIsUnbalance = eval.evaluate(scoredWithoutIsUnbalance)
    val metricWithIsUnbalance = eval.evaluate(scoredWithIsUnbalance)
    // Verify IsUnbalance parameter improves metric
    assert(metricWithoutIsUnbalance < metricWithIsUnbalance)
  }

  test("Verify LightGBM Classifier categorical parameter") {
    // Increment port index
    portIndex += numPartitions
    val fileName = "bank.train.csv"
    val categoricalColumns = Array("job", "marital", "education", "default",
      "housing", "loan", "contact", "y")
    val newCategoricalColumns = categoricalColumns.map("c_" + _)
    val labelColumnName = newCategoricalColumns.last
    val fileLocation = DatasetUtils.binaryTrainFile(fileName).toString
    val readDataset = readCSV(fileName, fileLocation).repartition(numPartitions)
    val mca = new MultiColumnAdapter().setInputCols(categoricalColumns).setOutputCols(newCategoricalColumns)
      .setBaseStage(new StringIndexer())
    val mcaModel = mca.fit(readDataset)
    val categoricalDataset = mcaModel.transform(readDataset).drop(categoricalColumns: _*)
    val seed = 42
    val data = categoricalDataset.randomSplit(Array(0.8, 0.2), seed.toLong)
    val trainData = data(0)
    val testData = data(1)

    val featuresColumn = "_features"
    val rawPredCol = "rawPrediction"
    val lgbm = new LightGBMClassifier()
      .setCategoricalSlotNames(newCategoricalColumns)
      .setLabelCol(labelColumnName)
      .setFeaturesCol(featuresColumn)
      .setRawPredictionCol(rawPredCol)
      .setDefaultListenPort(LightGBMConstants.defaultLocalListenPort + portIndex)
      .setNumLeaves(5)
      .setNumIterations(10)
      .setObjective(binaryObjective)

    val featurizer = LightGBMUtils.featurizeData(trainData, labelColumnName, featuresColumn)
    val transformedData = featurizer.transform(trainData)
    val scoredData = featurizer.transform(testData)
    val eval = new BinaryClassificationEvaluator()
      .setLabelCol(labelColumnName)
      .setRawPredictionCol(rawPredCol)
    val metric = eval.evaluate(
      lgbm.fit(transformedData).transform(scoredData))
    // Verify we get good result
    assert(metric > 0.8)
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
        .setObjective(binaryObjective)
        .fit(trainData)
      val scoredResult = model.transform(trainData).drop(featuresColumn)
      val splitFeatureImportances = model.getFeatureImportances("split")
      val gainFeatureImportances = model.getFeatureImportances("gain")
      assert(splitFeatureImportances.length == gainFeatureImportances.length)
      val featuresLength = trainData.select(featuresColumn).first().getAs[Vector](featuresColumn).size
      assert(featuresLength == splitFeatureImportances.length)
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
      val tmpTrainData = featurizer.transform(dataset)
      val labelizer = new ValueIndexer().setInputCol(labelColumnName).setOutputCol(labelColumnName).fit(tmpTrainData)
      val trainData = labelizer.transform(tmpTrainData)
      val model = lgbm.setLabelCol(labelColumnName)
        .setFeaturesCol(featuresColumn)
        .setPredictionCol(predCol)
        .setDefaultListenPort(LightGBMConstants.defaultLocalListenPort + portIndex)
        .setObjective(multiclassObject)
        .fit(trainData)
      val scoredResult = model.transform(trainData).drop(featuresColumn)
      val splitFeatureImportances = model.getFeatureImportances("split")
      val gainFeatureImportances = model.getFeatureImportances("gain")
      val featuresLength = trainData.select(featuresColumn).first().getAs[Vector](featuresColumn).size
      assert(featuresLength == splitFeatureImportances.length)
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
        .setObjective(binaryObjective),
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
        .setObjective(binaryObjective)
        .fit(testData)

      val targetDir: Path = Paths.get(getClass.getResource("/").toURI)
      val modelPath = targetDir.toString() + "/" + outputFileName
      FileUtils.deleteDirectory(new File(modelPath))
      model.saveNativeModel(modelPath, true)
      assert(Files.exists(Paths.get(modelPath)), true)

      val oldModelString = model.getModel.model
      val newModel = lgbm.setLabelCol(labelColumnName)
        .setFeaturesCol(featuresColumn)
        .setRawPredictionCol(rawPredCol)
        .setDefaultListenPort(LightGBMConstants.defaultLocalListenPort + portIndex)
        .setNumLeaves(5)
        .setNumIterations(10)
        .setObjective(binaryObjective)
        .setModelString(oldModelString)
        .fit(testData)
      // Verify can load model from file
      val newModelFromString = LightGBMClassificationModel.loadNativeModelFromString(oldModelString,
        labelColumnName, featuresColumn, rawPredictionColName = rawPredCol)
      val newModelFromFile = LightGBMClassificationModel.loadNativeModelFromFile(modelPath,
        labelColumnName, featuresColumn, rawPredictionColName = rawPredCol)
      val originalModelResult = model.transform(testData)
      val loadedModelFromStringResult = newModelFromString.transform(testData)
      val loadedModelFromFileResult = newModelFromFile.transform(testData)
      assert(loadedModelFromStringResult === originalModelResult)
      assert(loadedModelFromFileResult === originalModelResult)
    }
  }

  override def reader: MLReadable[_] = LightGBMClassifier

  override def modelReader: MLReadable[_] = LightGBMClassificationModel
}
