// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{Column, DataFrame}

/** Tests to validate the functionality of LightGBM module.
  */
class VerifyLightGBMRegressor extends Benchmarks with EstimatorFuzzing[LightGBMRegressor] {
  lazy val moduleName = "lightgbm"
  var portIndex = 0
  val numPartitions = 2

  verifyLearnerOnRegressionCsvFile("energyefficiency2012_data.train.csv", "Y1", 0,
    Some("X1,X2,X3,X4,X5,X6,X7,X8,Y1,Y2"))
  verifyLearnerOnRegressionCsvFile("airfoil_self_noise.train.csv", "Scaled sound pressure level", 1)
  verifyLearnerOnRegressionCsvFile("Buzz.TomsHardware.train.csv", "Mean Number of display (ND)", -3)
  verifyLearnerOnRegressionCsvFile("machine.train.csv", "ERP", -2)
  // TODO: Spark doesn't seem to like the column names here because of '.', figure out how to read in the data
  // verifyLearnerOnRegressionCsvFile("slump_test.train.csv", "Compressive Strength (28-day)(Mpa)", 2)
  verifyLearnerOnRegressionCsvFile("Concrete_Data.train.csv", "Concrete compressive strength(MPa, megapascals)", 0)

  test("Compare benchmark results file to generated file", TestBase.Extended) {
    verifyBenchmarks()
  }

  test("Verify LightGBM Regressor can be run with TrainValidationSplit") {
    // Increment port index
    portIndex += numPartitions
    val fileName = "airfoil_self_noise.train.csv"
    val labelColumnName = "Scaled sound pressure level"
    val fileLocation = DatasetUtils.regressionTrainFile(fileName).toString
    val dataset = readCSV(fileName, fileLocation).repartition(numPartitions)
    val featuresColumn = "_features"
    val rawPredCol = "rawPrediction"
    val lgbm = new LightGBMRegressor()
      .setLabelCol(labelColumnName)
      .setFeaturesCol(featuresColumn)
      .setDefaultListenPort(LightGBMConstants.defaultLocalListenPort + portIndex)
      .setNumLeaves(5)
      .setNumIterations(10)

    val paramGrid = new ParamGridBuilder()
      .addGrid(lgbm.numLeaves, Array(5, 10))
      .addGrid(lgbm.numIterations, Array(10, 20))
      .build()

    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lgbm)
      .setEvaluator(new RegressionEvaluator().setLabelCol(labelColumnName))
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)
      .setParallelism(2)

    val featurizer = LightGBMUtils.featurizeData(dataset, labelColumnName, featuresColumn)
    val model = trainValidationSplit.fit(featurizer.transform(dataset))
    model.transform(featurizer.transform(dataset))
    assert(model != null)
  }

  test("Verify LightGBM Regressor with tweedie distribution") {
    // Increment port index
    portIndex += numPartitions
    val fileName = "airfoil_self_noise.train.csv"
    val labelColumnName = "Scaled sound pressure level"
    val fileLocation = DatasetUtils.regressionTrainFile(fileName).toString
    val dataset = readCSV(fileName, fileLocation).repartition(numPartitions)
    val featuresColumn = "_features"
    val rawPredCol = "rawPrediction"
    val lgbm = new LightGBMRegressor()
      .setLabelCol(labelColumnName)
      .setFeaturesCol(featuresColumn)
      .setDefaultListenPort(LightGBMConstants.defaultLocalListenPort + portIndex)
      .setNumLeaves(5)
      .setNumIterations(10)
      .setObjective("tweedie")
      .setTweedieVariancePower(1.5)

    val paramGrid = new ParamGridBuilder()
      .addGrid(lgbm.tweedieVariancePower, Array(1.0, 1.2, 1.4, 1.6, 1.8, 1.99))
      .build()

    val cv = new CrossValidator()
      .setEstimator(lgbm)
      .setEvaluator(new RegressionEvaluator().setLabelCol(labelColumnName))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
      .setParallelism(2)

    val featurizer = LightGBMUtils.featurizeData(dataset, labelColumnName, featuresColumn)
    // Choose the best model for tweedie distribution
    val model = cv.fit(featurizer.transform(dataset))
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

  def verifyLearnerOnRegressionCsvFile(fileName: String,
                                       labelCol: String,
                                       decimals: Int,
                                       columnsFilter: Option[String] = None): Unit = {
    test("Verify LightGBMRegressor can be trained and scored on " + fileName, TestBase.Extended) {
      // Increment port index
      portIndex += numPartitions
      val fileLocation = DatasetUtils.regressionTrainFile(fileName).toString
      val readDataset = readCSV(fileName, fileLocation).repartition(numPartitions)
      val dataset =
        if (columnsFilter.isDefined) {
          readDataset.select(columnsFilter.get.split(",").map(new Column(_)): _*)
        } else {
          readDataset
        }
      val lgbm = new LightGBMRegressor()
      val featuresColumn = lgbm.uid + "_features"
      val featurizer = LightGBMUtils.featurizeData(dataset, labelCol, featuresColumn)
      val predCol = "pred"
      val trainData = featurizer.transform(dataset)
      val model = lgbm.setLabelCol(labelCol)
        .setFeaturesCol(featuresColumn)
        .setDefaultListenPort(LightGBMConstants.defaultLocalListenPort + portIndex)
        .setNumLeaves(5)
        .setNumIterations(10)
        .setPredictionCol(predCol)
        .fit(trainData)
      val scoredResult = model.transform(trainData).drop(featuresColumn)
      val splitFeatureImportances = model.getFeatureImportances("split")
      val gainFeatureImportances = model.getFeatureImportances("gain")
      assert(splitFeatureImportances.length == gainFeatureImportances.length)
      val eval = new RegressionEvaluator()
        .setLabelCol(labelCol)
        .setPredictionCol(predCol)
        .setMetricName("rmse")
      val metric = eval.evaluate(scoredResult)
      addBenchmark(s"LightGBMRegressor_$fileName", metric, decimals)
    }
  }

  override def testObjects(): Seq[TestObject[LightGBMRegressor]] = {
    val fileName = "machine.train.csv"
    val labelCol = "ERP"
    val featuresCol = "feature"
    val fileLocation = DatasetUtils.regressionTrainFile(fileName).toString
    val dataset = readCSV(fileName, fileLocation)
    val featurizer = LightGBMUtils.featurizeData(dataset, labelCol, featuresCol)
    val train = featurizer.transform(dataset)

    Seq(new TestObject(new LightGBMRegressor().setLabelCol(labelCol).setFeaturesCol(featuresCol).setNumLeaves(5),
      train))
  }

  override def reader: MLReadable[_] = LightGBMRegressor

  override def modelReader: MLReadable[_] = LightGBMRegressionModel
}
