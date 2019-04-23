// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{Column, DataFrame}

/** Tests to validate the functionality of LightGBM module.
  */
class VerifyLightGBMRegressor extends Benchmarks with EstimatorFuzzing[LightGBMRegressor] {
  lazy val moduleName = "lightgbm"
  var portIndex = 0
  val numPartitions = 2
  val boostingTypes = Array("gbdt", "rf", "dart", "goss")

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
      .addGrid(lgbm.lambdaL1, Array(0.1, 0.5))
      .addGrid(lgbm.lambdaL2, Array(0.1, 0.5))
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
    // Validate lambda parameters set on model
    val modelStr = model.bestModel.asInstanceOf[LightGBMRegressionModel].getModel.model
    assert(modelStr.contains("[lambda_l1: 0.1]") || modelStr.contains("[lambda_l1: 0.5]"))
    assert(modelStr.contains("[lambda_l2: 0.1]") || modelStr.contains("[lambda_l2: 0.5]"))
  }

  test("Verify LightGBM Regressor with weight column") {
    // Increment port index
    portIndex += numPartitions
    val fileName = "airfoil_self_noise.train.csv"
    val labelColumnName = "Scaled sound pressure level"
    val fileLocation = DatasetUtils.regressionTrainFile(fileName).toString
    val dataset = readCSV(fileName, fileLocation).repartition(numPartitions)
    val featuresColumn = "_features"
    val rawPredCol = "rawPrediction"
    val weightColName = "weight"
    val lgbm = new LightGBMRegressor()
      .setLabelCol(labelColumnName)
      .setFeaturesCol(featuresColumn)
      .setDefaultListenPort(LightGBMConstants.defaultLocalListenPort + portIndex)
      .setNumLeaves(5)
      .setNumIterations(10)
      .setWeightCol(weightColName)

    val featurizer = LightGBMUtils.featurizeData(dataset, labelColumnName, featuresColumn)
    val transformedData = featurizer.transform(dataset)
    import org.apache.spark.sql.functions._
    val datasetWithNoWeight = transformedData.withColumn(weightColName, lit(1.0))
    val datasetWithWeight = transformedData.withColumn(weightColName,
      when(col(labelColumnName) <= 120, 1000.0).otherwise(1.0))
    datasetWithNoWeight.show()
    // Verify changing weight to be higher on instances with lower sound pressure causes labels to decrease on average
    val avglabelNoWeight = lgbm.fit(datasetWithNoWeight)
      .transform(datasetWithNoWeight).select(avg("prediction")).first().getDouble(0)
    val avglabelWeight = lgbm.fit(datasetWithWeight)
      .transform(datasetWithWeight).select(avg("prediction")).first().getDouble(0)
    assert(avglabelWeight < avglabelNoWeight)
  }

  test("Verify LightGBM Regressor categorical parameter") {
    // Increment port index
    portIndex += numPartitions
    val fileName = "flare.data1.train.csv"
    val categoricalColumns = Array("Zurich class", "largest spot size", "spot distribution")
    val newCategoricalColumns = categoricalColumns.map("c_" + _)
    val labelColumnName = "M-class flares production by this region"
    val fileLocation = DatasetUtils.regressionTrainFile(fileName).toString
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
    val predCol = "prediction"
    val lgbm = new LightGBMRegressor()
      .setCategoricalSlotNames(newCategoricalColumns)
      .setLabelCol(labelColumnName)
      .setFeaturesCol(featuresColumn)
      .setDefaultListenPort(LightGBMConstants.defaultLocalListenPort + portIndex)
      .setNumLeaves(5)
      .setNumIterations(10)
      .setPredictionCol(predCol)

    val featurizer = LightGBMUtils.featurizeData(trainData, labelColumnName, featuresColumn)
    val transformedData = featurizer.transform(trainData)
    val scoredData = featurizer.transform(testData)
    val eval = new RegressionEvaluator()
      .setLabelCol(labelColumnName)
      .setPredictionCol(predCol)
      .setMetricName("rmse")
    val metric = eval.evaluate(
      lgbm.fit(transformedData).transform(scoredData))
    // Verify we get good result
    assert(metric < 0.6)
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
    boostingTypes.foreach { boostingType =>
      val boostingText = " with boosting type " + boostingType
      val testText = "Verify LightGBMRegressor can be trained and scored on "
      test(testText + fileName + boostingText, TestBase.Extended) {
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
        if (boostingType == "rf") {
          lgbm.setBaggingFraction(0.9)
          lgbm.setBaggingFreq(1)
        }
        val model = lgbm.setLabelCol(labelCol)
          .setFeaturesCol(featuresColumn)
          .setDefaultListenPort(LightGBMConstants.defaultLocalListenPort + portIndex)
          .setNumLeaves(5)
          .setNumIterations(10)
          .setPredictionCol(predCol)
          .setBoostingType(boostingType)
          .fit(trainData)
        val scoredResult = model.transform(trainData).drop(featuresColumn)
        val splitFeatureImportances = model.getFeatureImportances("split")
        val gainFeatureImportances = model.getFeatureImportances("gain")
        val featuresLength = trainData.select(featuresColumn).first().getAs[Vector](featuresColumn).size
        assert(splitFeatureImportances.length == gainFeatureImportances.length)
        assert(featuresLength == splitFeatureImportances.length)
        val eval = new RegressionEvaluator()
          .setLabelCol(labelCol)
          .setPredictionCol(predCol)
          .setMetricName("rmse")
        val metric = eval.evaluate(scoredResult)
        addBenchmark(s"LightGBMRegressor_${fileName}_${boostingType}", metric, decimals, false)
      }
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
