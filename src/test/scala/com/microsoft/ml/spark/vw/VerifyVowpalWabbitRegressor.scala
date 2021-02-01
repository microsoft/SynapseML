// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.vw

import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.core.test.benchmarks.{Benchmarks, DatasetUtils}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.{Column, DataFrame}

class VerifyVowpalWabbitRegressor extends Benchmarks {

  val args = Array("", "--bfgs", "--adaptive")

  val numPartitions = 2

  verifyLearnerOnRegressionCsvFile("energyefficiency2012_data.train.csv", "Y1", 0,
    Some("X1,X2,X3,X4,X5,X6,X7,X8,Y1,Y2"))
  verifyLearnerOnRegressionCsvFile("airfoil_self_noise.train.csv", "Scaled sound pressure level", 1)
  verifyLearnerOnRegressionCsvFile("Buzz.TomsHardware.train.csv", "Mean Number of display (ND)", -3)
  verifyLearnerOnRegressionCsvFile("machine.train.csv", "ERP", -2)
  verifyLearnerOnRegressionCsvFile("Concrete_Data.train.csv", "Concrete compressive strength(MPa, megapascals)", 0)

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
    args.foreach { arg =>
      val argText = s" with args $arg"
      val testText = "Verify VowpalWabbitRegressor can be trained and scored on "
      test(testText + fileName + argText, TestBase.Extended) {
        val fileLocation = DatasetUtils.regressionTrainFile(fileName).toString
        val readDataset = readCSV(fileName, fileLocation).repartition(numPartitions)
        val dataset =
          if (columnsFilter.isDefined) {
            readDataset.select(columnsFilter.get.split(",").map(new Column(_)): _*)
          } else {
            readDataset
          }

        val featuresColumn = "features"

        val featurizer = new VowpalWabbitFeaturizer()
          .setInputCols(dataset.columns.filter(col => col != labelCol))
          .setOutputCol("features")

        val vw = new VowpalWabbitRegressor()
        val predCol = "pred"
        val trainData = featurizer.transform(dataset)
        val model = vw.setLabelCol(labelCol)
          .setFeaturesCol(featuresColumn)
          .setPredictionCol(predCol)
          .setNumPasses(3)
          .setArgs(s" $arg --quiet")
          .fit(trainData)
        val scoredResult = model.transform(trainData).drop(featuresColumn)

        val eval = new RegressionEvaluator()
          .setLabelCol(labelCol)
          .setPredictionCol(predCol)
          .setMetricName("rmse")
        val metric = eval.evaluate(scoredResult)
        addBenchmark(s"VowpalWabbitRegressor_${fileName}_${arg}",
          metric, decimals, false)
      }
    }
  }

  test("Compare benchmark results file to generated file", TestBase.Extended) {
    verifyBenchmarks()
  }

  test("Verify multiple input columns") {
    val fileName = "energyefficiency2012_data.train.csv"
    val columnsFilter = Some("X1,X2,X3,X4,X5,X6,X7,X8,Y1,Y2")
    val labelCol = "Y1"

    val fileLocation = DatasetUtils.regressionTrainFile(fileName).toString
    val readDataset = readCSV(fileName, fileLocation).repartition(numPartitions)
    val dataset =
      if (columnsFilter.isDefined) {
        readDataset.select(columnsFilter.get.split(",").map(new Column(_)): _*)
      } else {
        readDataset
      }

    val featuresColumn = "features"

    val featurizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("X1", "X2"))
      .setOutputCol("a")

    val featurizer2 = new VowpalWabbitFeaturizer()
      .setInputCols(Array("X3", "X4"))
      .setOutputCol("b")

    val trainData =
      featurizer2.transform(featurizer.transform(dataset))
      .repartition(1)

    val vw = new VowpalWabbitRegressor()
    val predCol = "pred"
    val model = vw.setLabelCol(labelCol)
      .setFeaturesCol("a")
      .setAdditionalFeatures(Array("b"))
      .setPredictionCol(predCol)
      //.setArgs(s"-a") // don't pass -a (audit) when using interactions...
      .setArgs("--quiet")
      .setInteractions(Array("ab"))
      .fit(trainData)
    val scoredResult = model.transform(trainData).drop(featuresColumn)

    val eval = new RegressionEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol(predCol)
      .setMetricName("rmse")
    val metric = eval.evaluate(scoredResult)

    assert (metric < 11)
  }

  test("Verify SGD followed-by BFGS") {
    val dataset = session.read.format("libsvm")
      .load(DatasetUtils.regressionTrainFile("triazines.scale.reg.train.svmlight").toString)
      .coalesce(1)

    val model1 = new VowpalWabbitRegressor()
      .setNumPasses(20)
      .setArgs("--holdout_off --loss_function quantile -q :: -l 0.1")
      .fit(dataset)

    val model2 = new VowpalWabbitRegressor()
      .setNumPasses(20)
      .setArgs("--holdout_off --loss_function quantile -q :: -l 0.1 --bfgs")
      .setInitialModel(model1.getModel)
      .fit(dataset)
  }
}
