// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.core.env.FileUtilities.File
import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.core.test.benchmarks.{Benchmarks, RegressionTestUtils}
import com.microsoft.ml.spark.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.Column

/** Tests to validate the functionality of LightGBM module.
  */
class VerifyLightGBMRegressor extends Benchmarks with EstimatorFuzzing[LightGBMRegressor] {
  override val historicMetricsFile  = new File(thisDirectory, "regressionBenchmarkMetrics.csv")
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
    compareBenchmarkFiles()
  }

  def verifyLearnerOnRegressionCsvFile(fileName: String,
                                       labelColumnName: String,
                                       decimals: Int,
                                       columnsFilter: Option[String] = None): Unit = {
    test("Verify LightGBMRegressor can be trained and scored on " + fileName, TestBase.Extended) {
      // Increment port index
      portIndex += numPartitions
      val fileLocation = RegressionTestUtils.regressionTrainFile(fileName).toString
      val readDataset = readCSV(fileName, fileLocation).repartition(numPartitions)
      val dataset =
        if (columnsFilter.isDefined) {
          readDataset.select(columnsFilter.get.split(",").map(new Column(_)): _*)
        } else {
          readDataset
        }
      val lgbm = new LightGBMRegressor()
      val featuresColumn = lgbm.uid + "_features"
      val featurizer = LightGBMUtils.featurizeData(dataset, labelColumnName, featuresColumn)
      val predCol = "pred"
      val model = lgbm.setLabelCol(labelColumnName)
        .setFeaturesCol(featuresColumn)
        .setDefaultListenPort(LightGBMConstants.defaultLocalListenPort + portIndex)
        .setNumLeaves(5)
        .setNumIterations(10)
        .setPredictionCol(predCol)
        .fit(featurizer.transform(dataset))
      val scoredResult = model.transform(featurizer.transform(dataset)).drop(featuresColumn)
      val eval = new RegressionEvaluator()
        .setLabelCol(labelColumnName)
        .setPredictionCol(predCol)
        .setMetricName("rmse")
      val metric = eval.evaluate(scoredResult)
      addAccuracyResult(fileName, "LightGBMRegressor",
        round(metric, decimals))
    }
  }

  override def testObjects(): Seq[TestObject[LightGBMRegressor]] = {
    val fileName = "machine.train.csv"
    val labelCol = "ERP"
    val featuresCol = "feature"
    val fileLocation = RegressionTestUtils.regressionTrainFile(fileName).toString
    val dataset = readCSV(fileName, fileLocation)
    val featurizer = LightGBMUtils.featurizeData(dataset, labelCol, featuresCol)
    val train = featurizer.transform(dataset)

    Seq(new TestObject(new LightGBMRegressor().setLabelCol(labelCol).setFeaturesCol(featuresCol).setNumLeaves(5),
      train))
  }

  override def reader: MLReadable[_] = LightGBMRegressor

  override def modelReader: MLReadable[_] = LightGBMRegressionModel
}
