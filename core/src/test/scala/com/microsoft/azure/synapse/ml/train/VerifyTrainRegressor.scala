// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.train

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.core.env.FileUtilities
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import org.apache.spark.ml.regression.{LinearRegression, RandomForestRegressor}
import org.apache.spark.ml.util.{MLReadable, MLWritable}
import org.apache.spark.ml.{Estimator, PipelineStage}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import java.io.File
import scala.collection.immutable.Seq

/** Tests to validate the functionality of Train Regressor module. */
class VerifyTrainRegressor extends EstimatorFuzzing[TrainRegressor] {

  val mockLabelColumn = "Label"

  def createMockDataset: DataFrame = {
    spark.createDataFrame(Seq(
      (0, 2, 0.50, 0.60, 0),
      (1, 3, 0.40, 0.50, 1),
      (2, 4, 0.78, 0.99, 2),
      (3, 5, 0.12, 0.34, 3),
      (0, 1, 0.50, 0.60, 0),
      (1, 3, 0.40, 0.50, 1),
      (2, 3, 0.78, 0.99, 2),
      (3, 4, 0.12, 0.34, 3),
      (0, 0, 0.50, 0.60, 0),
      (1, 2, 0.40, 0.50, 1),
      (2, 3, 0.78, 0.99, 2),
      (3, 4, 0.12, 0.34, 3)))
      .toDF(mockLabelColumn, "col1", "col2", "col3", "col4")
  }

  lazy val dfRoundTrip: DataFrame = createMockDataset
  val reader: MLReadable[_] = TrainRegressor
  val modelReader: MLReadable[_] = TrainedRegressorModel
  lazy val stageRoundTrip: PipelineStage with MLWritable =
    TrainRegressorTestUtilities.createLinearRegressor(mockLabelColumn)

  test("Smoke test for training on a regressor") {
    val dataset = createMockDataset

    val linearRegressor = TrainRegressorTestUtilities.createLinearRegressor(mockLabelColumn)

    TrainRegressorTestUtilities.trainScoreDataset(mockLabelColumn, dataset, linearRegressor)
  }

  test("issue 1667 - TrainRegressor ignores constant text columns when useful features remain") {
    val dataset = spark.createDataFrame(Seq(
      (0.0, "always the same", "red fox", 1.0),
      (1.0, "always the same", "blue fox", 2.0),
      (2.0, "always the same", "red dog", 3.0),
      (3.0, "always the same", "blue dog", 4.0),
      (4.0, "always the same", "green fox", 5.0),
      (5.0, "always the same", "green dog", 6.0)))
      .toDF(mockLabelColumn, "constantText", "informativeText", "numeric")

    val baselineModel = TrainRegressorTestUtilities.createLinearRegressor(mockLabelColumn)
      .setInputCols(Array("informativeText", "numeric"))
      .setNumFeatures(1024)
      .fit(dataset)

    val modelWithConstantText = TrainRegressorTestUtilities.createLinearRegressor(mockLabelColumn)
      .setInputCols(Array("constantText", "informativeText", "numeric"))
      .setNumFeatures(1024)
      .fit(dataset)

    val baselineScores = baselineModel.transform(dataset)
      .select("prediction")
      .collect()
      .map(_.getDouble(0))
      .toSeq
    val scoresWithConstantText = modelWithConstantText.transform(dataset)
      .select("prediction")
      .collect()
      .map(_.getDouble(0))
      .toSeq

    assert(baselineScores.length == scoresWithConstantText.length)
    baselineScores.zip(scoresWithConstantText).foreach { case (expected, actual) =>
      assert(math.abs(expected - actual) < 1e-8)
    }
  }

  test("issue 1667 - TrainRegressor fails clearly when all text features are constant") {
    val dataset = spark.createDataFrame(Seq(
      (0.0, "2"),
      (1.0, "2"),
      (2.0, "2"),
      (3.0, "2")))
      .toDF(mockLabelColumn, "text")

    val ex = intercept[IllegalArgumentException] {
      TrainRegressorTestUtilities.createLinearRegressor(mockLabelColumn)
        .setInputCols(Array("text"))
        .setNumFeatures(1024)
        .fit(dataset)
    }

    assert(ex.getMessage.toLowerCase.contains("no usable"))
    assert(ex.getMessage.contains("text"))
  }

  test("issue 1667 - TrainRegressor fits the original RandomForest constant-text repro") {
    val dataset = spark.createDataFrame(Seq(
      (0.0, "2", 0.50, 0.60, 0),
      (1.0, "2", 0.40, 0.50, 1),
      (2.0, "2", 0.78, 0.99, 2),
      (3.0, "2", 0.12, 0.34, 3),
      (0.0, "2", 0.50, 0.60, 0),
      (1.0, "2", 0.40, 0.50, 1),
      (2.0, "2", 0.78, 0.99, 2),
      (3.0, "2", 0.12, 0.34, 3)))
      .toDF(mockLabelColumn, "col1", "col2", "col3", "col4")

    val model = TrainRegressorTestUtilities.createRandomForestRegressor(mockLabelColumn)
      .setInputCols(Array("col1", "col2", "col3", "col4"))
      .setNumFeatures(1024)
      .fit(dataset)
    val predictions = model.transform(dataset).select("prediction").collect().map(_.getDouble(0))

    assert(predictions.length == dataset.count())
    assert(predictions.forall(value => !value.isNaN && !value.isInfinity))

    val modelFile = new File(tmpDir.toFile, "rf")
    model.write.overwrite().save(modelFile.toString)
    val loadedModel = TrainedRegressorModel.load(modelFile.toString)
    val loadedPredictions = loadedModel.transform(dataset).select("prediction").collect().map(_.getDouble(0))
    assert(predictions.toSeq === loadedPredictions.toSeq)
  }

  test("Verify you can score on a dataset without a label column") {
    val dataset: DataFrame = createMockDataset

    val linearRegressor = TrainRegressorTestUtilities.createLinearRegressor(mockLabelColumn)

    val data = dataset.randomSplit(Seq(0.6, 0.4).toArray, 42)
    val trainData = data(0)
    val testData = data(1)

    val model = linearRegressor.fit(trainData)

    model.transform(testData.drop(mockLabelColumn))
  }

  test("Verify train regressor works with different output types") {
    val dataset = createMockDataset
    val castLabelCol = "cast_" + mockLabelColumn
    for (outputType <-
           Seq(IntegerType, LongType, ByteType, BooleanType, FloatType, DoubleType, ShortType)) {
      val modifiedDataset = dataset.withColumn(castLabelCol, dataset(mockLabelColumn).cast(outputType))
      val linearRegressor = TrainRegressorTestUtilities.createLinearRegressor(castLabelCol)
      TrainRegressorTestUtilities.trainScoreDataset(castLabelCol, modifiedDataset, linearRegressor)
    }
  }

  test("Verify a trained regression model can be saved") {
    val dataset: DataFrame = createMockDataset

    val linearRegressor = TrainRegressorTestUtilities.createLinearRegressor(mockLabelColumn)

    val model = linearRegressor.fit(dataset)

    val modelFile = new File(tmpDir.toFile, "testModel")
    model.write.overwrite().save(modelFile.toString)
    // write a second time with overwrite flag, verify still works
    model.write.overwrite().save(modelFile.toString)
    // assert directory exists
    assert(modelFile.exists())

    // load the model
    val loadedModel = TrainedRegressorModel.load(modelFile.toString)

    // verify model data loaded
    assert(loadedModel.getLabelCol == model.getLabelCol)
    assert(loadedModel.uid == model.uid)
    val transformedDataset = loadedModel.transform(dataset)
    val benchmarkDataset = model.transform(dataset)
    assert(verifyResult(transformedDataset, benchmarkDataset))

  }

  test("Verify regressor can be trained and scored on airfoil_self_noise-train-csv") {
    val fileLocation = FileUtilities.join(BuildInfo.datasetDir,
      "Regression", "Train", "airfoil_self_noise.train.csv").toString
    val dataset = spark.read.format("com.databricks.spark.csv")
      .option("header", "true").option("inferSchema", "true")
      .option("delimiter", ",").option("treatEmptyValuesAsNulls", "false")
      .load(fileLocation)

    val labelColumn = "Scaled sound pressure level"

    val linearRegressor = TrainRegressorTestUtilities.createLinearRegressor(labelColumn)

    TrainRegressorTestUtilities.trainScoreDataset(labelColumn, dataset, linearRegressor)
  }

  test("Verify regressor can be trained and scored on CASP-train-csv") {
    val fileLocation = FileUtilities.join(BuildInfo.datasetDir,
      "Regression", "Train", "CASP.train.csv").toString
    val dataset = spark.read.format("com.databricks.spark.csv")
      .option("header", "true").option("inferSchema", "true")
      .option("delimiter", ",").option("treatEmptyValuesAsNulls", "false")
      .load(fileLocation)

    val labelColumn = "RMSD"

    val parameters = TrainRegressorTestUtilities.createRandomForestRegressor(labelColumn)

    TrainRegressorTestUtilities.trainScoreDataset(labelColumn, dataset, parameters)
  }

  override def testObjects(): Seq[TestObject[TrainRegressor]] = Seq(
    new TestObject(TrainRegressorTestUtilities.createLinearRegressor(mockLabelColumn),
      createMockDataset))

}

/** Test helper methods for Train Regressor module. */
object TrainRegressorTestUtilities {

  def createLinearRegressor(labelColumn: String): TrainRegressor = {
    val linearRegressor = new LinearRegression()
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
    val trainRegressor = new TrainRegressor()
    trainRegressor
      .setModel(linearRegressor)
      .set(trainRegressor.labelCol, labelColumn)
  }

  def createRandomForestRegressor(labelColumn: String): TrainRegressor = {
    val linearRegressor = new RandomForestRegressor()
      .setFeatureSubsetStrategy("auto")
      .setMaxBins(32)
      .setMaxDepth(5)
      .setMinInfoGain(0.0)
      .setMinInstancesPerNode(1)
      .setNumTrees(20)
    val trainRegressor = new TrainRegressor()
    trainRegressor
      .setModel(linearRegressor)
      .set(trainRegressor.labelCol, labelColumn)
  }

  def trainScoreDataset(labelColumn: String, dataset: DataFrame, trainRegressor: Estimator[TrainedRegressorModel])
  : DataFrame = {
    val data = dataset.randomSplit(Seq(0.6, 0.4).toArray, 42)
    val trainData = data(0)
    val testData = data(1)

    val model = trainRegressor.fit(trainData)
    val scoredData = model.transform(testData)
    scoredData
  }

}
