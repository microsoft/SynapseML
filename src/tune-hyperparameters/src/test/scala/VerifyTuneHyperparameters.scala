// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.Estimator
import org.apache.spark.ml.classification._
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer

import com.microsoft.ml.spark.metrics.MetricConstants

/** Tests to validate the functionality of Tune Hyperparameters module. */
class VerifyTuneHyperparameters extends Benchmarks {

  lazy val moduleName = "tune-hyperparameters"

  val mockLabelColumn = "Label"
  def createMockDataset: DataFrame = {
    session.createDataFrame(Seq(
      (0, 2, 0.50, 0.60, 0),
      (1, 3, 0.40, 0.50, 1),
      (0, 4, 0.78, 0.99, 2),
      (1, 5, 0.12, 0.34, 3),
      (0, 1, 0.50, 0.60, 0),
      (1, 3, 0.40, 0.50, 1),
      (0, 3, 0.78, 0.99, 2),
      (1, 4, 0.12, 0.34, 3),
      (0, 0, 0.50, 0.60, 0),
      (1, 2, 0.40, 0.50, 1),
      (0, 3, 0.78, 0.99, 2),
      (1, 4, 0.12, 0.34, 3)))
      .toDF(mockLabelColumn, "col1", "col2", "col3", "col4")
  }

  // Getting negative feature values in NaiveBayes, need to look into
  // verifyLearnerOnMulticlassCsvFile("abalone.csv",                  "Rings", 2, false)
  // Has multiple columns with the same name.  Spark doesn't seem to be able to handle that yet.
  // verifyLearnerOnMulticlassCsvFile("arrhythmia.csv",               "Arrhythmia")
  // Getting negative feature values in NaiveBayes, need to look into
  verifyLearnerOnMulticlassCsvFile("BreastTissue.csv",             "Class", 2, false)
  verifyLearnerOnMulticlassCsvFile("CarEvaluation.csv",            "Col7", 2, true)
  // Getting "code generation" exceeded max size limit error
  // verifyLearnerOnMulticlassCsvFile("mnist.train.csv",              "Label")
  // This works with 2.0.0, but on 2.1.0 it looks like it loops infinitely while leaking memory
  // verifyLearnerOnMulticlassCsvFile("au3_25000.csv",                "class", 2, true)
  // This takes way too long for a gated build.  Need to make it something like a p3 test case.
  // verifyLearnerOnMulticlassCsvFile("Seattle911.train.csv",         "Event Clearance Group")

  verifyLearnerOnBinaryCsvFile("PimaIndian.csv",                   "Diabetes mellitus", 2, true)
  verifyLearnerOnBinaryCsvFile("data_banknote_authentication.csv", "class", 2, false)
  verifyLearnerOnBinaryCsvFile("task.train.csv",                   "TaskFailed10", 2, true)
  verifyLearnerOnBinaryCsvFile("breast-cancer.train.csv",          "Label", 2, true)
  verifyLearnerOnBinaryCsvFile("random.forest.train.csv",          "#Malignant", 2, true)
  verifyLearnerOnBinaryCsvFile("transfusion.csv",                  "Donated", 2, true)
  // verifyLearnerOnBinaryCsvFile("au2_10000.csv",                    "class", 1)
  verifyLearnerOnBinaryCsvFile("breast-cancer-wisconsin.csv",      "Class", 2, true)
  verifyLearnerOnBinaryCsvFile("fertility_Diagnosis.train.csv",    "Diagnosis", 2, false)
  verifyLearnerOnBinaryCsvFile("bank.train.csv",                   "y", 2, false)
  verifyLearnerOnBinaryCsvFile("TelescopeData.csv",                " Class", 2, false)

  test("Compare benchmark results file to generated file", TestBase.Extended) {
    compareBenchmarkFiles()
  }

  def verifyLearnerOnBinaryCsvFile(fileName: String,
                                   labelColumnName: String,
                                   decimals: Int,
                                   includeNaiveBayes: Boolean): Unit = {
    test("Verify classifier can be trained and scored on " + fileName, TestBase.Extended) {
      val fileLocation = ClassifierTestUtils.classificationTrainFile(fileName).toString
      val bestModel = tuneDataset(fileName, labelColumnName, fileLocation, true, includeNaiveBayes)
      val bestMetric = bestModel.bestMetric
      addAccuracyResult(fileName, bestModel.getBestModelInfo,
        round(bestMetric, decimals))
    }
  }

  def verifyLearnerOnMulticlassCsvFile(fileName: String,
                                       labelColumnName: String,
                                       decimals: Int,
                                       includeNaiveBayes: Boolean): Unit = {
    test("Verify classifier can be trained and scored on multiclass " + fileName, TestBase.Extended) {
      val fileLocation = ClassifierTestUtils.multiclassClassificationTrainFile(fileName).toString
      val bestModel = tuneDataset(fileName, labelColumnName, fileLocation, false, includeNaiveBayes)
      val bestMetric = bestModel.bestMetric
      addAccuracyResult(fileName, bestModel.getBestModelInfo,
        round(bestMetric, decimals))
    }
  }

  def tuneDataset(fileName: String,
                          labelColumnName: String,
                          fileLocation: String,
                          includeNonProb: Boolean,
                          includeNaiveBayes: Boolean): TuneHyperparametersModel = {
    // TODO: Add other file types for testing
    val dataset: DataFrame =
    session.read.format("com.databricks.spark.csv")
      .option("header", "true").option("inferSchema", "true")
      .option("treatEmptyValuesAsNulls", "false")
      .option("delimiter", if (fileName.endsWith(".csv")) "," else "\t")
      .load(fileLocation)

    val models = ListBuffer[Estimator[_]]()
    val hyperParams = ListBuffer[(Param[_], Dist[_])]()
    val logisticRegressor = TrainClassifierTestUtilities.createLogisticRegressor(labelColumnName)
    val decisionTreeClassifier = TrainClassifierTestUtilities.createDecisionTreeClassifier(labelColumnName)
    val randomForestClassifier = TrainClassifierTestUtilities.createRandomForestClassifier(labelColumnName)
    models += logisticRegressor
    hyperParams ++= DefaultHyperparams.defaultRange(
      logisticRegressor.getModel.asInstanceOf[LogisticRegression])
    models += decisionTreeClassifier
    hyperParams ++= DefaultHyperparams.defaultRange(
      decisionTreeClassifier.getModel.asInstanceOf[DecisionTreeClassifier])
    models += randomForestClassifier
    hyperParams ++= DefaultHyperparams.defaultRange(
      randomForestClassifier.getModel.asInstanceOf[RandomForestClassifier])

    if (includeNonProb) {
      val gbtClassifier = TrainClassifierTestUtilities.createGradientBoostedTreesClassifier(labelColumnName)
      val mlpClassifier = TrainClassifierTestUtilities.createMultilayerPerceptronClassifier(labelColumnName)
      models += gbtClassifier
      hyperParams ++= DefaultHyperparams.defaultRange(
        gbtClassifier.getModel.asInstanceOf[GBTClassifier])
      models += mlpClassifier
      hyperParams ++= DefaultHyperparams.defaultRange(
        mlpClassifier.getModel.asInstanceOf[MultilayerPerceptronClassifier])
    }

    if (includeNaiveBayes) {
      val naiveBayesClassifier = TrainClassifierTestUtilities.createNaiveBayesClassifier(labelColumnName)
      models += naiveBayesClassifier
      hyperParams ++= DefaultHyperparams.defaultRange(
        naiveBayesClassifier.getModel.asInstanceOf[NaiveBayes])
    }

    val randomSpace = new RandomSpace(hyperParams.toArray)

    new TuneHyperparameters()
      .setModels(models.toArray)
      .setEvaluationMetric(MetricConstants.AccuracySparkMetric)
      .setNumFolds(4)
      .setNumRuns(models.length * 3)
      .setParallelism(1)
      .setParamSpace(randomSpace)
      .setSeed(1234L)
      .fit(dataset)
  }

  val reader: MLReadable[_] = TuneHyperparameters
  val modelReader: MLReadable[_] = TuneHyperparametersModel

  def testObjects(): Seq[TestObject[TuneHyperparameters]] =
    Seq(new TestObject({
      val logisticRegressor =
        TrainClassifierTestUtilities.createLogisticRegressor(mockLabelColumn)
      val decisionTreeClassifier =
        TrainClassifierTestUtilities.createDecisionTreeClassifier(mockLabelColumn)
      val models = ListBuffer[Estimator[_]]()
      val hyperParams = ListBuffer[(Param[_], Dist[_])]()
      hyperParams ++= DefaultHyperparams.defaultRange(logisticRegressor.getModel.asInstanceOf[LogisticRegression])
      hyperParams ++=
        DefaultHyperparams.defaultRange(decisionTreeClassifier.getModel.asInstanceOf[DecisionTreeClassifier])
      models += logisticRegressor
      models += decisionTreeClassifier
      val randomSpace = new RandomSpace(hyperParams.toArray)
      new TuneHyperparameters()
        .setModels(models.toArray)
        .setEvaluationMetric(MetricConstants.AccuracySparkMetric)
        .setNumFolds(2)
        .setNumRuns(2)
        .setParallelism(1)
        .setParamSpace(randomSpace)
        .setSeed(345L)
    }, createMockDataset))

}
