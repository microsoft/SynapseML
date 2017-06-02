// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import FileUtilities._
import com.microsoft.ml.spark.schema.{CategoricalUtilities, SchemaConstants, SparkSchema}

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.classification._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

object ClassifierTestUtils {

  def classificationTrainFile(name: String): File =
    new File(s"${sys.env("DATASETS_HOME")}/Binary/Train", name)

  def multiclassClassificationTrainFile(name: String): File =
    new File(s"${sys.env("DATASETS_HOME")}/Multiclass/Train", name)

}

/**
  * Tests to validate the functionality of Train Classifier module.
  */
class VerifyTrainClassifier extends EstimatorFuzzingTest {

  val thisDirectory = new File("src/test/scala")
  val targetDirectory = new File("target")
  assert(thisDirectory.isDirectory, "-- the test should run in the sub-project root level")
  val historicMetricsFile  = new File(thisDirectory, "benchmarkMetrics.csv")
  val benchmarkMetricsFile = new File(targetDirectory, s"newMetrics_${System.currentTimeMillis}_.csv")

  val LogisticRegressionClassifierName = "LogisticRegression"
  val DecisionTreeClassifierName = "DecisionTreeClassification"
  val RandomForestClassifierName = "RandomForestClassification"
  val GradientBoostedTreesClassifierName = "GradientBoostedTreesClassification"
  val NaiveBayesClassifierName = "NaiveBayesClassifier"
  val MultilayerPerceptronClassifierName = "MultilayerPerceptronClassifier"

  val accuracyResults = ArrayBuffer.empty[String]
  def addAccuracyResult(items: Any*): Unit = {
    val line = items.map(_.toString).mkString(",")
    println(s"... $line")
    accuracyResults += line
    ()
  }

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

  test("Smoke test for training on a classifier") {
    val dataset: DataFrame = createMockDataset

    val logisticRegressor = TrainClassifierTestUtilities.createLogisticRegressor(mockLabelColumn)

    TrainClassifierTestUtilities.trainScoreDataset(mockLabelColumn, dataset, logisticRegressor)
  }

  test("Verify you can score on a dataset without a label column") {
    val dataset: DataFrame = createMockDataset

    val logisticRegressor = TrainClassifierTestUtilities.createLogisticRegressor(mockLabelColumn)

    val data = dataset.randomSplit(Seq(0.6, 0.4).toArray, 42)
    val trainData = data(0)
    val testData = data(1)

    val model = logisticRegressor.fit(trainData)

    model.transform(testData.drop(mockLabelColumn))
  }

  test("Verify train classifier works on a dataset with categorical columns") {
    val cat = "Cat"
    val dog = "Dog"
    val bird = "Bird"
    val dataset: DataFrame = session.createDataFrame(Seq(
      (0, 2, 0.50, 0.60, dog, cat),
      (1, 3, 0.40, 0.50, cat, dog),
      (0, 4, 0.78, 0.99, dog, bird),
      (1, 5, 0.12, 0.34, cat, dog),
      (0, 1, 0.50, 0.60, dog, bird),
      (1, 3, 0.40, 0.50, bird, dog),
      (0, 3, 0.78, 0.99, dog, cat),
      (1, 4, 0.12, 0.34, cat, dog),
      (0, 0, 0.50, 0.60, dog, cat),
      (1, 2, 0.40, 0.50, bird, dog),
      (0, 3, 0.78, 0.99, dog, bird),
      (1, 4, 0.12, 0.34, cat, dog)))
      .toDF(mockLabelColumn, "col1", "col2", "col3", "col4", "col5")

    val catDataset = SparkSchema.makeCategorical(
      SparkSchema.makeCategorical(dataset, "col4", "col4", false),
      "col5",
      "col5",
      false)

    val logisticRegressor = TrainClassifierTestUtilities.createLogisticRegressor(mockLabelColumn)
    TrainClassifierTestUtilities.trainScoreDataset(mockLabelColumn, catDataset, logisticRegressor)

    val randomForestClassifier = TrainClassifierTestUtilities.createRandomForestClassifier(mockLabelColumn)
    TrainClassifierTestUtilities.trainScoreDataset(mockLabelColumn, catDataset, randomForestClassifier)
  }

  test("Verify a trained classifier model can be saved and loaded") {
    val dataset: DataFrame = createMockDataset

    val logisticRegressor = TrainClassifierTestUtilities.createLogisticRegressor(mockLabelColumn)

    val model = logisticRegressor.fit(dataset)

    val myModelName = "testModel"
    lazy val dir = new File(myModelName)
    try {
      model.write.overwrite().save(myModelName)
      // write a second time with overwrite flag, verify still works
      model.write.overwrite().save(myModelName)
      // assert directory exists
      assert(dir.exists())

      // load the model
      val loadedModel = TrainedClassifierModel.load(myModelName)

      // verify model data loaded
      assert(loadedModel.labelColumn == model.labelColumn)
      assert(loadedModel.uid == model.uid)
      val transformedDataset = loadedModel.transform(dataset)
      val benchmarkDataset = model.transform(dataset)
      assert(verifyResult(transformedDataset, benchmarkDataset))
    } finally {
      // delete the file to cleanup
      FileUtilities.delTree(dir)
      ()
    }
  }

  test("Verify you can train on a dataset that contains a vector column") {
    val dataset: DataFrame = session.createDataFrame(Seq(
      (0, 2, 0.50, 0.60, 0, Vectors.dense(1.0, 0.1, -1.5)),
      (1, 3, 0.40, 0.50, 1, Vectors.dense(1.5, 0.2, -1.2)),
      (0, 4, 0.78, 0.99, 2, Vectors.dense(1.3, 0.3, -1.1)),
      (1, 5, 0.12, 0.34, 3, Vectors.sparse(3, Seq((0, 1.0), (2, 2.0)))),
      (0, 1, 0.50, 0.60, 0, Vectors.dense(1.0, 0.4, -1.23)),
      (1, 3, 0.40, 0.50, 1, Vectors.dense(1.1, 0.5, -1.024)),
      (0, 3, 0.78, 0.99, 2, Vectors.dense(1.0, 0.1, -1.22)),
      (1, 4, 0.12, 0.34, 3, Vectors.dense(Double.NaN, 0.2, -1.23)),
      (0, 0, 0.50, 0.60, 0, Vectors.dense(0.5, 0.3, 1.0)),
      (1, 2, 0.40, 0.50, 1, Vectors.dense(1.0, 0.4, -1.2)),
      (0, 3, 0.78, 0.99, 2, Vectors.dense(0.7, 0.5, -1.1)),
      (1, 4, 0.12, 0.34, 3, Vectors.dense(1.8, 0.1, 2.02))))
      .toDF(mockLabelColumn, "col1", "col2", "col3", "col4", "col5")

    val logisticRegressor = TrainClassifierTestUtilities.createLogisticRegressor(mockLabelColumn)
    TrainClassifierTestUtilities.trainScoreDataset(mockLabelColumn, dataset, logisticRegressor)
  }

  verifyLearnerOnMulticlassCsvFile("abalone.csv",                  "Rings", 2, true)
  // Has multiple columns with the same name.  Spark doesn't seem to be able to handle that yet.
  // verifyLearnerOnMulticlassCsvFile("arrhythmia.csv",               "Arrhythmia")
  verifyLearnerOnMulticlassCsvFile("BreastTissue.csv",             "Class", 2, true)
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

  test("Compare benchmark results file to generated file", TestBase.Extended){
    try writeFile(benchmarkMetricsFile, accuracyResults.mkString("\n") + "\n")
    catch {
      case e: java.io.IOException => throw new Exception("Not able to process benchmarks file")
    }
    val historicMetrics = readFile(historicMetricsFile, _.getLines.toList)
    if (historicMetrics.length != accuracyResults.length)
      throw new Exception(s"Mis-matching number of lines in new benchmarks file: $benchmarkMetricsFile")
    for (((hist,acc),i) <- (historicMetrics zip accuracyResults).zipWithIndex) {
      assert(hist == acc,
        s"""Lines do not match on file comparison:
           |  $historicMetricsFile:$i:
           |    $hist
           |  $benchmarkMetricsFile:$i:
           |    $acc
           |.""".stripMargin)
    }
  }

  def verifyLearnerOnBinaryCsvFile(fileName: String,
                                   labelColumnName: String,
                                   decimals: Int,
                                   includeNaiveBayes: Boolean): Unit = {
    test("Verify classifier can be trained and scored on " + fileName, TestBase.Extended) {
      val fileLocation = ClassifierTestUtils.classificationTrainFile(fileName).toString
      val (trainScoreResultLogisticRegression: DataFrame,
      trainScoreResultDecisionTree: DataFrame,
      trainScoreResultGradientBoostedTrees: Option[DataFrame],
      trainScoreResultRandomForest: DataFrame,
      trainScoreResultMultilayerPerceptron: Option[DataFrame],
      trainScoreResultNaiveBayes: Option[DataFrame]) =
        readAndScoreDataset(fileName, labelColumnName, fileLocation, true, includeNaiveBayes)

      // Evaluate and get auc, round to 2 decimals
      val (aucLogisticRegression, prLogisticRegression) =
        evalAUC(trainScoreResultLogisticRegression, labelColumnName, SchemaConstants.ScoresColumn, decimals)

      val (aucDecisionTree, prDecisionTree) =
        evalAUC(trainScoreResultDecisionTree, labelColumnName, SchemaConstants.ScoresColumn, decimals)

      val (aucGradientBoostedTrees, prGradientBoostedTrees) =
        evalAUC(trainScoreResultGradientBoostedTrees.get,
          labelColumnName,
          SchemaConstants.ScoredLabelsColumn,
          decimals)

      val (aucRandomForest, prRandomForest) =
        evalAUC(trainScoreResultRandomForest, labelColumnName, SchemaConstants.ScoresColumn, decimals)

      val (aucMultilayerPerceptron, prMultilayerPerceptron) =
        evalAUC(trainScoreResultMultilayerPerceptron.get,
          labelColumnName,
          SchemaConstants.ScoredLabelsColumn,
          decimals)

      addAccuracyResult(fileName, LogisticRegressionClassifierName,
                        aucLogisticRegression, prLogisticRegression)
      addAccuracyResult(fileName, DecisionTreeClassifierName,
                        aucDecisionTree, prDecisionTree)
      addAccuracyResult(fileName, GradientBoostedTreesClassifierName,
                        aucGradientBoostedTrees, prGradientBoostedTrees)
      addAccuracyResult(fileName, RandomForestClassifierName,
                        aucRandomForest, prRandomForest)
      addAccuracyResult(fileName, MultilayerPerceptronClassifierName,
                        aucMultilayerPerceptron, prMultilayerPerceptron)
      if (includeNaiveBayes) {
        val (aucNaiveBayes, prNaiveBayes) =
          evalAUC(trainScoreResultNaiveBayes.get,
            labelColumnName,
            SchemaConstants.ScoredLabelsColumn,
            decimals)
        addAccuracyResult(fileName, NaiveBayesClassifierName,
          aucNaiveBayes, prNaiveBayes)
      }
    }
  }

  def verifyLearnerOnMulticlassCsvFile(fileName: String,
                                       labelColumnName: String,
                                       decimals: Int,
                                       includeNaiveBayes: Boolean): Unit = {
    test("Verify classifier can be trained and scored on multiclass " + fileName, TestBase.Extended) {
      val fileLocation = ClassifierTestUtils.multiclassClassificationTrainFile(fileName).toString
      val (trainScoreResultLogisticRegression: DataFrame,
      trainScoreResultDecisionTree: DataFrame,
      trainScoreResultGradientBoostedTrees: Option[DataFrame],
      trainScoreResultRandomForest: DataFrame,
      trainScoreResultMultilayerPerceptron: Option[DataFrame],
      trainScoreResultNaiveBayes: Option[DataFrame]) =
        readAndScoreDataset(fileName, labelColumnName, fileLocation, false, includeNaiveBayes)

      // Evaluate and get accuracy, F1-Measure
      val (accuracyLogisticRegression, f1LogisticRegression) =
        evalMulticlass(trainScoreResultLogisticRegression,
          labelColumnName,
          SchemaConstants.ScoredLabelsColumn,
          decimals)

      val (accuracyDecisionTree, f1DecisionTree) =
        evalMulticlass(trainScoreResultDecisionTree, labelColumnName, SchemaConstants.ScoredLabelsColumn, decimals)

      val (accuracyRandomForest, f1RandomForest) =
        evalMulticlass(trainScoreResultRandomForest, labelColumnName, SchemaConstants.ScoredLabelsColumn, decimals)

      addAccuracyResult(fileName, LogisticRegressionClassifierName,
                        accuracyLogisticRegression, f1LogisticRegression)

      addAccuracyResult(fileName, DecisionTreeClassifierName,
                        accuracyDecisionTree, f1DecisionTree)

      addAccuracyResult(fileName, RandomForestClassifierName,
                        accuracyRandomForest, f1RandomForest)

      if (includeNaiveBayes) {
        val (accuracyNaiveBayes, f1NaiveBayes) =
          evalMulticlass(trainScoreResultNaiveBayes.get, labelColumnName, SchemaConstants.ScoredLabelsColumn, decimals)

        addAccuracyResult(fileName, NaiveBayesClassifierName,
          accuracyNaiveBayes, f1NaiveBayes)
      }
    }
  }

  def readAndScoreDataset(fileName: String,
                          labelColumnName: String,
                          fileLocation: String,
                          includeNonProb: Boolean,
                          includeNaiveBayes: Boolean)
      : (DataFrame, DataFrame, Option[DataFrame], DataFrame, Option[DataFrame], Option[DataFrame]) = {
    // TODO: Add other file types for testing
    val dataset: DataFrame =
    session.read.format("com.databricks.spark.csv")
      .option("header", "true").option("inferSchema", "true")
      .option("treatEmptyValuesAsNulls", "false")
      .option("delimiter", if (fileName.endsWith(".csv")) "," else "\t")
      .load(fileLocation)
    val logisticRegressor =
      TrainClassifierTestUtilities.createLogisticRegressor(labelColumnName)

    val decisionTreeClassifier =
      TrainClassifierTestUtilities.createDecisionTreeClassifier(labelColumnName)

    val gradientBoostedTreesClassifier =
      TrainClassifierTestUtilities.createGradientBoostedTreesClassifier(labelColumnName)

    val randomForestClassifier =
      TrainClassifierTestUtilities.createRandomForestClassifier(labelColumnName)

    val multilayerPerceptronClassifier =
      TrainClassifierTestUtilities.createMultilayerPerceptronClassifier(labelColumnName)

    val naiveBayesClassifier =
      TrainClassifierTestUtilities.createNaiveBayesClassifier(labelColumnName)

    val trainScoreResultLogisticRegression =
      TrainClassifierTestUtilities.trainScoreDataset(labelColumnName, dataset, logisticRegressor)

    val trainScoreResultDecisionTree =
      TrainClassifierTestUtilities.trainScoreDataset(labelColumnName, dataset, decisionTreeClassifier)

    val trainScoreResultGradientBoostedTrees =
      if (includeNonProb) {
        Some(TrainClassifierTestUtilities.trainScoreDataset(labelColumnName, dataset, gradientBoostedTreesClassifier))
      }
      else None

    val trainScoreResultMultilayerPerceptron =
      if (includeNonProb) {
        Some(TrainClassifierTestUtilities.trainScoreDataset(labelColumnName, dataset, multilayerPerceptronClassifier))
      }
      else None

    val trainScoreResultNaiveBayes =
      if (includeNaiveBayes) {
        Some(TrainClassifierTestUtilities.trainScoreDataset(labelColumnName, dataset, naiveBayesClassifier))
      }
      else None

    val trainScoreResultRandomForest =
      TrainClassifierTestUtilities.trainScoreDataset(labelColumnName, dataset, randomForestClassifier)
    (trainScoreResultLogisticRegression, trainScoreResultDecisionTree,
     trainScoreResultGradientBoostedTrees, trainScoreResultRandomForest,
      trainScoreResultMultilayerPerceptron, trainScoreResultNaiveBayes)
  }

  /**
    * Get the auc and area over PR for the scored dataset.
    *
    * @param scoredDataset The scored dataset to evaluate.
    * @param labelColumn The label column.
    * @param predictionColumn The prediction column.
    * @return The AUC for the scored dataset.
    */
  def evalAUC(scoredDataset: DataFrame,
              labelColumn: String,
              predictionColumn: String,
              decimals: Int): (Double, Double) = {
    // Get levels if categorical
    val levels = CategoricalUtilities.getLevels(scoredDataset.schema, labelColumn)
    if (levels.isEmpty) throw new Exception("Test unexpectedly received empty levels")
    val levelsToIndexMap: Map[Any, Double] = levels.get.zipWithIndex.map(t => t._1 -> t._2.toDouble).toMap

    val scoreAndLabels =
      scoredDataset.select(col(predictionColumn), col(labelColumn)).na.drop().rdd.map {
        case Row(prediction: Vector, label) => (prediction(1), levelsToIndexMap(label))
        case Row(prediction: Double, label) => (prediction, levelsToIndexMap(label))
      }
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val result = (round(metrics.areaUnderROC(), decimals),
      round(metrics.areaUnderPR(), decimals))
    metrics.unpersist()
    result
  }

  /**
    * Get the accuracy and f1-score from multiclass data.
    *
    * @param scoredDataset The scored dataset to evaluate.
    * @param labelColumn The label column.
    * @param predictionColumn The prediction column.
    * @return The AUC for the scored dataset.
    */
  def evalMulticlass(scoredDataset: DataFrame,
                     labelColumn: String,
                     predictionColumn: String,
                     decimals: Int): (Double, Double) = {

    // Get levels if categorical
    val levels = CategoricalUtilities.getLevels(scoredDataset.schema, labelColumn)
    if (levels.isEmpty) throw new Exception("Test unexpectedly received empty levels")
    val levelsToIndexMap: Map[Any, Double] = levels.get.zipWithIndex.map(t => t._1 -> t._2.toDouble).toMap

    val scoreAndLabels =
      scoredDataset.select(col(predictionColumn), col(labelColumn)).na.drop().rdd.map {
        case Row(prediction: Vector, label) => (prediction(1), levelsToIndexMap(label))
        case Row(prediction: Double, label) => (prediction, levelsToIndexMap(label))
      }
    val metrics = new MulticlassMetrics(scoreAndLabels)
    val result = (round(metrics.accuracy, decimals),
      round(metrics.weightedFMeasure, decimals))
    result
  }

  /**
    * Rounds the given metric to 2 decimals.
    * @param metric The metric to round.
    * @return The rounded metric.
    */
  def round(metric: Double, decimals: Int): Double = {
    BigDecimal(metric)
      .setScale(decimals, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  override def setParams(fitDataset: DataFrame, estimator: Estimator[_]): Estimator[_] =
    estimator.asInstanceOf[TrainClassifier].setModel(new LogisticRegression()).setLabelCol(mockLabelColumn)

  override def createFitDataset: DataFrame = createMockDataset

  override def schemaForDataset: StructType = ???

  override def getEstimator(): Estimator[_] = new TrainClassifier()
}

/**
  * Test helper methods for Train Classifier module.
  */
object TrainClassifierTestUtilities {

  def createLogisticRegressor(labelColumn: String): Estimator[TrainedClassifierModel] = {
    val logisticRegression = new LogisticRegression()
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setMaxIter(10)
    val trainClassifier = new TrainClassifier()
    trainClassifier
      .setModel(logisticRegression)
      .set(trainClassifier.labelCol, labelColumn)
  }

  def createDecisionTreeClassifier(labelColumn: String): Estimator[TrainedClassifierModel] = {
    val decisionTreeClassifier = new DecisionTreeClassifier()
      .setMaxBins(32)
      .setMaxDepth(5)
      .setMinInfoGain(0.0)
      .setMinInstancesPerNode(1)
      .setSeed(0L)
    val trainClassifier = new TrainClassifier()
    trainClassifier
      .setModel(decisionTreeClassifier)
      .set(trainClassifier.labelCol, labelColumn)
  }

  def createGradientBoostedTreesClassifier(labelColumn: String): Estimator[TrainedClassifierModel] = {
    val decisionTreeClassifier = new GBTClassifier()
      .setMaxBins(32)
      .setMaxDepth(5)
      .setMaxIter(20)
      .setMinInfoGain(0.0)
      .setMinInstancesPerNode(1)
      .setStepSize(0.1)
      .setSubsamplingRate(1.0)
      .setSeed(0L)
    val trainClassifier = new TrainClassifier()
    trainClassifier
      .setModel(decisionTreeClassifier)
      .set(trainClassifier.labelCol, labelColumn)
  }

  def createRandomForestClassifier(labelColumn: String): Estimator[TrainedClassifierModel] = {
    val decisionTreeClassifier = new RandomForestClassifier()
      .setMaxBins(32)
      .setMaxDepth(5)
      .setMinInfoGain(0.0)
      .setMinInstancesPerNode(1)
      .setNumTrees(20)
      .setSubsamplingRate(1.0)
      .setSeed(0L)
    val trainClassifier = new TrainClassifier()
    trainClassifier
      .setModel(decisionTreeClassifier)
      .set(trainClassifier.labelCol, labelColumn)
  }

  def createMultilayerPerceptronClassifier(labelColumn: String): Estimator[TrainedClassifierModel] = {
    val layers = Array[Int](2, 5, 2)
    val multilayerPerceptronClassifier = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(1)
      .setMaxIter(1)
      .setTol(1e-6)
      .setSeed(0L)
    val trainClassifier = new TrainClassifier()
    trainClassifier
      .setModel(multilayerPerceptronClassifier)
      .set(trainClassifier.labelCol, labelColumn)
  }

  def createNaiveBayesClassifier(labelColumn: String): Estimator[TrainedClassifierModel] = {
    val naiveBayesClassifier = new NaiveBayes()
    val trainClassifier = new TrainClassifier()
    trainClassifier
      .setModel(naiveBayesClassifier)
      .set(trainClassifier.labelCol, labelColumn)
  }

  def trainScoreDataset(labelColumn: String, dataset: DataFrame,
                        trainClassifier: Estimator[TrainedClassifierModel]): DataFrame = {
    val data = dataset.randomSplit(Seq(0.6, 0.4).toArray, 42)
    val trainData = data(0)
    val testData = data(1)

    val model = trainClassifier.fit(trainData)

    val scoredData = model.transform(testData)
    scoredData
  }

}
