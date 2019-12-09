// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm.split1

import java.io.File
import java.nio.file.{Files, Path, Paths}

import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.core.test.benchmarks.{Benchmarks, DatasetUtils}
import com.microsoft.ml.spark.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import com.microsoft.ml.spark.featurize.ValueIndexer
import com.microsoft.ml.spark.lightgbm._
import com.microsoft.ml.spark.stages.{MultiColumnAdapter, SPConstants, StratifiedRepartition}
import org.apache.commons.io.FileUtils
import org.apache.spark.TaskContext
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.linalg.{DenseVector, Vector}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._

// scalastyle:off magic.number
trait LightGBMTestUtils extends TestBase {

  /** Reads a CSV file given the file name and file location.
    *
    * @param fileLocation The full path to the csv file.
    * @return A dataframe from read CSV file.
    */
  def readCSV(fileLocation: String): DataFrame = {
    session.read
      .option("header", "true").option("inferSchema", "true")
      .option("treatEmptyValuesAsNulls", "false")
      .option("delimiter", if (fileLocation.endsWith(".csv")) "," else "\t")
      .csv(fileLocation)
  }

  def loadBinary(name: String, originalLabelCol: String): DataFrame = {
    val df = readCSV(DatasetUtils.binaryTrainFile(name).toString).repartition(numPartitions)
      .withColumnRenamed(originalLabelCol, labelCol)
    LightGBMUtils.getFeaturizer(df, labelCol, featuresCol).transform(df)
  }

  def loadRegression(name: String,
                     originalLabelCol: String,
                     columnsFilter: Option[Seq[String]] = None): DataFrame = {
    lazy val df = readCSV(DatasetUtils.regressionTrainFile(name).toString).repartition(numPartitions)
      .withColumnRenamed(originalLabelCol, labelCol)
    lazy val df2 =
      if (columnsFilter.isDefined) {
        df.select(columnsFilter.get.map(col): _*)
      } else {
        df
      }
    LightGBMUtils.getFeaturizer(df2, labelCol, featuresCol).transform(df)
  }

  def loadMulticlass(name: String, originalLabelCol: String): DataFrame = {
    val df = readCSV(DatasetUtils.multiclassTrainFile(name).toString).repartition(numPartitions)
      .withColumnRenamed(originalLabelCol, labelCol)
    val featurizedDF = LightGBMUtils.getFeaturizer(df, labelCol, featuresCol).transform(df)
    val indexedDF = new ValueIndexer().setInputCol(labelCol).setOutputCol(labelCol)
      .fit(featurizedDF).transform(featurizedDF)
    indexedDF
  }

  def assertProbabilities(tdf: DataFrame, model: LightGBMClassifier): Unit = {
    tdf.select(model.getRawPredictionCol, model.getProbabilityCol)
      .collect()
      .foreach(row => {
        val probabilities = row.getAs[DenseVector](1).values
        assert((probabilities.sum - 1.0).abs < 0.001)
        assert(probabilities.forall(probability => probability >= 0 && probability <= 1))
      })
  }

  def assertFitWithoutErrors(model: Estimator[_ <: Model[_]], df: DataFrame): Unit = {
    assert(model.fit(df).transform(df).collect().length > 0)
  }

  def assertImportanceLengths(fitModel: Model[_] with HasFeatureImportanceGetters, df: DataFrame): Unit = {
    val splitLength = fitModel.getFeatureImportances("split").length
    val gainLength = fitModel.getFeatureImportances("gain").length
    val featuresLength = df.select(featuresCol).first().getAs[Vector](featuresCol).size
    assert(splitLength == gainLength && splitLength == featuresLength)
  }

  lazy val numPartitions = 2
  val startingPortIndex = 0
  private var portIndex = startingPortIndex

  def getAndIncrementPort(): Int = {
    portIndex += numPartitions
    LightGBMConstants.DefaultLocalListenPort + portIndex
  }

  val boostingTypes = Array("gbdt", "rf", "dart", "goss")
  val featuresCol = "features"
  val labelCol = "labels"
  val rawPredCol = "rawPrediction"
  val initScoreCol = "initScore"
  val predCol = "prediction"
  val probCol = "probability"
  val weightCol = "weight"
  val validationCol = "validation"
  val seed = 42L

}

// scalastyle:off magic.number
/** Tests to validate the functionality of LightGBM module. */
class VerifyLightGBMClassifier extends Benchmarks with EstimatorFuzzing[LightGBMClassifier]
  with LightGBMTestUtils {

  lazy val pimaDF: DataFrame = loadBinary("PimaIndian.csv", "Diabetes mellitus").cache()
  lazy val taskDF: DataFrame = loadBinary("task.train.csv", "TaskFailed10").cache()
  lazy val breastTissueDF: DataFrame = loadMulticlass("BreastTissue.csv", "Class").cache()
  lazy val unfeaturizedBankTrainDF: DataFrame = {
    val categoricalColumns = Array(
      "job", "marital", "education", "default", "housing", "loan", "contact", "y")
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
  lazy val bankTrainDF: DataFrame = {
    LightGBMUtils.getFeaturizer(unfeaturizedBankTrainDF, labelCol, featuresCol).transform(unfeaturizedBankTrainDF)
  }.cache()

  val binaryObjective = "binary"
  val multiclassObject = "multiclass"

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

  // TODO: Look into error on abalone dataset
  // verifyLearnerOnMulticlassCsvFile("abalone.csv",                  "Rings", 2)
  verifyLearnerOnMulticlassCsvFile("BreastTissue.csv", "Class", .07)
  verifyLearnerOnMulticlassCsvFile("CarEvaluation.csv", "Col7", 2)
  verifyLearnerOnBinaryCsvFile("PimaIndian.csv", "Diabetes mellitus", 1)
  verifyLearnerOnBinaryCsvFile("data_banknote_authentication.csv", "class", 1)
  verifyLearnerOnBinaryCsvFile("task.train.csv", "TaskFailed10", 1)
  verifyLearnerOnBinaryCsvFile("breast-cancer.train.csv", "Label", 1)
  verifyLearnerOnBinaryCsvFile("random.forest.train.csv", "#Malignant", 1)
  verifyLearnerOnBinaryCsvFile("transfusion.csv", "Donated", 1)

  verifySaveBooster(
    fileName = "PimaIndian.csv",
    labelColumnName = "Diabetes mellitus",
    outputFileName = "model.txt",
    colsToVerify = Array("Diabetes pedigree function", "Age (years)"))

  test("Compare benchmark results file to generated file", TestBase.Extended) {
    verifyBenchmarks()
  }

  override def testExperiments(): Unit = {
    super.testExperiments()
  }

  override def testSerialization(): Unit = {
    super.testSerialization()
  }

  def baseModel: LightGBMClassifier = {
    new LightGBMClassifier()
      .setFeaturesCol(featuresCol)
      .setRawPredictionCol(rawPredCol)
      .setDefaultListenPort(getAndIncrementPort())
      .setNumLeaves(5)
      .setNumIterations(10)
      .setObjective(binaryObjective)
      .setLabelCol(labelCol)
  }

  test("Verify LightGBM Classifier can be run with TrainValidationSplit") {
    val model = baseModel.setUseBarrierExecutionMode(true)

    val paramGrid = new ParamGridBuilder()
      .addGrid(model.numLeaves, Array(5, 10))
      .addGrid(model.numIterations, Array(10, 20))
      .addGrid(model.lambdaL1, Array(0.1, 0.5))
      .addGrid(model.lambdaL2, Array(0.1, 0.5))
      .build()

    val fitModel = new TrainValidationSplit()
      .setEstimator(model)
      .setEvaluator(binaryEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)
      .setParallelism(2)
      .fit(pimaDF)

    fitModel.transform(pimaDF)
    assert(fitModel != null)

    // Validate lambda parameters set on model
    val modelStr = fitModel.bestModel.asInstanceOf[LightGBMClassificationModel].getModel.model
    assert(modelStr.contains("[lambda_l1: 0.1]") || modelStr.contains("[lambda_l1: 0.5]"))
    assert(modelStr.contains("[lambda_l2: 0.1]") || modelStr.contains("[lambda_l2: 0.5]"))
  }

  ignore("Verify LightGBM Classifier with batch training") {
    val batches = Array(0, 2, 10)
    batches.foreach(nBatches => assertFitWithoutErrors(baseModel.setNumBatches(nBatches), pimaDF))
  }

  def assertBinaryImprovement(sdf1: DataFrame, sdf2: DataFrame): Unit = {
    assert(binaryEvaluator.evaluate(sdf1) < binaryEvaluator.evaluate(sdf2))
  }

  def assertBinaryImprovement(v1: LightGBMClassifier, train1: DataFrame, test1: DataFrame,
                              v2: LightGBMClassifier, train2: DataFrame, test2: DataFrame
                             ): Unit = {
    assertBinaryImprovement(v1.fit(train1).transform(test1), v2.fit(train2).transform(test2))
  }

  test("Verify LightGBM Classifier continued training with initial score") {
    val convertUDF = udf((vector: DenseVector) => vector(1))
    val scoredDF1 = baseModel.fit(pimaDF).transform(pimaDF)
    val df2 = scoredDF1.withColumn(initScoreCol, convertUDF(col(rawPredCol)))
      .drop(predCol, rawPredCol, probCol)
    val scoredDF2 = baseModel.setInitScoreCol(initScoreCol).fit(df2).transform(df2)

    assertBinaryImprovement(scoredDF1, scoredDF2)
  }

  test("Verify LightGBM Classifier with min gain to split parameter") {
    // If the min gain to split is too high, assert AUC lower for training data (assert parameter works)
    val scoredDF1 = baseModel.setMinGainToSplit(99999).fit(pimaDF).transform(pimaDF)
    val scoredDF2 = baseModel.fit(pimaDF).transform(pimaDF)
    assertBinaryImprovement(scoredDF1, scoredDF2)
  }

  test("Verify LightGBM Classifier with max delta step parameter") {
    // If the max delta step is specified, assert AUC differs (assert parameter works)
    // Note: the final max output of leaves is learning_rate * max_delta_step, so param should reduce the effect
    val Array(train, test) = taskDF.randomSplit(Array(0.8, 0.2), seed)
    val baseModelWithLR = baseModel.setLearningRate(0.9).setNumIterations(100)
    val scoredDF1 = baseModelWithLR.fit(train).transform(test)
    val scoredDF2 = baseModelWithLR.setMaxDeltaStep(0.1).fit(train).transform(test)
    assertBinaryImprovement(scoredDF1, scoredDF2)
  }

  test("Verify LightGBM Classifier with weight column") {
    val model = baseModel.setWeightCol(weightCol)

    val df = pimaDF.withColumn(weightCol, lit(1.0))
    val dfWeight = df.withColumn(weightCol, when(col(labelCol) >= 1, 100.0).otherwise(1.0))

    def countPredictions(df: DataFrame): Long = {
      model.fit(df).transform(df).where(col("prediction") === 1.0).count()
    }

    // Verify changing weight of one label significantly skews the results
    val constLabelPredictionCount = countPredictions(df)
    assert(constLabelPredictionCount * 2 < countPredictions(dfWeight))

    // Also validate with int and long values for weight column predictions are the same within some threshold
    val threshold = 0.2 * constLabelPredictionCount
    val dfInt = pimaDF.withColumn(weightCol, lit(1))
    assert(math.abs(constLabelPredictionCount - countPredictions(dfInt)) < threshold)
    val dfLong = pimaDF.withColumn(weightCol, lit(1L))
    assert(math.abs(constLabelPredictionCount - countPredictions(dfLong)) < threshold)
  }

  test("Verify LightGBM Classifier with unbalanced dataset") {
    val Array(train, test) = taskDF.randomSplit(Array(0.8, 0.2), seed)
    assertBinaryImprovement(
      baseModel, train, test,
      baseModel.setIsUnbalance(true), train, test
    )
  }

  test("Verify LightGBM Classifier with validation dataset") {
    val df = taskDF.orderBy(rand()).withColumn(validationCol, lit(false))

    val Array(train, validIntermediate, test) = df.randomSplit(Array(0.1, 0.6, 0.3), seed)
    val valid = validIntermediate.withColumn(validationCol, lit(true))
    val trainAndValid = train.union(valid.orderBy(rand()))

    // model1 should overfit on the given dataset
    val model1 = baseModel
      .setNumLeaves(100)
      .setNumIterations(200)
      .setIsUnbalance(true)

    // model2 should terminate early before overfitting
    val model2 = baseModel
      .setNumLeaves(100)
      .setNumIterations(200)
      .setIsUnbalance(true)
      .setValidationIndicatorCol(validationCol)
      .setEarlyStoppingRound(5)

    // Assert evaluation metric improves
    Array("auc", "binary_logloss", "binary_error").foreach { metric =>
      assertBinaryImprovement(
        model1, train, test,
        model2.setMetric(metric), trainAndValid, test
      )
    }
  }

  test("Verify LightGBM Classifier categorical parameter") {
    val Array(train, test) = indexedBankTrainDF.randomSplit(Array(0.8, 0.2), seed)
    val untrainedModel = baseModel
      .setCategoricalSlotNames(indexedBankTrainDF.columns.filter(_.startsWith("c_")))
    val model = untrainedModel.fit(train)
    // Verify categorical features used in some tree in the model
    assert(model.getModel.model.contains("num_cat=1"))
    val metric = binaryEvaluator
      .evaluate(model.transform(test))
    // Verify we get good result
    assert(metric > 0.8)
  }

  test("Verify LightGBM Classifier with slot names parameter") {

    val binBankTrainDf: DataFrame = loadBinary("bank.train.csv", "y").cache()
    val categoricalColumns = Array("job", "marital", "education", "default", "housing", "loan", "contact")

    // define slot names that has a slot renamed pday to p_day
    val slotNames = Array("age", "job", "marital", "education", "default", "balance", "housing", "loan", "contact",
      "day", "month", "duration", "campaign", "p_days", "previous", "poutcome")

    val Array(train, test) = binBankTrainDf.randomSplit(Array(0.8, 0.2), seed)
    val untrainedModel = baseModel
      .setSlotNames(slotNames)
      .setCategoricalSlotNames(categoricalColumns)

    assert(untrainedModel.getSlotNames.length == slotNames.length)
    assert(untrainedModel.getSlotNames.contains("p_days"))

    val model = untrainedModel.fit(train)
    // Verify categorical features used in some tree in the model
    assert(model.getModel.model.contains("num_cat=1"))

    // Verify the p_days column that is renamed  used in some tree in the model
    assert(model.getModel.model.contains("p_days"))

    val metric = binaryEvaluator.evaluate(model.transform(test))

    // Verify we get good result
    assert(metric > 0.8)
  }

  test("Verify LightGBM Classifier won't get stuck on empty partitions") {
    val baseDF = pimaDF.select(labelCol, featuresCol)
    val df = baseDF.mapPartitions { rows =>
      // Create an empty partition
      if (TaskContext.get.partitionId == 0) {
        Iterator()
      } else {
        rows
      }
    }(RowEncoder(baseDF.schema))

    assertFitWithoutErrors(baseModel, df)
  }

  test("Verify LightGBM Classifier won't get stuck on unbalanced classes in multiclass classification") {
    val baseDF = breastTissueDF.select(labelCol, featuresCol)
    val df = baseDF.mapPartitions({ rows =>
      // Remove all instances of some classes
      if (TaskContext.get.partitionId == 1) {
        rows.filter(_.getInt(0) > 2)
      } else {
        rows
      }
    })(RowEncoder(baseDF.schema))

    val model = new LightGBMClassifier()
      .setLabelCol(labelCol)
      .setFeaturesCol(featuresCol)
      .setPredictionCol(predCol)
      .setDefaultListenPort(getAndIncrementPort())
      .setObjective(multiclassObject)

    // Validate fit works and doesn't get stuck
    assertFitWithoutErrors(model, df)
  }

  test("Verify LightGBM Classifier won't get stuck on unbalanced classes in binary classification") {
    val baseDF = pimaDF.select(labelCol, featuresCol)
    val df = baseDF.mapPartitions({ rows =>
      // Remove all instances of some classes
      if (TaskContext.get.partitionId == 1) {
        rows.filter(_.getInt(0) < 1)
      } else {
        rows
      }
    })(RowEncoder(baseDF.schema))

    // Validate fit works and doesn't get stuck
    assertFitWithoutErrors(baseModel, df)
  }

  def verifyLearnerOnBinaryCsvFile(fileName: String,
                                   labelColumnName: String,
                                   decimals: Int): Unit = {
    boostingTypes.foreach { boostingType =>
      test("Verify LightGBMClassifier can be trained " +
        s"and scored on $fileName with boosting type $boostingType", TestBase.Extended) {
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
      test(s"Verify LightGBMClassifier can be trained and scored " +
        s"on multiclass $fileName with boosting type $boostingType", TestBase.Extended) {
        val model = baseModel
          .setObjective(multiclassObject)
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
    test("Verify LightGBMClassifier save booster to " + fileName) {
      val model = baseModel
      val df = loadBinary(fileName, labelColumnName)
      val fitModel = model.fit(df)

      val targetDir: Path = Paths.get(getClass.getResource("/").toURI)
      val modelPath = targetDir.toString + "/" + outputFileName
      FileUtils.deleteDirectory(new File(modelPath))
      fitModel.saveNativeModel(modelPath, overwrite = true)
      assert(Files.exists(Paths.get(modelPath)), true)

      val oldModelString = fitModel.getModel.model
      // Verify model string contains some feature
      colsToVerify.foreach(col => oldModelString.contains(col))

      assertFitWithoutErrors(model.setModelString(oldModelString), df)

      // Verify can load model from file
      val resultsFromString = LightGBMClassificationModel
        .loadNativeModelFromString(oldModelString, labelColumnName, featuresCol, rawPredictionColName = rawPredCol)
        .transform(df)

      val resultsFromFile = LightGBMClassificationModel.
        loadNativeModelFromFile(modelPath, labelColumnName, featuresCol, rawPredictionColName = rawPredCol)
        .transform(df)

      val resultsOriginal = fitModel.transform(df)

      assert(resultsFromString === resultsOriginal)
      assert(resultsFromFile === resultsOriginal)
    }
  }

  override def reader: MLReadable[_] = LightGBMClassifier

  override def modelReader: MLReadable[_] = LightGBMClassificationModel
}
