// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm.split2

import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.core.test.benchmarks.{Benchmarks, DatasetUtils}
import com.microsoft.ml.spark.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import com.microsoft.ml.spark.lightgbm.split1.LightGBMTestUtils
import com.microsoft.ml.spark.lightgbm.{LightGBMRegressionModel, LightGBMRegressor, LightGBMUtils}
import com.microsoft.ml.spark.stages.MultiColumnAdapter
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{avg, col, lit, when}

// scalastyle:off magic.number
/** Tests to validate the functionality of LightGBM module.
  */
class VerifyLightGBMRegressor extends Benchmarks
  with EstimatorFuzzing[LightGBMRegressor] with LightGBMTestUtils {
  override val startingPortIndex = 30

  verifyLearnerOnRegressionCsvFile("energyefficiency2012_data.train.csv", "Y1", 0,
    Some(Seq("X1", "X2", "X3", "X4", "X5", "X6", "X7", "X8", "Y2")))
  verifyLearnerOnRegressionCsvFile("airfoil_self_noise.train.csv", "Scaled sound pressure level", 1)
  verifyLearnerOnRegressionCsvFile("Buzz.TomsHardware.train.csv", "Mean Number of display (ND)", -3)
  verifyLearnerOnRegressionCsvFile("machine.train.csv", "ERP", -2)
  // TODO: Spark doesn't seem to like the column names here because of '.', figure out how to read in the data
  // verifyLearnerOnRegressionCsvFile("slump_test.train.csv", "Compressive Strength (28-day)(Mpa)", 2)
  verifyLearnerOnRegressionCsvFile("Concrete_Data.train.csv", "Concrete compressive strength(MPa, megapascals)", 0)

  override def testExperiments(): Unit = {
    super.testExperiments()
  }

  override def testSerialization(): Unit = {
    super.testSerialization()
  }

  test("Compare benchmark results file to generated file") {
    verifyBenchmarks()
  }

  lazy val airfoilDF: DataFrame = loadRegression(
    "airfoil_self_noise.train.csv", "Scaled sound pressure level").cache()

  def baseModel: LightGBMRegressor = {
    new LightGBMRegressor()
      .setLabelCol(labelCol)
      .setFeaturesCol(featuresCol)
      .setDefaultListenPort(getAndIncrementPort())
      .setNumLeaves(5)
      .setNumIterations(10)
  }

  test("Verify LightGBM Regressor can be run with TrainValidationSplit") {
    val model = baseModel

    val paramGrid = new ParamGridBuilder()
      .addGrid(model.numLeaves, Array(5, 10))
      .addGrid(model.numIterations, Array(10, 20))
      .addGrid(model.lambdaL1, Array(0.1, 0.5))
      .addGrid(model.lambdaL2, Array(0.1, 0.5))
      .build()

    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(model)
      .setEvaluator(new RegressionEvaluator().setLabelCol(labelCol))
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)
      .setParallelism(2)

    val fitModel = trainValidationSplit.fit(airfoilDF)
    fitModel.transform(airfoilDF)
    assert(fitModel != null)

    // Validate lambda parameters set on model
    val modelStr = fitModel.bestModel.asInstanceOf[LightGBMRegressionModel].getModel.modelStr.get
    assert(modelStr.contains("[lambda_l1: 0.1]") || modelStr.contains("[lambda_l1: 0.5]"))
    assert(modelStr.contains("[lambda_l2: 0.1]") || modelStr.contains("[lambda_l2: 0.5]"))
  }

  test("Verify LightGBM with single dataset mode") {
    val df = airfoilDF
    val model = baseModel.setUseSingleDatasetMode(true)
    model.fit(df).transform(df).show()
  }

  test("Verify LightGBM Regressor with weight column") {
    val df = airfoilDF.withColumn(weightCol, lit(1.0))

    val model = baseModel.setWeightCol(weightCol)
    val dfWeight = df.withColumn(weightCol, when(col(labelCol) > 120, 1000.0).otherwise(1.0))

    def avgPredictions(df: DataFrame): Double = {
      model.fit(df).transform(df).select(avg("prediction")).first().getDouble(0)
    }

    assert(avgPredictions(df) < avgPredictions(dfWeight))
  }

  lazy val flareDF: DataFrame = {
    val categoricalColumns = Array("Zurich class", "largest spot size", "spot distribution")

    val newCategoricalColumns: Array[String] = categoricalColumns.map("c_" + _)
    val df = readCSV(DatasetUtils.regressionTrainFile("flare.data1.train.csv").toString)
      .repartition(numPartitions)

    val df2 = new MultiColumnAdapter().setInputCols(categoricalColumns).setOutputCols(newCategoricalColumns)
      .setBaseStage(new StringIndexer())
      .fit(df)
      .transform(df).drop(categoricalColumns: _*)
      .withColumnRenamed("M-class flares production by this region", labelCol)

    LightGBMUtils.getFeaturizer(df2, labelCol, featuresCol).transform(df2)
    }.cache()

  def regressionEvaluator: RegressionEvaluator = {
    new RegressionEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol(predCol)
      .setMetricName("rmse")
  }

  test("Verify LightGBM Regressor categorical parameter") {
    val Array(train, test) = flareDF.randomSplit(Array(0.8, 0.2), seed.toLong)
    val model = baseModel.setCategoricalSlotNames(flareDF.columns.filter(_.startsWith("c_")))
    val metric = regressionEvaluator.evaluate(model.fit(train).transform(test))

    // Verify we get good result
    assert(metric < 0.6)
  }

  test("Verify LightGBM Regressor with bad column names fails early") {
    val baseModelWithBadSlots = baseModel.setSlotNames(Range(0, 22).map(i =>
      "Invalid characters \",:[]{} " + i).toArray)
    interceptWithoutLogging[IllegalArgumentException]{baseModelWithBadSlots.fit(flareDF).transform(flareDF).collect()}
  }

  test("Verify LightGBM Regressor with tweedie distribution") {
    val model = baseModel.setObjective("tweedie").setTweedieVariancePower(1.5)

    val paramGrid = new ParamGridBuilder()
      .addGrid(model.tweedieVariancePower, Array(1.0, 1.2, 1.4, 1.6, 1.8, 1.99))
      .build()

    val cv = new CrossValidator()
      .setEstimator(model)
      .setEvaluator(new RegressionEvaluator().setLabelCol(labelCol))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
      .setParallelism(2)

    // Choose the best model for tweedie distribution
    assertFitWithoutErrors(cv, airfoilDF)
  }

  test("Verify LightGBM Regressor features shap") {
    val Array(train, test) = flareDF.randomSplit(Array(0.8, 0.2), seed)
    val untrainedModel = baseModel
      .setFeaturesShapCol(featuresShapCol)
      .setCategoricalSlotNames(flareDF.columns.filter(_.startsWith("c_")))
    val model = untrainedModel.fit(train)

    val evaluatedDf = model.transform(test)

    val featuresShap: Array[Double] = evaluatedDf.select(featuresShapCol).rdd.map {
      case Row(v: Vector) => v
    }.first.toArray

    assert(featuresShap.length == (model.getModel.numFeatures + 1))

    // if featuresShap is not wanted, it is possible to remove it.
    val evaluatedDf2 = model.setFeaturesShapCol("").transform(test)
    assert(!evaluatedDf2.columns.contains(featuresShapCol))
  }

  def verifyLearnerOnRegressionCsvFile(fileName: String,
                                       labelCol: String,
                                       decimals: Int,
                                       columnsFilter: Option[Seq[String]] = None): Unit = {
    test(s"Verify LightGBMRegressor can be trained " +
      s"and scored on $fileName ") {
      lazy val df = loadRegression(fileName, labelCol, columnsFilter).cache()
      boostingTypes.foreach { boostingType =>

        val model = baseModel.setBoostingType(boostingType)

        if (boostingType == "rf") {
          model.setBaggingFraction(0.9)
          model.setBaggingFreq(1)
        }

        val fitModel = model.fit(df)
        assertImportanceLengths(fitModel, df)
        addBenchmark(s"LightGBMRegressor_${fileName}_$boostingType",
          regressionEvaluator.evaluate(fitModel.transform(df)), decimals, higherIsBetter = false)
      }
      df.unpersist()
    }
  }

  override def testObjects(): Seq[TestObject[LightGBMRegressor]] = {
    val labelCol = "ERP"
    val featuresCol = "feature"
    val dataset = readCSV(DatasetUtils.regressionTrainFile("machine.train.csv").toString)
    val featurizer = LightGBMUtils.getFeaturizer(dataset, labelCol, featuresCol)
    val train = featurizer.transform(dataset)

    Seq(new TestObject(new LightGBMRegressor()
      .setLabelCol(labelCol).setFeaturesCol(featuresCol).setNumLeaves(5),
      train))
  }

  override def reader: MLReadable[_] = LightGBMRegressor

  override def modelReader: MLReadable[_] = LightGBMRegressionModel
}
