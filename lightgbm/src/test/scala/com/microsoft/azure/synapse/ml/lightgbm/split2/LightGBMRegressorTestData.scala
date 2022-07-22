// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split2

import com.microsoft.azure.synapse.ml.core.test.benchmarks.{Benchmarks, DatasetUtils}
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import com.microsoft.azure.synapse.ml.lightgbm.split1.LightGBMTestUtils
import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMRegressionModel, LightGBMRegressor, LightGBMUtils}
import com.microsoft.azure.synapse.ml.stages.MultiColumnAdapter
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame

// scalastyle:off magic.number

/** Tests to validate the functionality of LightGBM module.
  */
class LightGBMRegressorTestData extends Benchmarks
  with EstimatorFuzzing[LightGBMRegressor] with LightGBMTestUtils {
  override val startingPortIndex = 30

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

  val verifyLearnerTitleTemplate: String = "Verify LightGBMRegressor can be trained and scored on %s, %s mode"
  val energyEffFile: String = "energyefficiency2012_data.train.csv"
  val airfoilFile: String = "airfoil_self_noise.train.csv"
  val tomsHardwareFile: String = "Buzz.TomsHardware.train.csv"
  val machineFile: String = "machine.train.csv"
  val slumpFile: String = "slump_test.train.csv"
  val concreteFile: String = "Concrete_Data.train.csv"

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

  def verifyLearnerOnRegressionCsvFile(fileName: String,
                                       labelCol: String,
                                       decimals: Int,
                                       columnsFilter: Option[Seq[String]] = None): Unit = {
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

  override def testObjects(): Seq[TestObject[LightGBMRegressor]] = {
    val labelCol = "ERP"
    val featuresCol = "feature"
    val dataset = readCSV(DatasetUtils.regressionTrainFile("machine.train.csv").toString)
    val featurizer = LightGBMUtils.getFeaturizer(dataset, labelCol, featuresCol)
    val train = featurizer.transform(dataset)

    Seq(new TestObject(new LightGBMRegressor().setDefaultListenPort(getAndIncrementPort())
      .setLabelCol(labelCol).setFeaturesCol(featuresCol).setNumLeaves(5),
      train))
  }

  override def reader: MLReadable[_] = LightGBMRegressor

  override def modelReader: MLReadable[_] = LightGBMRegressionModel
}
