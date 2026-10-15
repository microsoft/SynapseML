// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMClassifier, LightGBMConstants, LightGBMRegressor}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, isnan, lit, sin, when}

class VerifyLightGBMDatasetParams extends TestBase with LightGBMTestUtils {
  private class DatasetParameterProbe extends LightGBMClassifier {
    def datasetCreationParams: String = getDatasetCreationParams(Array.empty[Int], 1)
  }

  private def twoFeatureData: DataFrame = {
    val rows = Seq(
      (0.0, Vectors.dense(1.0, 2.0)),
      (1.0, Vectors.dense(2.0, 3.0)),
      (0.0, Vectors.dense(3.0, 4.0)),
      (1.0, Vectors.dense(4.0, 5.0)))
    spark.createDataFrame(rows).toDF(labelCol, featuresCol)
  }

  private def classifier: LightGBMClassifier = new LightGBMClassifier()
    .setFeaturesCol(featuresCol)
    .setLabelCol(labelCol)
    .setDefaultListenPort(getAndIncrementPort())
    .setNumTasks(1)
    .setNumLeaves(2)
    .setNumIterations(1)
    .setDataTransferMode(LightGBMConstants.StreamingDataTransferMode)

  private def regressor(mode: String): LightGBMRegressor = new LightGBMRegressor()
    .setFeaturesCol(featuresCol)
    .setLabelCol(labelCol)
    .setDefaultListenPort(getAndIncrementPort())
    .setNumTasks(1)
    .setNumLeaves(31)
    .setNumIterations(20)
    .setMinDataInLeaf(1)
    .setDataTransferMode(mode)

  private def thresholdCount(model: LightGBMRegressor, data: DataFrame): Int = {
    "(?m)^threshold=(.*)$".r.findAllMatchIn(model.fit(data).getNativeModel)
      .flatMap(_.group(1).split(" ")).toSet.size
  }

  private def missingTypes(model: LightGBMRegressor, data: DataFrame): Set[Int] = {
    "(?m)^decision_type=(.*)$".r.findAllMatchIn(model.fit(data).getNativeModel)
      .flatMap(_.group(1).split(" ")).map(value => (value.toInt >> 2) & 3).toSet
  }

  test("Verify explicitly set Dataset parameters reach Dataset creation") {
    val defaultParams = new DatasetParameterProbe().datasetCreationParams.split(" ").toSet
    val configuredParams = new DatasetParameterProbe()
      .setMaxBinByFeature(Array(2, 3))
      .setMinDataPerBin(7)
      .setIsEnableSparse(false)
      .setUseMissing(false)
      .setZeroAsMissing(true)
      .datasetCreationParams.split(" ").toSet

    assert(!defaultParams.exists(_.startsWith("max_bin_by_feature=")))
    assert(!defaultParams.exists(_.startsWith("min_data_in_bin=")))
    assert(!defaultParams.exists(_.startsWith("is_enable_sparse=")))
    assert(!defaultParams.exists(_.startsWith("use_missing=")))
    assert(!defaultParams.exists(_.startsWith("zero_as_missing=")))
    assert(configuredParams.contains("max_bin_by_feature=2,3"))
    assert(configuredParams.contains("min_data_in_bin=7"))
    assert(configuredParams.contains("is_enable_sparse=false"))
    assert(configuredParams.contains("use_missing=false"))
    assert(configuredParams.contains("zero_as_missing=true"))
  }

  test("Verify maxBinByFeature rejects a value count that differs from the feature count") {
    val error = intercept[IllegalArgumentException] {
      classifier.setMaxBinByFeature(Array(2)).fit(twoFeatureData)
    }
    assert(error.getMessage.contains("expected 2 but found 1"))
  }

  test("Verify maxBinByFeature rejects values less than two") {
    val error = intercept[IllegalArgumentException] {
      classifier.setMaxBinByFeature(Array(2, 1)).fit(twoFeatureData)
    }
    assert(error.getMessage.contains("must be greater than 1"))
  }

  test("Verify Dataset binning parameters affect bulk and streaming training") {
    val scalarData = spark.range(10000)
      .withColumn("x", (col("id") % 1000) / 1000.0)
      .withColumn(labelCol, sin(col("x") * 6.28))
    val data = new VectorAssembler().setInputCols(Array("x")).setOutputCol(featuresCol)
      .transform(scalarData).repartition(1)

    Seq(LightGBMConstants.BulkDataTransferMode, LightGBMConstants.StreamingDataTransferMode)
      .foreach(mode => {
        val perFeature = thresholdCount(regressor(mode).setMaxBinByFeature(Array(2)), data)
        val global = thresholdCount(regressor(mode).setMaxBin(2), data)
        val minPerBin = thresholdCount(regressor(mode).setMinDataPerBin(4000), data)
        assert(perFeature == global)
        assert(global == 1)
        assert(minPerBin <= 2)
      })
  }

  test("Verify missing-value Dataset parameters affect bulk and streaming training") {
    val scalarData = spark.range(10000)
      .withColumn("x", when(col("id") % 5 === 0, lit(Double.NaN))
        .when(col("id") % 5 === 1, lit(0.0))
        .otherwise((col("id") % 1000) / 1000.0 + 0.001))
      .withColumn(labelCol, when(isnan(col("x")), lit(3.0)).when(col("x") === 0.0, lit(-3.0))
        .otherwise(sin(col("x") * 6.28)))
    val data = new VectorAssembler().setInputCols(Array("x")).setOutputCol(featuresCol)
      .setHandleInvalid("keep").transform(scalarData).repartition(1)

    Seq(LightGBMConstants.BulkDataTransferMode, LightGBMConstants.StreamingDataTransferMode)
      .foreach(mode => {
        assert(missingTypes(regressor(mode), data) == Set(2))
        assert(missingTypes(regressor(mode).setUseMissing(false), data) == Set(0))
        assert(missingTypes(regressor(mode).setZeroAsMissing(true), data) == Set(1))
      })
  }
}
