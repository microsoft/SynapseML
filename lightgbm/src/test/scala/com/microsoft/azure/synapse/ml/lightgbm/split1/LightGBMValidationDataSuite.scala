// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.core.test.base.{SparkSessionManagement, TestBase}
import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMClassifier, LightGBMClassificationModel, LightGBMConstants}
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.functions.{col, lit, udf}

import java.io.File
import java.util.UUID

// scalastyle:off magic.number
class LightGBMValidationDataSuite extends TestBase {
  private object SmallResultSparkProvider extends SparkSessionManagement {
    override def sparkConfiguration: SparkConf = {
      super.sparkConfiguration
        .set("spark.driver.maxResultSize", "256k")
        .set("spark.sql.shuffle.partitions", "8")
    }
  }

  override lazy val sparkProvider: SparkSessionManagement = SmallResultSparkProvider

  override protected def afterAll(): Unit = {
    try super.afterAll()
    finally SmallResultSparkProvider.stopSparkSession()
  }

  test("validationIndicatorCol does not collect sparse validation rows on the driver") {
    val featureCount = 4096
    val nonZeroCount = 256
    val rowCount = 16000L
    val partitionCount = 8
    val validationCol = "isValidation"
    val featuresCol = "features"
    val labelCol = "label"

    val sparseFeatures = udf { id: Long =>
      val start = (id % (featureCount - nonZeroCount)).toInt
      val indices = Array.tabulate(nonZeroCount)(offset => start + offset)
      val values = Array.fill(nonZeroCount)(1.0)
      Vectors.sparse(featureCount, indices, values)
    }

    val data = spark.range(0L, rowCount, 1L, partitionCount)
      .select(
        (col("id") % 2).cast("double").as(labelCol),
        sparseFeatures(col("id")).as(featuresCol),
        (col("id") % 4 === 0).as(validationCol))

    val estimator = new LightGBMClassifier()
      .setLabelCol(labelCol)
      .setFeaturesCol(featuresCol)
      .setValidationIndicatorCol(validationCol)
      .setNumTasks(partitionCount)
      .setNumIterations(2)
      .setNumLeaves(4)
      .setMinDataInLeaf(1)
      .setBinSampleCount(128)
      .setDefaultListenPort(12400)

    val copied = estimator.copy(ParamMap.empty)
    assert(copied.getValidationIndicatorCol == validationCol)

    val estimatorPath = scratchPath("estimator")
    val modelPath = scratchPath("model")
    try {
      estimator.write.overwrite().save(estimatorPath.toString)
      val loadedEstimator = LightGBMClassifier.load(estimatorPath.toString)
      assert(loadedEstimator.getValidationIndicatorCol == validationCol)

      val model = loadedEstimator.fit(data)
      assert(validationSpoolDirectories.isEmpty)
      val transformed = model.transform(data)
      assert(transformed.schema.fieldNames.toSet == model.transformSchema(data.schema).fieldNames.toSet)
      assert(transformed.select("prediction").count() == rowCount)

      model.write.overwrite().save(modelPath.toString)
      val loadedModel = LightGBMClassificationModel.load(modelPath.toString)
      val prediction = loadedModel.transform(data.limit(1)).select("prediction").head().getDouble(0)
      assert(prediction == 0.0 || prediction == 1.0)
      assert(data.schema(featuresCol).dataType.typeName == "vector")
      assert(data.select(featuresCol).head().getAs[Vector](0).numNonzeros == nonZeroCount)
    } finally {
      FileUtils.deleteDirectory(estimatorPath)
      FileUtils.deleteDirectory(modelPath)
    }
  }

  test("bulk mode streams complete validation data to every training task") {
    val partitionCount = 4
    val sparseFeatures = udf { id: Long =>
      Vectors.sparse(64, Array((id % 64).toInt), Array(1.0))
    }
    val data = spark.range(0L, 512L, 1L, partitionCount)
      .select(
        (col("id") % 2).cast("double").as("label"),
        sparseFeatures(col("id")).as("features"),
        (col("id") % 4 === 0).as("isValidation"))

    val model = new LightGBMClassifier()
      .setValidationIndicatorCol("isValidation")
      .setDataTransferMode(LightGBMConstants.BulkDataTransferMode)
      .setUseSingleDatasetMode(false)
      .setNumTasks(partitionCount)
      .setNumIterations(2)
      .setNumLeaves(4)
      .setMinDataInLeaf(1)
      .setBinSampleCount(64)
      .setDefaultListenPort(12500)
      .fit(data)

    assert(model.transform(data).select("prediction").count() == 512L)
    assert(validationSpoolDirectories.isEmpty)
  }

  test("validationIndicatorCol rejects null indicators instead of dropping rows") {
    val denseFeatures = udf { id: Long => Vectors.dense(id.toDouble) }
    val data = spark.range(0L, 2L, 1L, 2)
      .select(
        col("id").cast("double").as("label"),
        denseFeatures(col("id")).as("features"),
        lit(null).cast("boolean").as("isValidation")) // scalastyle:ignore null

    val error = intercept[IllegalArgumentException] {
      new LightGBMClassifier()
        .setValidationIndicatorCol("isValidation")
        .setNumTasks(2)
        .fit(data)
    }
    assert(error.getMessage.contains("contains null"))
    assert(validationSpoolDirectories.isEmpty)
  }

  private def scratchPath(name: String): File = {
    new File(System.getProperty("user.dir"), s".synapseml-lightgbm-validation-$name-${UUID.randomUUID()}")
  }

  private def validationSpoolDirectories: Array[File] = {
    Option(new File(System.getProperty("user.dir")).listFiles())
      .getOrElse(Array.empty)
      .filter(_.getName.startsWith(".synapseml-lightgbm-validation-spool-"))
  }
}
