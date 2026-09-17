// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.core.test.base.{SparkSessionManagement, TestBase}
import com.microsoft.azure.synapse.ml.lightgbm.{BulkPartitionTask, LightGBMClassifier, LightGBMClassificationModel}
import com.microsoft.azure.synapse.ml.lightgbm.LightGBMConstants
import com.microsoft.azure.synapse.ml.lightgbm.LightGBMRanker
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.functions.{col, lit, udf}

import java.io.File
import java.util.UUID

// scalastyle:off magic.number
class LightGBMValidationDataSuite extends LightGBMTestUtils {
  private object SmallResultSparkProvider extends SparkSessionManagement {
    override def sparkConfiguration: SparkConf = {
      super.sparkConfiguration
        .set("spark.driver.maxResultSize", "128k")
        .set("spark.sql.shuffle.partitions", "8")
    }
  }

  override lazy val sparkProvider: SparkSessionManagement = SmallResultSparkProvider

  override protected def beforeAll(): Unit = {
    TestBase.stopSparkSession()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try super.afterAll()
    finally {
      try SmallResultSparkProvider.stopSparkSession()
      finally TestBase.resetSparkSession()
    }
  }

  test("validation scaling tests use the bounded driver result configuration") {
    assert(spark.sparkContext.getConf.get("spark.driver.maxResultSize") == "128k")
  }

  test("bulk single dataset mode reads validation data only on the active task") {
    val task = new BulkPartitionTask
    assert(task.shouldReadValidationData(useSingleDatasetMode = true, shouldExecuteTraining = true))
    assert(!task.shouldReadValidationData(useSingleDatasetMode = true, shouldExecuteTraining = false))
    assert(task.shouldReadValidationData(useSingleDatasetMode = false, shouldExecuteTraining = true))
    validateBulkMode(useSingleDatasetMode = true)
  }

  test("validationIndicatorCol does not collect sparse validation rows on the driver") {
    val featureCount = 4096
    val nonZeroCount = 1024
    val rowCount = 256L
    val partitionCount = 2
    val validationCol = "isValidation"
    val featuresCol = "features"
    val labelCol = "label"

    val sparseFeatures = udf { id: Long =>
      val start = (id % (featureCount - nonZeroCount)).toInt
      val indices = Array.tabulate(nonZeroCount)(offset => start + offset)
      val values = Array.tabulate(nonZeroCount) { offset =>
        (((id * 104729L + offset * 13007L) % 1000003L) + 1L).toDouble / 1000004.0
      }
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
      .setBinSampleCount(8)
      .setDefaultListenPort(getAndIncrementPort())

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
    validateBulkMode(useSingleDatasetMode = false)
  }

  test("ranker streams grouped validation data after algorithm preprocessing") {
    import spark.implicits._

    val data = Seq(
      (0L, 3.0, Vectors.dense(0.9, 0.1), false),
      (0L, 2.0, Vectors.dense(0.7, 0.3), false),
      (0L, 1.0, Vectors.dense(0.4, 0.6), false),
      (1L, 2.0, Vectors.dense(0.8, 0.2), false),
      (1L, 1.0, Vectors.dense(0.5, 0.5), false),
      (1L, 0.0, Vectors.dense(0.2, 0.8), false),
      (2L, 3.0, Vectors.dense(0.95, 0.05), true),
      (2L, 1.0, Vectors.dense(0.6, 0.4), true),
      (2L, 0.0, Vectors.dense(0.1, 0.9), true),
      (3L, 2.0, Vectors.dense(0.85, 0.15), true),
      (3L, 1.0, Vectors.dense(0.55, 0.45), true),
      (3L, 0.0, Vectors.dense(0.15, 0.85), true)
    ).toDF("query", "label", "features", "isValidation")

    val model = new LightGBMRanker()
      .setGroupCol("query")
      .setValidationIndicatorCol("isValidation")
      .setDataTransferMode(LightGBMConstants.BulkDataTransferMode)
      .setRepartitionByGroupingColumn(false)
      .setNumTasks(1)
      .setNumIterations(3)
      .setEarlyStoppingRound(1)
      .setNumLeaves(4)
      .setMinDataInLeaf(1)
      .setBinSampleCount(8)
      .setDefaultListenPort(getAndIncrementPort())
      .fit(data)

    assert(model.transform(data).select("prediction").count() == data.count())
    assert(validationSpoolDirectories.isEmpty)
  }

  test("validationIndicatorCol rejects null indicators instead of dropping rows") {
    val denseFeatures = udf { id: Long =>
      val random = new scala.util.Random(id)
      Vectors.dense(Array.fill(128 * 1024)(random.nextDouble()))
    }
    val data = spark.range(0L, 2L, 1L, 2)
      .select(
        col("id").cast("double").as("label"),
        denseFeatures(col("id")).as("features"),
        lit(null).cast("boolean").as("isValidation")) // scalastyle:ignore null

    val error = intercept[Exception] {
      new LightGBMClassifier()
        .setValidationIndicatorCol("isValidation")
        .setNumTasks(2)
        .fit(data)
    }
    assert(error.getMessage.contains("contains null"))
    assert(validationSpoolDirectories.isEmpty)
  }

  private def validateBulkMode(useSingleDatasetMode: Boolean): Unit = {
    val partitionCount = 2
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
      .setUseSingleDatasetMode(useSingleDatasetMode)
      .setNumTasks(partitionCount)
      .setNumIterations(2)
      .setNumLeaves(4)
      .setMinDataInLeaf(1)
      .setBinSampleCount(64)
      .setDefaultListenPort(getAndIncrementPort())
      .fit(data)

    assert(model.transform(data).select("prediction").count() == 512L)
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
