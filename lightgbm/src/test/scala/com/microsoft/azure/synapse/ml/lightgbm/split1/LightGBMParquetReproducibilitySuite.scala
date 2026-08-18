// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.lightgbm._
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

// scalastyle:off magic.number
class LightGBMParquetReproducibilitySuite extends LightGBMTestUtils {

  private class ReproducibilityProbe extends LightGBMClassifier {
    def warningMessages(dataset: DataFrame): Seq[String] = reproducibilityWarningMessages(dataset)
  }

  private def makeTrainingData(): DataFrame = {
    val source = spark.range(0, 500, 1, 4)
      .withColumn(labelCol, when(col("id") % 11 === 0, 1.0).otherwise(0.0))
      .withColumn(weightCol, when(col(labelCol) === 1.0, 2.0).otherwise(1.0))
      .withColumn(initScoreCol, lit(0.0))
      .withColumn("category", concat(lit("c"), (col("id") % 3).cast("string")))
      .withColumn("f0", when(col("id") % 2 === 0, (col("id") % 17).cast("double")).otherwise(0.0))
      .withColumn("f1", when(col(labelCol) === 1.0, 1.0).otherwise(0.0))
      .withColumn("f2", when(col("id") % 5 === 0, 1.0).otherwise(0.0))
      .withColumn("f3", when(col("id") % 7 === 0, 1.0).otherwise(0.0))

    val indexed = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .fit(source)
      .transform(source)
    new VectorAssembler()
      .setInputCols(Array("categoryIndex", "f0", "f1", "f2", "f3"))
      .setOutputCol(featuresCol)
      .transform(indexed)
      .select("id", labelCol, weightCol, initScoreCol, featuresCol)
      .orderBy(rand())
  }

  private def estimator: LightGBMClassifier = {
    new LightGBMClassifier()
      .setFeaturesCol(featuresCol)
      .setLabelCol(labelCol)
      .setWeightCol(weightCol)
      .setInitScoreCol(initScoreCol)
      .setRawPredictionCol(rawPredCol)
      .setNumIterations(20)
      .setNumLeaves(16)
      .setLearningRate(0.1)
      .setBaggingFraction(0.9)
      .setBaggingFreq(1)
      .setFeatureFraction(0.8)
      .setSeed(777)
      .setBaggingSeed(521)
      .setDataRandomSeed(777)
      .setDeterministic(true)
      .setNumTasks(2)
      .setNumThreads(1)
      .setDefaultListenPort(getAndIncrementPort())
      .setUseBarrierExecutionMode(true)
      .setSamplingMode(LightGBMConstants.SubsetSamplingModeGlobal)
      .setDataTransferMode(LightGBMConstants.StreamingDataTransferMode)
  }

  private def auc(model: LightGBMClassificationModel, data: DataFrame): Double = {
    new BinaryClassificationEvaluator()
      .setLabelCol(labelCol)
      .setRawPredictionCol(rawPredCol)
      .setMetricName("areaUnderROC")
      .setNumBins(0)
      .evaluate(model.transform(data))
  }

  private def assertOnlySparkLineageWarning(warnings: Seq[String]): Unit = {
    assert(warnings.exists(_.contains("nondeterministic Spark expressions")))
    assert(!warnings.exists(_.contains("histogram strategy")))
  }

  test("Deterministic training warns about Spark lineage and LightGBM histogram selection") {
    val assembled = makeTrainingData()
    assert(!assembled.queryExecution.optimizedPlan.deterministic)

    val probe = new ReproducibilityProbe()
      .setDeterministic(true)
    val warnings = probe.warningMessages(assembled)
    assert(warnings.exists(_.contains("nondeterministic Spark expressions")))
    assert(warnings.exists(_.contains("force_col_wise=true or force_row_wise=true")))

    val forcedProbe = new ReproducibilityProbe()
      .setDeterministic(true)
      .setPassThroughArgs("force_col_wise=true")
    val forcedWarnings = forcedProbe.warningMessages(assembled)
    assert(forcedWarnings.exists(_.contains("nondeterministic Spark expressions")))
    assert(!forcedWarnings.exists(_.contains("does not by itself select a stable LightGBM histogram strategy")))

    val conflictingProbe = new ReproducibilityProbe()
      .setDeterministic(true)
      .setPassThroughArgs("force_col_wise=true force_row_wise=true")
    assert(conflictingProbe.warningMessages(assembled)
      .exists(_.contains("Both force_col_wise and force_row_wise are enabled")))

    val passThroughProbe = new ReproducibilityProbe()
      .setPassThroughArgs("deterministic=true force_col_wise=true")
    assert(passThroughProbe.warningMessages(assembled)
      .exists(_.contains("nondeterministic Spark expressions")))

    val overriddenProbe = new ReproducibilityProbe()
      .setDeterministic(true)
      .setPassThroughArgs("deterministic=false")
    assert(overriddenProbe.warningMessages(assembled).isEmpty)

    val gpuProbe = new ReproducibilityProbe()
      .setDeterministic(true)
      .setDeviceType(LightGBMConstants.GPUDeviceType)
    assertOnlySparkLineageWarning(gpuProbe.warningMessages(assembled))

    val cudaProbe = new ReproducibilityProbe()
      .setDeterministic(true)
      .setPassThroughArgs("device=cuda")
    assertOnlySparkLineageWarning(cudaProbe.warningMessages(assembled))

    val passThroughCpuProbe = new ReproducibilityProbe()
      .setDeterministic(true)
      .setDeviceType(LightGBMConstants.GPUDeviceType)
      .setPassThroughArgs("device_type=cpu")
    val passThroughCpuWarnings = passThroughCpuProbe.warningMessages(assembled)
    assert(passThroughCpuWarnings.exists(_.contains("nondeterministic Spark expressions")))
    assert(passThroughCpuWarnings.exists(_.contains("histogram strategy")))

    val passThroughGpuProbe = new ReproducibilityProbe()
      .setDeterministic(true)
      .setPassThroughArgs("device_type=gpu")
    assertOnlySparkLineageWarning(passThroughGpuProbe.warningMessages(assembled))
  }

  test("Parquet round trips preserve deterministic LightGBM training inputs and outputs") {
    val assembled = makeTrainingData().persist(StorageLevel.MEMORY_AND_DISK)
    try {
      assert(assembled.count() === 500)

      val parquetPath = tmpDir.resolve("lightgbm-parquet-reproducibility").toString
      assembled.write.mode("overwrite").parquet(parquetPath)
      val reloaded = spark.read.parquet(parquetPath)

      val memoryRows = assembled.orderBy("id").collect()
      val parquetRows = reloaded.orderBy("id").collect()
      assert(memoryRows.sameElements(parquetRows))
      assert(assembled.schema.map(field => field.name -> field.metadata.json) ===
        reloaded.schema.map(field => field.name -> field.metadata.json))
      assert(AttributeGroup.fromStructField(assembled.schema(featuresCol)) ===
        AttributeGroup.fromStructField(reloaded.schema(featuresCol)))
      assert(assembled.select(featuresCol).collect().map(_.getAs[Vector](0).getClass.getName).toSet ===
        reloaded.select(featuresCol).collect().map(_.getAs[Vector](0).getClass.getName).toSet)

      val forcedEstimator = estimator.setPassThroughArgs("force_col_wise=true")
      val memoryModels = Array.fill(2)(forcedEstimator.fit(assembled))
      val parquetModels = Array.fill(2)(forcedEstimator.fit(reloaded))
      val nativeModels = (memoryModels ++ parquetModels).map(_.getNativeModel())
      assert(nativeModels.distinct.length === 1)

      val predictionColumns = Seq("id", rawPredCol, "probability", "prediction")
      val predictions = (memoryModels ++ parquetModels).map { model =>
        model.transform(reloaded).select(predictionColumns.map(col): _*).orderBy("id").collect()
      }
      assert(predictions.tail.forall(_.sameElements(predictions.head)))

      val aucValues = memoryModels.map(auc(_, assembled)) ++ parquetModels.map(auc(_, reloaded))
      assert(aucValues.max - aucValues.min < 1e-12)
    } finally {
      assembled.unpersist()
    }
  }
}
