// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.lightgbm.{InstrumentationMeasures, LightGBMRegressionModel, LightGBMRegressor}
import com.microsoft.azure.synapse.ml.lightgbm.dataset.DatasetUtils
import org.apache.spark.TaskContext
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions.{col, first, lit, udf, when}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}

import java.io.File
import java.util.UUID
import java.util.concurrent.{Callable, ExecutionException, Executors, TimeUnit}

class StreamingFeaturePreflightSuite extends LightGBMTestUtils with Eventually {
  private val rowCount = 128L
  private val partitionCount = 16

  private def input(sparse: Boolean, validation: Boolean, malformed: Boolean): DataFrame = {
    val lastId = rowCount - 1
    val features = udf { id: Long =>
      val values = if (malformed && id == lastId) Array.empty[Double] else Array((id % 7 + 1).toDouble)
      val vector: Vector = if (sparse) Vectors.dense(values).toSparse else Vectors.dense(values)
      vector
    }
    spark.range(0L, rowCount, 1L, partitionCount).select(
      col("id"),
      (col("id") % 2).cast("double").as(labelCol),
      (lit(validation) && (col("id") % 5 === 0 || col("id") === lastId)).as(validationCol),
      features(col("id")).as(featuresCol))
  }

  private def learner(matrixType: String, validation: Boolean): LightGBMRegressor = {
    val estimator = new LightGBMRegressor()
      .setDefaultListenPort(getAndIncrementPort())
      .setLabelCol(labelCol)
      .setFeaturesCol(featuresCol)
      .setDataTransferMode("streaming")
      .setMatrixType(matrixType)
      .setNumTasks(partitionCount)
      .setNumThreads(1)
      .setMaxStreamingOMPThreads(1)
      .setSamplingMode("fixed")
      .setBinSampleCount(16)
      .setMicroBatchSize(8)
      .setMinDataPerBin(1)
      .setMinDataInLeaf(1)
      .setNumLeaves(4)
      .setNumIterations(1)
      .setTimeout(20)
    if (validation) estimator.setValidationIndicatorCol(validationCol) else estimator
  }

  private def assertRejected(estimator: LightGBMRegressor,
                             data: DataFrame,
                             message: String,
                             expectedTrainingStarted: Boolean): Unit = {
    val session = spark
    val group = s"streaming-feature-preflight-${UUID.randomUUID()}"
    val executor = Executors.newSingleThreadExecutor()
    val result = executor.submit(new Callable[LightGBMRegressionModel] {
      override def call(): LightGBMRegressionModel = {
        SparkSession.setActiveSession(session)
        session.sparkContext.setJobGroup(group, "Validate streaming feature dimensions", interruptOnCancel = true)
        try estimator.fit(data)
        finally {
          session.sparkContext.clearJobGroup()
          SparkSession.clearActiveSession()
        }
      }
    })
    try {
      val failure = intercept[ExecutionException](result.get(60, TimeUnit.SECONDS)).getCause
      assert(failure.getMessage.contains(message))
      eventually(timeout(Span(10, Seconds)), interval(Span(50, Millis))) {
        val tracker = session.sparkContext.statusTracker
        assert(tracker.getJobIdsForGroup(group).toSet.intersect(tracker.getActiveJobIds().toSet).isEmpty)
      }
      assert(estimator.getPerformanceMeasures.get.hasTrainingStarted == expectedTrainingStarted)
    } finally {
      session.sparkContext.cancelJobGroup(group)
      result.cancel(true)
      executor.shutdownNow()
      assert(executor.awaitTermination(30, TimeUnit.SECONDS))
    }
  }

  private def assertRejectedBeforeTraining(estimator: LightGBMRegressor,
                                           data: DataFrame,
                                           message: String): Unit = {
    assertRejected(estimator, data, message, expectedTrainingStarted = false)
  }

  private def validationSpools: Set[String] = {
    Option(new File(System.getProperty("user.dir")).listFiles()).getOrElse(Array.empty)
      .filter(_.getName.startsWith(".synapseml-lightgbm-validation-spool-")).map(_.getName).toSet
  }

  Seq("dense", "sparse").foreach { matrixType =>
    Seq(false, true).foreach { validation =>
      val source = if (validation) "validation" else "training"
      test(s"malformed $matrixType $source vectors reject before shared native preparation and permit another fit") {
        val data = input(matrixType == "sparse", validation, malformed = true).cache()
        val spoolsBefore = validationSpools
        try {
          assert(data.count() == rowCount)
          val estimator = learner(matrixType, validation)
          assertRejectedBeforeTraining(estimator, data, "Expected feature vector size 1 but found 0")
          assert(validationSpools == spoolsBefore)
          val valid = input(matrixType == "sparse", validation, malformed = false).coalesce(2)
          val model = estimator.setNumTasks(2).fit(valid)
          val predictions = model.transform(valid).select("prediction").collect().map(_.getDouble(0))
          assert(predictions.length == rowCount)
          assert(predictions.forall(value => !value.isNaN && !value.isInfinity))
          assert(validationSpools == spoolsBefore)
        } finally {
          data.unpersist()
        }
      }
    }
  }

  test("null training and validation vectors fail with an input error before native preparation") {
    Seq(false, true).foreach { validation =>
      val data = input(sparse = false, validation = validation, malformed = false)
        .withColumn(featuresCol, when(col("id") =!= rowCount - 1, col(featuresCol)))
      assertRejectedBeforeTraining(learner("dense", validation), data, "Feature vector must not be null")
    }
  }

  test("reused reference datasets reject a different feature count during executor reference preparation") {
    val valid = input(sparse = false, validation = false, malformed = false).coalesce(2)
    val estimator = learner("dense", validation = false).setNumTasks(2)
    estimator.fit(valid)
    assert(estimator.getReferenceDataset.nonEmpty)
    val twoFeatures = udf { vector: Vector => Vectors.dense(vector(0), vector(0)) }
    val changed = valid.withColumn(featuresCol, twoFeatures(col(featuresCol)))
    assertRejected(estimator, changed, "Expected feature vector size 2 but found 1", expectedTrainingStarted = true)
    assert(estimator.fit(valid).transform(valid).count() == rowCount)
  }

  test("feature row validation is lazy and preserves valid rows including empty sparse storage") {
    val valid = Row(Vectors.sparse(2, Array.empty[Int], Array.empty[Double]))
    var reads = 0
    val rows = Iterator(valid, Row(Vectors.dense(1.0))).map { row =>
      reads += 1
      row
    }
    val checked = DatasetUtils.validateFeatureRows(rows, 0, 2)
    assert(reads == 0)
    assert(checked.next() eq valid)
    assert(reads == 1)
    val failure = intercept[IllegalArgumentException](checked.next())
    assert(failure.getMessage.contains("Expected feature vector size 2 but found 1"))
    assert(reads == 2)
    assert(!DatasetUtils.validateFeatureRows(Iterator.empty, 0, 2).hasNext)
  }

  test("training-start observation distinguishes preflight rejection from failed training") {
    val measures = new InstrumentationMeasures()
    assert(!measures.hasTrainingStarted)
    assert(measures.trainingTime() == 0)
    measures.markTrainingStart()
    assert(measures.hasTrainingStarted)
    assert(measures.trainingTime() == 0)
    measures.markTrainingStop()
    assert(measures.hasTrainingStarted)
    assert(measures.trainingTime() > 0)
  }

  test("validated row counts preserve uncached adaptive partition topology through a public fit") {
    val adaptiveSpark = spark.newSession()
    adaptiveSpark.conf.set("spark.sql.adaptive.enabled", value = true)
    adaptiveSpark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", value = true)
    adaptiveSpark.conf.set("spark.sql.adaptive.coalescePartitions.parallelismFirst", value = false)
    adaptiveSpark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", 64 * 1024)
    adaptiveSpark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", 1)
    adaptiveSpark.conf.set("spark.sql.shuffle.partitions", 20)
    val width = 16
    val features = udf { id: Long =>
      Vectors.dense(Array.tabulate(width)(index => math.sin(id.toDouble * 17 + index)))
    }
    val data = adaptiveSpark.range(0L, 40000L, 1L, 20)
      .select((col("id") % 20000).as("key"), features(col("id")).as(featuresCol))
      .groupBy("key").agg(first(featuresCol).as(featuresCol))
      .withColumn(labelCol, (col("key") % 2).cast("double"))

    val census = data.mapPartitions { rows =>
      rows.map(row => (TaskContext.getPartitionId(), row))
    }(Encoders.tuple(Encoders.scalaInt, Encoders.row(data.schema))).collect()
    val expected = census.groupBy(_._1).toSeq.sortBy(_._1).map(_._2.length.toLong).toArray
    val projected = data.select(lit(0)).mapPartitions { rows =>
      Iterator.single(rows.foldLeft(0L)((count, _) => count + 1L))
    }(Encoders.scalaLong).collect()
    val actual = DatasetUtils.validatedRowCounts(data, featuresCol, width)

    assert(projected.length < expected.length,
      s"Fixture must expose AQE: ${projected.length} projected vs ${expected.length} full partitions")
    assert(actual.sameElements(expected),
      s"Expected ${expected.mkString(",")}, got ${actual.mkString(",")}")
    assert(actual.sum == 20000L)
    val model = learner("dense", validation = false).setNumTasks(2).fit(data)
    val predictions = model.transform(data).select("prediction").collect().map(_.getDouble(0))
    assert(predictions.length == actual.sum)
    assert(predictions.forall(value => !value.isNaN && !value.isInfinity))
  }
}
