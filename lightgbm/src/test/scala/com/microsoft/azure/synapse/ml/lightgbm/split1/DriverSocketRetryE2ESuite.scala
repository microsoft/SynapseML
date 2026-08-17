// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.lightgbm.params.BaseTrainParams
import com.microsoft.azure.synapse.ml.lightgbm.{ColumnParams, LightGBMClassifier, LightGBMDelegate}
import org.apache.spark.SparkException
import org.apache.spark.TaskContext
import org.apache.spark.ml.linalg.{SQLDataTypes, Vectors}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.slf4j.Logger

import java.io.{PrintWriter, StringWriter}

object FailFirstAttemptDelegate {
  val Sentinel: String = "INJECTED_TASK_FAILURE_AFTER_TOPOLOGY_HANDSHAKE"
}

/** Fails the first attempt of a training task, after it has already completed the driver
  * topology handshake. Later attempts do nothing, so the job could succeed on retry.
  */
class FailFirstAttemptDelegate extends LightGBMDelegate {
  override def beforeGenerateTrainDataset(batchIndex: Int,
                                          partitionId: Int,
                                          columnParams: ColumnParams,
                                          schema: StructType,
                                          log: Logger,
                                          trainParams: BaseTrainParams): Unit = {
    val context = TaskContext.get()
    if (context != null && context.attemptNumber() == 0) {
      throw new RuntimeException(FailFirstAttemptDelegate.Sentinel)
    }
  }
}

/** End-to-end coverage for distributed LightGBM training when a task fails after it has joined the
  * LightGBM network. Every Spark retry of that task is refused by the driver, which used to replace
  * the real failure with a bare "java.net.ConnectException: Connection refused".
  */
class DriverSocketRetryE2ESuite extends LightGBMTestUtils {

  private def makeDataframe: DataFrame = {
    val schema = StructType(Seq(
      StructField(labelCol, DoubleType),
      StructField(featuresCol, SQLDataTypes.VectorType)))
    val rows = (0 until 400).map(i =>
      Row(if (i % 2 == 0) 0.0 else 1.0, Vectors.dense((i % 13).toDouble, (i % 7).toDouble)))
    spark.createDataFrame(spark.sparkContext.parallelize(rows, 2), schema)
  }

  private def stackTraceOf(throwable: Throwable): String = {
    val writer = new StringWriter()
    throwable.printStackTrace(new PrintWriter(writer))
    writer.toString
  }

  test("A task retry that cannot rejoin the LightGBM network reports why instead of Connection refused") {
    // Spark must be allowed to retry tasks for the failure cascade to appear.
    sparkProvider.resetSparkSession(numRetries = 4, numCores = Some(2))
    try {
      val classifier = new LightGBMClassifier()
        .setLabelCol(labelCol)
        .setFeaturesCol(featuresCol)
        .setNumLeaves(5)
        .setNumIterations(5)
        .setDefaultListenPort(getAndIncrementPort())
        .setDelegate(new FailFirstAttemptDelegate())

      val thrown = intercept[SparkException] {
        classifier.fit(makeDataframe)
      }
      val trace = stackTraceOf(thrown)

      // Spark only surfaces the last attempt, and that attempt can never reach the driver again.
      assert(trace.contains("ConnectException"),
        s"Expected the retries to fail against the closed driver endpoint, got:\n${trace.take(4000)}")

      // The reported message must explain the cascade and point at the attempt that really failed.
      assert(thrown.getMessage.contains("retry attempt"),
        s"Expected the failure to identify itself as a retry, got:\n${thrown.getMessage.take(2000)}")
      assert(thrown.getMessage.contains("consequence of an earlier failure"),
        s"Expected the failure to point at the original error, got:\n${thrown.getMessage.take(2000)}")
    } finally {
      sparkProvider.resetSparkSession()
    }
  }
}
