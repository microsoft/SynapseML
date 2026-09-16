// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.lightgbm.{GroupIdManager, LightGBMConstants, LightGBMRanker}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.{abs, col, count, countDistinct, floor, isnan, lit, when}

class GroupIdManagerSuite extends LightGBMTestUtils {
  test("distinct string groups get distinct ids") {
    val manager = new GroupIdManager()
    val ids = Array("query_a", "query_b", "query_c").map(manager.getUniqueIdForGroup)

    assert(ids.distinct.length === 3,
      "Every distinct string group must map to its own id, otherwise LightGBM treats " +
        "the whole executor dataset as a single query.")
    assert(ids.sorted.toSeq === Seq(0, 1, 2))
  }

  test("repeated string groups are stable") {
    val manager = new GroupIdManager()
    val first = manager.getUniqueIdForGroup("query_a")
    manager.getUniqueIdForGroup("query_b")

    assert(manager.getUniqueIdForGroup("query_a") === first)
  }

  test("distinct long groups get distinct ids") {
    val manager = new GroupIdManager()
    val ids = Array(100L, 200L, 300L).map(manager.getUniqueIdForGroup)

    assert(ids.distinct.length === 3)
    assert(ids.sorted.toSeq === Seq(0, 1, 2))
    assert(manager.getUniqueIdForGroup(200L) === ids(1))
  }

  test("int groups pass through unchanged") {
    val manager = new GroupIdManager()

    assert(manager.getUniqueIdForGroup(42) === 42)
    assert(manager.getUniqueIdForGroup(7) === 7)
  }

  test("unsupported group types are rejected") {
    val manager = new GroupIdManager()

    assertThrows[IllegalArgumentException](manager.getUniqueIdForGroup(1.5d))
  }

  Seq("dense", "sparse").foreach { matrixType =>
    test(s"streaming ranker preserves string query groups with $matrixType features") {
      val queryCount = 4L
      val rowsPerQuery = 3000L
      val rowCount = queryCount * rowsPerQuery
      val queryCol = "query"
      val input = spark.range(0L, rowCount, 1L, 1)
        .withColumn(queryCol, (col("id") % queryCount).cast("string"))
        .withColumn(labelCol, (floor(col("id") / queryCount) % 3).cast("double"))
        .withColumn("signal", col(labelCol))
        .withColumn("other", (col("id") % 7).cast("double"))
      val data = new VectorAssembler()
        .setInputCols(Array("signal", "other"))
        .setOutputCol(featuresCol)
        .transform(input)
        .select(queryCol, labelCol, featuresCol)
        .cache()

      try {
        // Each query is legal, but merging this executor's queries exceeds LightGBM's 10000-row query limit.
        val estimator = new LightGBMRanker()
          .setFeaturesCol(featuresCol)
          .setLabelCol(labelCol)
          .setGroupCol(queryCol)
          .setDataTransferMode(LightGBMConstants.StreamingDataTransferMode)
          .setRepartitionByGroupingColumn(true)
          .setUseBarrierExecutionMode(true)
          .setNumTasks(1)
          .setNumThreads(1)
          .setMaxStreamingOMPThreads(1)
          .setMatrixType(matrixType)
          .setMicroBatchSize(127)
          .setNumLeaves(3)
          .setNumIterations(1)
          .setDefaultListenPort(getAndIncrementPort())

        val model = estimator.fit(data)
        try {
          val prediction = col(model.getPredictionCol)
          val invalid = prediction.isNull || isnan(prediction) || (abs(prediction) > Double.MaxValue)
          val summary = model.transform(data)
            .agg(count(lit(1)), count(when(invalid, 1)), countDistinct(prediction))
            .head()

          assert(model.numFeatures === 2)
          assert(summary.getLong(0) === rowCount)
          assert(summary.getLong(1) === 0L)
          assert(summary.getLong(2) > 1L)
        } finally {
          model.getModel.freeNativeMemory()
        }
      } finally {
        data.unpersist()
      }
    }
  }
}
