// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMConstants, LightGBMRanker}
import org.apache.spark.TaskContext
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, floor}

// scalastyle:off magic.number
class LightGBMRankerPartitionSuite extends LightGBMTestUtils {

  private val queryCol = "query"

  private def rankerData(inputPartitions: Int): DataFrame = {
    val adaptiveSpark = spark.newSession()
    adaptiveSpark.conf.set("spark.sql.adaptive.enabled", "true")
    adaptiveSpark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    adaptiveSpark.conf.set("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false")
    adaptiveSpark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", 64L * 1024L * 1024L)
    adaptiveSpark.conf.set("spark.sql.shuffle.partitions", "16")

    val input = adaptiveSpark.range(0L, 512L, 1L, inputPartitions)
      .withColumn(queryCol, floor(col("id") / 8L).cast("long"))
      .withColumn(labelCol, (col("id") % 4L).cast("double"))
      .withColumn("signal", col(labelCol))
      .withColumn("other", (col("id") % 11L).cast("double"))

    new VectorAssembler()
      .setInputCols(Array("signal", "other"))
      .setOutputCol(featuresCol)
      .transform(input)
      .select(queryCol, labelCol, featuresCol)
  }

  private def ranker(numTasks: Int,
                     useBarrierExecutionMode: Boolean = false,
                     transferMode: String = LightGBMConstants.StreamingDataTransferMode): LightGBMRanker = {
    new LightGBMRanker()
      .setFeaturesCol(featuresCol)
      .setLabelCol(labelCol)
      .setGroupCol(queryCol)
      .setRepartitionByGroupingColumn(true)
      .setUseBarrierExecutionMode(useBarrierExecutionMode)
      .setDataTransferMode(transferMode)
      .setNumTasks(numTasks)
      .setNumThreads(1)
      .setNumLeaves(8)
      .setNumIterations(10)
      // Bound a worker-count mismatch so it fails the test instead of waiting for the 1200s default.
      .setTimeout(120)
      .setDefaultListenPort(getAndIncrementPort())
  }

  private def assertFitRanksBySignal(estimator: LightGBMRanker, data: DataFrame): Unit = {
    val model = estimator.fit(data)
    try {
      val scored = model.transform(data).select(labelCol, predCol).collect()
      assert(scored.length === 512)
      val predictions = scored.map(_.getDouble(1))
      assert(predictions.forall(p => !p.isNaN && !p.isInfinite))

      // The signal feature equals the relevance label, so a ranker trained on correctly grouped
      // queries must score each higher relevance level above the one below it.
      val meanByLabel = scored.groupBy(_.getDouble(0)).map { case (label, rows) =>
        label -> rows.map(_.getDouble(1)).sum / rows.length
      }
      assert(meanByLabel.keySet === Set(0.0, 1.0, 2.0, 3.0))
      (0 until 3).foreach { label =>
        assert(meanByLabel(label + 1.0) > meanByLabel(label.toDouble),
          s"Mean score for label ${label + 1} should exceed label $label: $meanByLabel")
      }
    } finally {
      model.getModel.freeNativeMemory()
    }
  }

  private def partitionGroups(df: DataFrame): Array[(Int, Set[Long])] = {
    import df.sparkSession.implicits._
    // mapPartitions runs once per partition, so empty partitions are still counted.
    df.select(queryCol).as[Long].mapPartitions { groups =>
      Iterator(TaskContext.getPartitionId() -> groups.toSet.toSeq)
    }.collect().map { case (partitionIndex, groups) => partitionIndex -> groups.toSet }
  }

  test("non-barrier ranker preserves grouping partitions under AQE") {
    val requestedTasks = 8
    val data = rankerData(inputPartitions = 16)
    val estimator = ranker(requestedTasks)
    val partitions = partitionGroups(estimator.prepareDataframe(data, requestedTasks))

    assert(partitions.length === requestedTasks)
    val groupLocations = partitions.flatMap { case (partitionIndex, groups) =>
      groups.map(_ -> partitionIndex)
    }.groupBy(_._1).map { case (group, locations) =>
      group -> locations.map(_._2).toSet
    }
    assert(groupLocations.values.forall(_.size === 1))
  }

  test("barrier grouping repartition does not expand inputs with fewer partitions") {
    val data = rankerData(inputPartitions = 2)
    val estimator = ranker(numTasks = 4, useBarrierExecutionMode = true)
    val partitions = partitionGroups(estimator.prepareDataframe(data, numTasks = 4))

    assert(partitions.length === 2)
  }

  test("non-barrier grouping expands inputs to the requested task count") {
    val requestedTasks = 4
    val data = rankerData(inputPartitions = 2)
    val partitions = partitionGroups(ranker(requestedTasks).prepareDataframe(data, requestedTasks))

    assert(partitions.length === requestedTasks)
  }

  // Non-barrier training needs every task running at once, and CI agents have two cores,
  // so the fit tests below request two tasks.
  test("non-barrier ranker fits when requested tasks exceed input partitions") {
    val requestedTasks = 2
    val data = rankerData(inputPartitions = 1)
    val estimator = ranker(requestedTasks)
    val partitions = partitionGroups(estimator.prepareDataframe(data, requestedTasks))
    assert(partitions.length === requestedTasks)

    assertFitRanksBySignal(estimator, data)
  }

  Seq(LightGBMConstants.StreamingDataTransferMode, LightGBMConstants.BulkDataTransferMode).foreach { mode =>
    test(s"non-barrier ranker fits when AQE would coalesce the grouping shuffle ($mode)") {
      // More input partitions than tasks is the common default shape. Without an explicit grouping
      // partition count, AQE coalesces this shuffle to one partition and training waits for the rest.
      assertFitRanksBySignal(ranker(numTasks = 2, transferMode = mode), rankerData(inputPartitions = 16))
    }
  }
}
