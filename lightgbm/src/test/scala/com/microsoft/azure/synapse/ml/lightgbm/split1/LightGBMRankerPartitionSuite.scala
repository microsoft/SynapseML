// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.lightgbm.LightGBMRanker
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

  private def ranker(numTasks: Int, useBarrierExecutionMode: Boolean = false): LightGBMRanker = {
    new LightGBMRanker()
      .setFeaturesCol(featuresCol)
      .setLabelCol(labelCol)
      .setGroupCol(queryCol)
      .setRepartitionByGroupingColumn(true)
      .setUseBarrierExecutionMode(useBarrierExecutionMode)
      .setNumTasks(numTasks)
      .setNumThreads(1)
      .setNumLeaves(3)
      .setNumIterations(1)
      .setDefaultListenPort(getAndIncrementPort())
  }

  private def partitionGroups(df: DataFrame): Array[(Int, Set[Long])] = {
    df.select(queryCol).rdd.mapPartitionsWithIndex { case (partitionIndex, rows) =>
      Iterator(partitionIndex -> rows.map(_.getLong(0)).toSet)
    }.collect()
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

  test("non-barrier ranker fits when requested tasks exceed input partitions") {
    val requestedTasks = 4
    val data = rankerData(inputPartitions = 2)
    val estimator = ranker(requestedTasks)
    val partitions = partitionGroups(estimator.prepareDataframe(data, requestedTasks))
    assert(partitions.length === requestedTasks)

    val model = estimator.fit(data)

    try {
      assert(model.transform(data).count() === data.count())
    } finally {
      model.getModel.freeNativeMemory()
    }
  }
}
