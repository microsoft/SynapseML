// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMClassifier, LightGBMMissingTasksException, LightGBMRanker}
import org.apache.spark.TaskContext
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, floor}

/** Covers non-barrier training when the requested numTasks does not match the tasks that can run. */
// scalastyle:off magic.number
class LightGBMNumTasksSuite extends LightGBMTestUtils {

  private val queryCol = "query"
  private val numRows = 512

  private def trainingData(inputPartitions: Int): DataFrame = {
    val input = spark.range(0L, numRows.toLong, 1L, inputPartitions)
      .withColumn(queryCol, floor(col("id") / 8L).cast("long"))
      .withColumn(labelCol, (col("id") % 2L).cast("double"))
      .withColumn("signal", col(labelCol))
      .withColumn("other", (col("id") % 11L).cast("double"))

    new VectorAssembler()
      .setInputCols(Array("signal", "other"))
      .setOutputCol(featuresCol)
      .transform(input)
      .select(queryCol, labelCol, featuresCol)
  }

  /** Exposes the protected partition preparation that fit uses. */
  private class PreparingClassifier extends LightGBMClassifier {
    def prepare(data: DataFrame, numTasks: Int): DataFrame = prepareDataframe(data, numTasks)
  }

  private def classifier(numTasks: Option[Int], timeoutSeconds: Double = 120): PreparingClassifier = {
    val estimator = new PreparingClassifier()
    estimator
      .setFeaturesCol(featuresCol)
      .setLabelCol(labelCol)
      .setUseBarrierExecutionMode(false)
      .setNumThreads(1)
      .setNumLeaves(4)
      .setNumIterations(10)
      // Bound a worker-count mismatch so it fails the test instead of waiting for the 1200s default.
      .setTimeout(timeoutSeconds)
      .setDefaultListenPort(getAndIncrementPort())
    numTasks.foreach(estimator.setNumTasks)
    estimator
  }

  private def ranker(numTasks: Int): LightGBMRanker = {
    new LightGBMRanker()
      .setFeaturesCol(featuresCol)
      .setLabelCol(labelCol)
      .setGroupCol(queryCol)
      .setRepartitionByGroupingColumn(false)
      .setUseBarrierExecutionMode(false)
      .setNumTasks(numTasks)
      .setNumThreads(1)
      .setNumLeaves(4)
      .setNumIterations(10)
      .setTimeout(120)
      .setDefaultListenPort(getAndIncrementPort())
  }

  private def partitionGroups(df: DataFrame): Array[(Int, Set[Long])] = {
    import df.sparkSession.implicits._
    // mapPartitions runs once per partition, so empty partitions are still counted.
    df.select(queryCol).as[Long].mapPartitions { groups =>
      Iterator(TaskContext.getPartitionId() -> groups.toSet.toSeq)
    }.collect().map { case (partitionIndex, groups) => partitionIndex -> groups.toSet }
  }

  private def causes(failure: Throwable): Seq[Throwable] = {
    Iterator.iterate(failure)(_.getCause).takeWhile(_ != null).take(20).toSeq  //scalastyle:ignore null
  }

  test("an explicit numTasks above the input partition count repartitions to numTasks") {
    val prepared = classifier(Some(4)).prepare(trainingData(inputPartitions = 1), numTasks = 4)

    assert(prepared.rdd.getNumPartitions === 4)
    assert(prepared.count() === numRows)
  }

  test("an explicit numTasks at or below the input partition count still coalesces") {
    val prepared = classifier(Some(2)).prepare(trainingData(inputPartitions = 4), numTasks = 2)

    assert(prepared.rdd.getNumPartitions === 2)
  }

  test("an automatic numTasks does not add a shuffle") {
    // determineNumTasks never picks more tasks than input partitions, so this only guards the check.
    val prepared = classifier(None).prepare(trainingData(inputPartitions = 1), numTasks = 4)

    assert(prepared.rdd.getNumPartitions === 1)
  }

  // Non-barrier training needs every task running at once, and CI agents have two cores,
  // so the fit tests below request two tasks.
  test("non-barrier classifier fits when numTasks exceeds the input partitions") {
    val data = trainingData(inputPartitions = 1)
    val model = classifier(Some(2)).fit(data)
    try {
      val scored = model.transform(data).select(labelCol, predCol).collect()
      assert(scored.length === numRows)
      val accuracy = scored.count(row => row.getDouble(0) == row.getDouble(1)).toDouble / scored.length
      assert(accuracy > 0.95, s"The signal feature equals the label, but accuracy was $accuracy")
    } finally {
      model.getModel.freeNativeMemory()
    }
  }

  test("ranker expansion keeps each query group in one partition without grouping repartition") {
    val partitions = partitionGroups(ranker(4).prepareDataframe(trainingData(inputPartitions = 1), numTasks = 4))

    assert(partitions.length === 4)
    val groupLocations = partitions.flatMap { case (partitionIndex, groups) =>
      groups.map(_ -> partitionIndex)
    }.groupBy(_._1).map { case (group, locations) => group -> locations.map(_._2).toSet }
    assert(groupLocations.size === numRows / 8)
    assert(groupLocations.values.forall(_.size === 1))
  }

  test("non-barrier ranker fits when numTasks exceeds the input partitions without grouping repartition") {
    val data = trainingData(inputPartitions = 1)
    val model = ranker(2).fit(data)
    try {
      val predictions = model.transform(data).select(predCol).collect().map(_.getDouble(0))
      assert(predictions.length === numRows)
      assert(predictions.forall(p => !p.isNaN && !p.isInfinite))
    } finally {
      model.getModel.freeNativeMemory()
    }
  }

  test("numTasks above the concurrent task slots fails with the missing-task explanation") {
    // Only defaultParallelism tasks can run at once in local mode, so one task never starts and the
    // driver stops waiting after the timeout.
    val numTasks = spark.sparkContext.defaultParallelism + 1
    val data = trainingData(inputPartitions = numTasks)
    val failure = intercept[Exception] {
      classifier(Some(numTasks), timeoutSeconds = 10).fit(data)
    }

    val missingTasks = causes(failure).collectFirst { case e: LightGBMMissingTasksException => e }
    assert(missingTasks.isDefined, s"Expected a missing-task explanation, got: $failure")
    val message = missingTasks.get.getMessage
    assert(message.contains(s"of $numTasks training tasks"), message)
    assert(message.contains("Missing partitions:"), message)
    assert(message.contains("numTasks is no larger than the number of tasks Spark can run at once"), message)
  }
}
