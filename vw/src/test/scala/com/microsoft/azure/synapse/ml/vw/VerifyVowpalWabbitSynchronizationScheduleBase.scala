package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.test.benchmarks.Benchmarks
import org.apache.spark.TaskContext

class VerifyVowpalWabbitSynchronizationScheduleBase extends Benchmarks {
  lazy val moduleName = "vw"
  val numPartitions = 2

  test("Verify VW Sync Schedule Splits") {
    import spark.implicits._

    val df = Seq(
      (1, "A"),
      (1, "B"),
      (1, "C"),
      (1, "D"),
      (2, "X"),
      (2, "Y"),
      (2, "Z")
    ) .toDF("partitionKey", "value")
      .repartition(2, $"partitionKey")

    val splits = new VowpalWabbitSynchronizationScheduleSplits(df, 2)

    val actual = df.mapPartitions(it =>
      { it.map { row => (TaskContext.getPartitionId(), splits.shouldTriggerAllReduce(row)) } })
      .toDF("partitionKey", "shouldTrigger")

    val expected = Seq(
      (0, false),
      (0, false),
      (0, true),
      (0, false),
      (1, false),
      (1, true),
      (1, false)
    ).toDF("partitionKey", "shouldTrigger")

    // TODO: is this the right way of doing it?
    assert (actual.except(expected).isEmpty)

    // TODO: what does this do?
    // assert(actual === expected)
  }
}
