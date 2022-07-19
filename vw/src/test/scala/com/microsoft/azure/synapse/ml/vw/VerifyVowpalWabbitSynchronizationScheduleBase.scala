package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.test.benchmarks.Benchmarks
import org.apache.spark.TaskContext
import org.apache.spark.sql.{types => T}

import java.sql.Timestamp
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime, ZoneId}
import java.util.Calendar

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
      (1, "X"),
      (1, "Y"),
      (1, "Z")
    ) .toDF("partitionKey", "value")
      .repartition(2, $"partitionKey")

    val splits = new VowpalWabbitSynchronizationScheduleSplits(df, 3)

    val actual = df.mapPartitions(it =>
      { it.map { row => (TaskContext.getPartitionId(), splits.getAllReduceTriggerCount(row)) } })
      .toDF("partitionKey", "shouldTrigger")

    actual.show()

    val expected = Seq(
      (0, 0),
      (0, 0),
      (0, 0),
      (0, 1),
      (1, 0),
      (1, 0),
      (1, 1)
    ).toDF("partitionKey", "shouldTrigger")

    // TODO: this completely wrong
    assert (actual.except(expected).isEmpty)

    // TODO: what does this do?
    // assert(actual === expected)
  }

  private def SimpleTimestamp(year: Int, month: Int, day: Int): Timestamp =
    Timestamp.valueOf(LocalDateTime.of(year, month, day, 12, 0, 0))

  test("Verify VW Sync Schedule Teemporal Splits") {
    import spark.implicits._

    val df = Seq(
      (1, SimpleTimestamp(2022, 1, 1)),
      (1, SimpleTimestamp(2022, 1, 1)),
      (1, SimpleTimestamp(2022, 1, 3)),
      (1, SimpleTimestamp(2022, 1, 4)),
      (2, SimpleTimestamp(2022, 1, 1)),
      (2, SimpleTimestamp(2022, 1, 2)),
      (2, SimpleTimestamp(2022, 1, 5))
    ).toDF("partitionKey", "timestamp")
      .repartition(2, $"partitionKey")

    val splits = new VowpalWabbitSynchronizationScheduleTemporal(df, "timestamp", ChronoUnit.DAYS, 1)

    val actual = df.mapPartitions(it =>
    { it.map { row => (TaskContext.getPartitionId(), splits.getAllReduceTriggerCount(row)) } })
      .toDF("partitionKey", "shouldTrigger")

    actual.show

    val expected = Seq(
      (0, 0),
      (0, 1),
      (0, 3),
      (1, 0),
      (1, 0),
      (1, 2),
      (1, 1),
    ).toDF("partitionKey", "shouldTrigger")

    // TODO: this is completely wrong
    assert (actual.except(expected).isEmpty)
  }

  // TODO: unit test w/ temporal split w/ multiple days
}
