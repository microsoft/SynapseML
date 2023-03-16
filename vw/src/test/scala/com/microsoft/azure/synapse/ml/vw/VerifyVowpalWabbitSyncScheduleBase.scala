// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.test.benchmarks.Benchmarks
import org.apache.spark.SparkException

class VerifyVowpalWabbitSyncScheduleBase extends Benchmarks {
  val numPartitions = 2

  test("Verify VW Sync Schedule Splits") {
    import spark.implicits._

    val df = Seq(
      (1, "A", 1),
      (1, "B", 2),
      (1, "C", 3),
      (1, "D", 4),
      (2, "X", 5),
      (2, "Y", 6),
      (2, "Z", 7)
    ) .toDF("partitionKey", "value", "rowId")
      .repartition(2, $"partitionKey")

    val splits = new VowpalWabbitSyncScheduleSplits(df, 2)

    val actual = df.mapPartitions(it =>
      { it.map { row => (row.getInt(2), splits.shouldTriggerAllReduce(row)) } })
      .toDF("rowId", "shouldTrigger")
      .orderBy("rowId")

    val expected = Seq(
      (1, false),
      (2, true),
      (3, false),
      (4, true),
      (5, false),
      (6, true),
      (7, true)
    ).toDF("partitionKey", "shouldTrigger")

    verifyResult(expected, actual)
  }

  test("Verify VW Sync Schedule Splits to large") {
    import spark.implicits._

    val df = Seq("A", "B") .toDF("value")

    val splits = new VowpalWabbitSyncScheduleSplits(df, 3)

    withoutLogging {
      intercept[SparkException] {
        df.mapPartitions(it => {
          it.map { row => splits.shouldTriggerAllReduce(row) }
        }).count()
      }
    }
  }
}
