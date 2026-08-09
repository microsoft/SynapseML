// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.utils

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.sql.functions.{col, expr, first, lit}
import org.slf4j.LoggerFactory

class VerifyClusterUtil extends TestBase {
  test("Verify ClusterUtil can get default number of executor cores based on master") {
    val log = LoggerFactory.getLogger("VerifyClusterUtil")

    // https://spark.apache.org/docs/latest/configuration.html
    assert(ClusterUtil.getDefaultNumExecutorCores(spark, log, Option("yarn")) == 1)
    assert(ClusterUtil.getDefaultNumExecutorCores(spark, log, Option("spark://localhost:7077")) ==
      ClusterUtil.getJVMCPUs(spark))
  }

  test("Verify row counts preserve the DataFrame partition topology") {
    // Isolate SQL settings without closing TestBase's shared SparkContext.
    val adaptiveSpark = spark.newSession()
    val partitionCount = 20
    adaptiveSpark.conf.set("spark.sql.adaptive.enabled", value = true)
    adaptiveSpark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", value = true)
    adaptiveSpark.conf.set("spark.sql.adaptive.coalescePartitions.parallelismFirst", value = false)
    adaptiveSpark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", 64 * 1024)
    adaptiveSpark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", 1)
    adaptiveSpark.conf.set("spark.sql.shuffle.partitions", partitionCount)

    val payloadExpression = (0 until 2)
      .map(index => s"sha2(concat(cast(id as string), ':$index'), 256)")
      .mkString("concat(", ",", ")")
    val dataframe = adaptiveSpark.range(0L, 40000L, 1L, partitionCount)
      .select((col("id") % 20000).as("key"), expr(payloadExpression).as("payload"))
      .groupBy("key")
      .agg(first("payload").as("payload"))

    val expected = dataframe.rdd
      .mapPartitionsWithIndex { case (index, rows) => Iterator(index -> rows.size.toLong) }
      .collect()
      .sortBy(_._1)
      .map(_._2)
    val projected = dataframe.select(lit(0)).rdd
      .mapPartitions(rows => Iterator(rows.size.toLong))
      .collect()
    val actual = ClusterUtil.getNumRowsPerPartition(dataframe, lit(0))

    assert(projected.length < expected.length,
      s"Fixture must expose adaptive coalescing: ${projected.length} projected vs ${expected.length} actual")
    assert(actual.sameElements(expected),
      s"Expected partition counts ${expected.mkString(",")}, got ${actual.mkString(",")}")
  }
}
