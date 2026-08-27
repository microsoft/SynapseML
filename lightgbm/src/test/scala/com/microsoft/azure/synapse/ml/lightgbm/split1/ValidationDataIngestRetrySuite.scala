// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.lightgbm.{ValidationDataServer, ValidationDataServerResourceFactory}
import org.apache.commons.io.FileUtils
import org.apache.spark.TaskContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StructField, StructType}

import java.io.File
import java.util.UUID

class ValidationDataIngestRetrySuite extends TestBase {
  private val host = "127.0.0.1"
  private val timeoutSeconds = 60.0

  test("ingest promotes the Spark attempt that successfully commits after a fully sent attempt fails") {
    sparkProvider.resetSparkSession(numRetries = 4, numCores = Some(1))
    val spool = scratchDirectory("ingest-task-retry")
    var server = Option.empty[ValidationDataServer]
    try {
      val schema = StructType(Seq(StructField("attempt", LongType, nullable = false)))
      val retryRows = spark.sparkContext.parallelize(Seq(Row(0L)), 1).mapPartitions { _ =>
        val context = TaskContext.get()
        if (context.attemptNumber() == 0) {
          context.addTaskCompletionListener[Unit] { _ =>
            throw new RuntimeException("synthetic failure after validation transfer")
          }
        }
        Iterator.single(Row(context.attemptNumber().toLong))
      }
      val validationData = spark.createDataFrame(retryRows, schema)
      val created = ValidationDataServer.create(
        validationData, host, 1, timeoutSeconds, spool, ValidationDataServerResourceFactory.Default)
      server = Option(created)
      val descriptor = spark.sparkContext.broadcast(created.params.toRows)
      try {
        val iterator = ValidationDataServer.read(descriptor)
        val rows = ValidationDataServer.withRows(iterator)(iterator.toArray)
        assert(rows.length == 1)
        assert(rows.head.getLong(0) > 0L, "The failed first task attempt was retained as canonical validation data")
        assert(Option(spool.listFiles()).getOrElse(Array.empty).map(_.getName).sameElements(Array("part-0")))
      } finally {
        descriptor.destroy()
      }
    } finally {
      try server.foreach(_.close())
      finally {
        deleteIfPresent(spool)
        sparkProvider.resetSparkSession()
      }
    }
  }

  private def scratchDirectory(name: String): File = {
    new File(System.getProperty("user.dir"), s".synapseml-validation-$name-${UUID.randomUUID()}")
  }

  private def deleteIfPresent(directory: File): Unit = {
    if (directory.exists()) FileUtils.deleteDirectory(directory)
  }
}
