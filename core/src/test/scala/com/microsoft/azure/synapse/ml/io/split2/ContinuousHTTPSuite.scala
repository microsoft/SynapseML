// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.split2

import com.microsoft.azure.synapse.ml.core.test.base.{Flaky, TestBase}
import com.microsoft.azure.synapse.ml.io.IOImplicits._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, length}
import org.apache.spark.sql.streaming.{DataStreamReader, StreamingQuery, Trigger}
import org.apache.spark.sql.types.BinaryType

import java.io.File
import java.util.UUID
import scala.concurrent.Await

// scalastyle:off magic.number
class ContinuousHTTPSuite extends TestBase with Flaky with HTTPTestUtils {

  def baseReader: DataStreamReader = {
    spark
      .readStream
      .continuousServer
      .address(host, port, apiPath)
      .option("name", apiName)
  }

  def baseWrite(df: DataFrame, continuous: Boolean = true): StreamingQuery = {
    val writer = df.writeStream
      .continuousServer
      .option("name", apiName)
      .queryName("foo")
      .option("checkpointLocation", new File(tmpDir.toFile, s"checkpoints-${UUID.randomUUID()}").toString)

    if (continuous) {
      writer.trigger(Trigger.Continuous("10 seconds")).start()
    } else {
      writer.start()
    }
  }

  test("continuous mode") {
    val server = baseWrite(baseReader
      .load()
      .withColumn("foo", col("id.requestId"))
      .makeReply("foo"))

    using(server) {
      Thread.sleep(10000)
      val responsesWithLatencies = (1 to 500).map(_ => sendStringRequest())
      assertLatency(responsesWithLatencies, 5)
    }
  }

  test("continuous mode with files") {
    val server = baseWrite(baseReader
      .load()
      .parseRequest(apiName, BinaryType)
      .withColumn("length", length(col("bytes")))
      .makeReply("length"))

    using(server) {
      Thread.sleep(5000)
      val responsesWithLatencies = (1 to 30).map(_ => sendFileRequest())
      assertLatency(responsesWithLatencies, 100)
    }

  }

  ignore("forwarding ports to vm") {
    val server = baseWrite(baseReader
      .address("0.0.0.0", 9010, apiPath)
      .option("forwarding.enabled", value = true)
      .option("forwarding.sshHost", "")
      .option("forwarding.keySas", "")
      .option("forwarding.username", "")
      .load()
      .withColumn("foo", col("id.requestId"))
      .makeReply("foo"))

    using(server) {
      Thread.sleep(10000)
    }
  }

  test("async") {
    val server = baseWrite(baseReader
      .load()
      .withColumn("foo", col("id.requestId"))
      .makeReply("foo"))

    using(server) {
      Thread.sleep(5000)
      val futures = (1 to 10).map(_ =>
        sendStringRequestAsync()
      )
      futures.foreach { f =>
        val resp = Await.result(f, requestDuration)
        println(resp)
      }
    }
  }

  test("non continuous mode") {
    val server = baseWrite(baseReader
      .option("numPartitions", 1)
      .load()
      .withColumn("foo", col("id.requestId"))
      .makeReply("foo"), continuous = false)

    using(server) {
      Thread.sleep(10000)
      println(server.status)
      val responsesWithLatencies = (1 to 500).map(_ => sendStringRequest())
      assertLatency(responsesWithLatencies, 5)
    }
  }

}
