// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.io.http

import java.io.File
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.io.http.ServingImplicits._
import org.apache.spark.sql.functions.{col, length}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.BinaryType

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class ContinuousHTTPSuite extends TestBase with HTTPTestUtils {

  test("continuous mode"){
    val server = session
      .readStream
      .continuousServer
      .address(host, port, apiPath)
      .option("name", apiName)
      .load()
      .withColumn("foo", col("id.requestId"))
      .makeReply("foo")
      .writeStream
      .continuousServer
      .option("name", apiName)
      .queryName("foo").option("checkpointLocation",
        new File(tmpDir.toFile, s"checkpoints-${UUID.randomUUID()}").toString)
      .trigger(Trigger.Continuous("10 seconds"))  // only change in query
      .start()

    using(server){
      Thread.sleep(10000)
      val responsesWithLatencies = (1 to 100).map( i => sendStringRequest(client))
      assertLatency(responsesWithLatencies, 5)
    }
  }

  test("continuous mode with files"){
    val server = session
      .readStream
      .continuousServer
      .address(host, port, apiPath)
      .option("name", apiName)
      .load()
      .parseRequest(apiName, BinaryType)
      .withColumn("length", length(col("bytes")))
      .makeReply("length")
      .writeStream
      .continuousServer
      .option("name", apiName)
      .queryName("foo").option("checkpointLocation",
      new File(tmpDir.toFile, s"checkpoints-${UUID.randomUUID()}").toString)
      .trigger(Trigger.Continuous("1 second"))  // only change in query
      .start()

    using(server){
      Thread.sleep(5000)
      val responsesWithLatencies = (1 to 10).map( i =>  sendFileRequest(client))
      assertLatency(responsesWithLatencies, 100)
    }

  }

  ignore("forwarding ports to vm"){
    val server = session
      .readStream
      .continuousServer
      .address("0.0.0.0", 9010, apiPath)
      .option("name", apiName)
      .option("forwarding.enabled", true)
      .option("forwarding.sshHost", "")
      .option("forwarding.keySas", "")
      .option("forwarding.username", "")
      .load()
      .withColumn("foo", col("id.requestId"))
      .makeReply("foo")
      .writeStream
      .continuousServer
      .option("name", apiName)
      .queryName("foo").option("checkpointLocation",
      new File(tmpDir.toFile, s"checkpoints-${UUID.randomUUID()}").toString)
      .trigger(Trigger.Continuous("1 second"))
      .start()

    using(server){Thread.sleep(10000)}
  }

  test("async"){
    val server = session
      .readStream
      .continuousServer
      .address(host, port, apiPath)
      .option("name", apiName)
      .load()
      .withColumn("foo", col("id.requestId"))
      .makeReply("foo")
      .writeStream
      .continuousServer
      .option("name", apiName)
      .queryName("foo").option("checkpointLocation",
      new File(tmpDir.toFile, s"checkpoints-${UUID.randomUUID()}").toString)
      .trigger(Trigger.Continuous("1 second"))  // only change in query
      .start()

    using(server){
      Thread.sleep(5000)
      val futures = (1 to 10).map( i =>
        sendStringRequestAsync(client)
      )
      futures.foreach { f =>
        val resp = Await.result(f, Duration(5, TimeUnit.SECONDS))
        println(resp)
      }
    }
  }

  test("non continuous mode"){
    val server = session
      .readStream
      .continuousServer
      .address(host, port, apiPath)
      .option("name", apiName)
      .option("numPartitions", 1)
      .load()
      .withColumn("foo", col("id.requestId"))
      .makeReply("foo")
      .writeStream
      .format("console")
      .continuousServer
      .option("name", apiName)
      .queryName("foo").option("checkpointLocation",
      new File(tmpDir.toFile, s"checkpoints-${UUID.randomUUID()}").toString)
      .start()

    using(server){
      Thread.sleep(10000)
      println(server.status)
      val responsesWithLatencies = (1 to 100).map( i => sendStringRequest(client))
      assertLatency(responsesWithLatencies, 5)
    }

  }

}
