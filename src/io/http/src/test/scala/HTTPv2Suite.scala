// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.microsoft.ml.spark.FileUtilities.File
import com.microsoft.ml.spark.ServingImplicits._
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.streaming.continuous._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.types._
import org.scalatest.Assertion

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.util.Try

class HTTPv2Suite extends TestBase with HTTPTestUtils {
  //override val logLevel: String = "INFO"
  override protected val numRetries: Int = 20

  def baseDF(numPartitions: Int = 4,
             apiName: String = apiName,
             apiPath: String = apiPath,
             port: Int = port,
             epochLength: Long = 5000): DataFrame = {
    session
      .readStream
      .format(classOf[HTTPSourceProviderV2].getName)
      .address(host, port, apiPath)
      .option("name", apiName)
      .option("epochLength", epochLength)
      .option("numPartitions", numPartitions.toLong)
      .load()
  }

  def baseWrite(df: DataFrame,
                name: String = "foo",
                apiName: String = apiName): DataStreamWriter[Row] = {
    df.writeStream
      .format(classOf[HTTPSinkProviderV2].getName)
      .option("name", apiName)
      .queryName(name).option("checkpointLocation",
      new File(tmpDir.toFile, s"checkpoints-${UUID.randomUUID()}").toString)
  }

  def basePipeline(numPartitions: Int = 4,
                   name: String = "foo",
                   apiName: String = apiName,
                   apiPath: String = apiPath,
                   port: Int = port): DataStreamWriter[Row] = {
    baseWrite(baseDF(numPartitions, apiName, apiPath, port)
      .withColumn("foo", col("id.requestId"))
      .makeReply("foo"), name, apiName
    )
  }

  test("continuous") {
    val server = basePipeline()
      .trigger(Trigger.Continuous("10 seconds")) // only change in query
      .start()
    using(server) {
      Thread.sleep(3000)
      assertLatency((1 to 100).map(i => sendStringRequest(client)), 5)
      println(HTTPSourceStateHolder.serviceInfoJson(apiName))
    }
  }

  test("async continuous") {
    val server = basePipeline()
      .trigger(Trigger.Continuous("1 seconds")) // only change in query
      .start()

    using(server) {
      Thread.sleep(3000)
      val futures = (1 to 100).map(i => sendStringRequestAsync(client))
      val responsesWithLatencies = futures.map(Await.result(_, Duration(5, TimeUnit.SECONDS)))
      assertLatency(responsesWithLatencies, 2000)
    }

  }

  test("async microbatch") {
    val server = basePipeline()
      .start()

    using(server) {
      Thread.sleep(3000)
      val futures = (1 to 100).map(i => sendStringRequestAsync(client))
      val responsesWithLatencies = futures.map(Await.result(_, Duration(5, TimeUnit.SECONDS)))
      assertLatency(responsesWithLatencies, 2000)
      println(HTTPSourceStateHolder.serviceInfoJson(apiName))
    }

  }

  test("double pipeline") {
    val server = basePipeline()
      .start()

    using(server) {
      waitForServer(server)
      (1 to 100).foreach(i => sendStringRequest(client))
      println(HTTPSourceStateHolder.serviceInfoJson(apiName))
    }

    Thread.sleep(5000)
    val server2 = basePipeline()
      .start()

    using(server2) {
      waitForServer(server2)
      (1 to 100).foreach(i => sendStringRequest(client))
      println(HTTPSourceStateHolder.serviceInfoJson(apiName))
    }

  }

  test("microbatch") {
    val server = basePipeline()
      .start()

    using(server) {
      waitForServer(server)
      val responsesWithLatencies = (1 to 100).map(_ => sendStringRequest(client))
      Thread.sleep(1000)
      (1 to 100).foreach(_ => sendStringRequest(client))
      assertLatency(responsesWithLatencies, 10)
      println(HTTPSourceStateHolder.serviceInfoJson(apiName))
    }

  }

  test("errors if 2 services are made at the same time with the same name") {
    val server1 = basePipeline(2, apiName = "n1")
      .start()

    assertThrows[AssertionError] {
      val server2 = basePipeline(2, "bar", apiName = "n1")
        .start()
      Thread.sleep(1000)
      throw server2.exception.get.cause
    }

    println("stopping server")
    server1.stop()
  }

  test("two services can run independently") {
    val server1 = basePipeline(numPartitions = 2, name = "q1",
      apiPath = "foo", apiName = "n1").start()
    using(server1) {
      val server2 = basePipeline(numPartitions = 2, name = "q2",
        apiPath = "bar", apiName = "n2", port = port2).start()
      waitForServer(server1)
      using(server2) {
        waitForServer(server2)
        val l1 = (1 to 100).map(_ => sendStringRequest(client, s"http://$host:$port/foo"))
        val l2 = (1 to 100).map(_ => sendStringRequest(client, s"http://$host:$port2/bar"))
        assertLatency(l1, 10)
        assertLatency(l2, 10)

        println(HTTPSourceStateHolder.serviceInfoJson("n1"))
        println(HTTPSourceStateHolder.serviceInfoJson("n2"))
      }
    }
  }

  test("can reply to bad requests immediately partial") {
    val server = baseWrite(baseDF()
      .parseRequest(apiName, new StructType().add("value", IntegerType), parsingCheck = "partial")
      .makeReply("value"))
      .start()

    using(server) {
      waitForServer(server)
      val r1 = (1 to 10).map(i =>
        sendStringRequest(client, payload = """{"value": 1}""")
      )

      val r2 = (1 to 10).map(i =>
        sendStringRequest(client, payload = """{"valu111e": 1}""")
      )

      val r3 = (1 to 10).map(i =>
        sendStringRequest(client, payload = """jskdfjkdhdjfdjkh""", targetCode = 400)
      )
      assertLatency(r1, 60)
      assertLatency(r2, 60)
      assertLatency(r3, 60)
      r3.foreach(p => assert(Option(p._1).isEmpty))
    }
  }

  test("can reply to bad requests immediately") {
    val server = baseWrite(baseDF()
      .parseRequest(apiName, new StructType().add("value", IntegerType), parsingCheck = "full")
      .makeReply("value"))
      .start()

    using(server) {
      waitForServer(server)
      val r1 = (1 to 10).map(i =>
        sendStringRequest(client, payload = """{"value": 1}""")
      )

      val r2 = (1 to 10).map(i =>
        sendStringRequest(client, payload = """{"valu111e": 1}""", targetCode = 400)
      )

      val r3 = (1 to 10).map(i =>
        sendStringRequest(client, payload = """jskdfjkdhdjfdjkh""", targetCode = 400)
      )
      assertLatency(r1, 60)
      assertLatency(r2, 60)
      assertLatency(r3, 60)

      (r2 ++ r3).foreach(p => assert(Option(p._1).isEmpty))
    }
  }

  test("can reply from the middle of the pipeline") {
    val server = baseWrite(baseDF()
      .parseRequest(apiName, new StructType().add("value", IntegerType))
      .withColumn("didReply",
        when(col("value").isNull,
          ServingUDFs.sendReplyUDF(
            lit(apiName),
            ServingUDFs.makeReplyUDF(lit(null), NullType, code = lit(400), reason = lit("JSON Parsing Error")),
            col("id")
          )
        )
          .otherwise(lit(null)))
      .filter(col("didReply").isNull)
      .makeReply("value"))
      .start()

    using(server) {
      waitForServer(server)
      val r1 = (1 to 100).map(i =>
        sendStringRequest(client, payload = """{"value": 1}""")
      )

      val r2 = (1 to 100).map(i =>
        sendStringRequest(client, payload = """{"valu111e": 1}""", targetCode = 400)
      )

      val r3 = (1 to 100).map(i =>
        sendStringRequest(client, payload = """jskdfjkdhdjfdjkh""", targetCode = 400)
      )
      assertLatency(r1, 60)
      assertLatency(r2, 60)
      assertLatency(r3, 60)

      (r2 ++ r3).foreach(p => assert(Option(p._1).isEmpty))
    }
  }

  test("fault tolerance") {
    val r = scala.util.Random
    val flakyUDF = udf({ x: Int =>
      val d = r.nextDouble()
      if (d < .03 && x == 1) {
        println(s"failing on partition $x")
        throw new RuntimeException()
      } else {
        println(s"passing on partition $x")
        d
      }
    }, DoubleType)

    Thread.sleep(1000)
    val server = baseWrite(baseDF(epochLength = 1000)
      .withColumn("foo", flakyUDF(col("id.partitionId")))
      .makeReply("foo"))
      .start()

    using(server) {
      waitForServer(server)
      val responsesWithLatencies = (1 to 300).map(i =>
        sendStringRequest(client)
      )
      assertLatency(responsesWithLatencies, 200)
      println(HTTPSourceStateHolder.serviceInfoJson(apiName))
    }

  }

  test("joins") {
    //TODO figure out how to get spark streaming to shuffle for real
    import session.implicits._

    val df2 = (0 until 1000)
      .map(i => (i, i.toString + "_foo"))
      .toDF("key", "value").cache()

    val df1 = baseDF(1).parseRequest(apiName, new StructType().add("data", IntegerType))

    df1.printSchema()

    val sdf = df2.join(df1, col("key") === col("data"))
      .makeReply("value")

    println(df2.rdd.getNumPartitions)
    //println(df1.rdd.getNumPartitions)
    val server = baseWrite(sdf).start()

    using(server) {
      Thread.sleep(10000)
      val responsesWithLatencies = (1 to 100).map { i =>
        val ret = sendJsonRequest(client, i)
        ret
      }

      val latencies = responsesWithLatencies.drop(3).map(_._2.toInt).toList
      val meanLatency = mean(latencies)
      val stdLatency = stddev(latencies, meanLatency)
      println(s"Latency = $meanLatency +/- $stdLatency")
      assert(meanLatency < 20)

      println(HTTPSourceStateHolder.serviceInfoJson(apiName))
    }
  }

  test("flaky connection") {
    val server = basePipeline()
      .trigger(Trigger.Continuous("1 second")) // only change in query
      .start()

    using(server) {
      Thread.sleep(10000)

      lazy val requestTimeout2 = 1000
      lazy val requestConfig2: RequestConfig = RequestConfig.custom()
        .setConnectTimeout(requestTimeout2)
        .setConnectionRequestTimeout(requestTimeout2)
        .setSocketTimeout(requestTimeout2)
        .build()

      lazy val client2: CloseableHttpClient = HttpClientBuilder
        .create().setDefaultRequestConfig(requestConfig2).build()

      val futures = (1 to 100).map(i => Future(sendFileRequest(client2)))
      val responsesWithLatencies = futures.flatMap(f => Try(Await.result(f, Duration(5, TimeUnit.SECONDS))).toOption)
      Thread.sleep(6000)
      assert(server.isActive)
      assert(responsesWithLatencies.length >= 0)
      assertLatency(responsesWithLatencies, 2000)
    }
  }

}
