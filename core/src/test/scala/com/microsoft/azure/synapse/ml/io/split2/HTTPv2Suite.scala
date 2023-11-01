// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.split2

import com.microsoft.azure.synapse.ml.core.test.base.TestBase.getSession
import com.microsoft.azure.synapse.ml.core.test.base.{Flaky, TestBase}
import com.microsoft.azure.synapse.ml.io.IOImplicits._
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.spark.injections.UDFUtils
import org.apache.spark.sql.execution.streaming.ServingUDFs
import org.apache.spark.sql.execution.streaming.continuous._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.File
import java.util.UUID
import scala.concurrent.{Await, Future}
import scala.util.Try
import org.apache.log4j.{LogManager, Level}

// scalastyle:off null
class HTTPv2Suite extends TestBase
  with Flaky
  with HTTPTestUtils {


  override def beforeAll(): Unit = {
    TestBase.resetSparkSession(numRetries = 20)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    TestBase.resetSparkSession()
    super.afterAll()
  }

  override lazy val spark: SparkSession = getSession(s"$this", numRetries = 20).newSession()

  def baseDF(numPartitions: Int = 4,
             apiName: String = apiName,
             apiPath: String = apiPath,
             port: Int = port,
             epochLength: Long = 5000): DataFrame = {
    spark
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

  def waitForBuild(): Unit = Thread.sleep(3000)

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
    waitForBuild()
    val newPort = getFreePort
    val server = basePipeline(port = newPort)
      .trigger(Trigger.Continuous("10 seconds")) // only change in query
      .start()
    using(server) {
      Thread.sleep(3000)
      assertLatency((1 to 400).map(i => sendStringRequest(url = url(newPort))), 10)
      println(HTTPSourceStateHolder.serviceInfoJson(apiName))
    }
  }

  test("async continuous") {
    waitForBuild()
    val newPort = getFreePort
    val server = basePipeline(port = newPort)
      .trigger(Trigger.Continuous("1 seconds")) // only change in query
      .start()

    using(server) {
      Thread.sleep(3000)
      val futures = (1 to 100).map(i => sendStringRequestAsync(url = url(newPort)))
      val responsesWithLatencies = futures.map(Await.result(_, requestDuration))
      assertLatency(responsesWithLatencies, 2000)
    }

  }

  test("async microbatch") {
    waitForBuild()
    val newPort = getFreePort
    val server = basePipeline(port = newPort)
      .start()

    using(server) {
      Thread.sleep(3000)
      val futures = (1 to 100).map(i => sendStringRequestAsync(url = url(newPort)))
      val responsesWithLatencies = futures.map(Await.result(_, requestDuration))
      assertLatency(responsesWithLatencies, 2000)
      println(HTTPSourceStateHolder.serviceInfoJson(apiName))
    }

  }

  test("double pipeline") {
    waitForBuild()
    val newPort = getFreePort
    val server = basePipeline(port = newPort)
      .start()

    using(server) {
      waitForServer(server)
      (1 to 100).foreach(i => sendStringRequest(url = url(newPort)))
      println(HTTPSourceStateHolder.serviceInfoJson(apiName))
    }

    Thread.sleep(5000)
    val server2 = basePipeline(port = newPort)
      .start()

    using(server2) {
      waitForServer(server2)
      (1 to 100).foreach(i => sendStringRequest(url = url(newPort)))
      println(HTTPSourceStateHolder.serviceInfoJson(apiName))
    }

  }

  test("microbatch") {
    waitForBuild()
    val newPort = getFreePort
    val server = basePipeline(port = newPort)
      .start()

    using(server) {
      waitForServer(server)
      sendStringRequest(url = url(newPort))
      //      val responsesWithLatencies = (1 to 100).map(_ => sendStringRequest(client, url = url(newPort)))
      //      Thread.sleep(1000)
      //      (1 to 100).foreach(_ => sendStringRequest(client, url = url(newPort)))
      //      assertLatency(responsesWithLatencies, 20)
      //      println(HTTPSourceStateHolder.serviceInfoJson(apiName))
    }

  }

  test("errors if 2 services are made at the same time with the same name") {
    waitForBuild()
    val newPort = getFreePort
    val server1 = basePipeline(2, apiName = "n1", port = newPort)
      .start()
    waitForBuild()
    assertThrows[AssertionError] {
      val server2 = basePipeline(2, "bar", apiName = "n1", port = newPort)
        .start()
      Thread.sleep(1000)
      throw server2.exception.get.cause
    }

    println("stopping server")
    server1.stop()
  }

  test("two services can run independently") {
    waitForBuild()
    val port1 = getFreePort
    val server1 = basePipeline(numPartitions = 1, name = "q1",
      apiPath = "foo", apiName = "n1", port = port1).start()
    using(server1) {
      waitForBuild()
      val port2 = getFreePort
      val server2 = basePipeline(numPartitions = 1, name = "q2",
        apiPath = "bar", apiName = "n2", port = port2).start()
      waitForServer(server1)
      using(server2) {
        waitForServer(server2)
        val l1 = (1 to 100).map(_ => sendStringRequest(s"http://$host:$port1/foo"))
        val l2 = (1 to 100).map(_ => sendStringRequest(s"http://$host:$port2/bar"))
        assertLatency(l1, 20)
        assertLatency(l2, 20)

        println(HTTPSourceStateHolder.serviceInfoJson("n1"))
        println(HTTPSourceStateHolder.serviceInfoJson("n2"))
      }
    }
  }

  test("forwarding ports to vm error check") {
    waitForBuild()
    val newPort = getFreePort
    try {
      val server = baseWrite(spark
        .readStream
        .format(classOf[HTTPSourceProviderV2].getName)
        .address(host, newPort, apiPath)
        .option("name", apiName)
        .option("epochLength", 5000)
        .option("numPartitions", 2)
        .option("forwarding.enabled", true)
        .option("forwarding.sshHost", host)
        .option("forwarding.keySas", "bad key")
        .option("forwarding.username", "sshuser")
        .load()
        .withColumn("foo", col("id.requestId"))
        .makeReply("foo")).start()

      using(server) {
        Thread.sleep(10000)
      }
    } catch {
      case _: java.net.URISyntaxException => ()
    }
  }

  test("can reply to bad requests immediately partial") {
    waitForBuild()
    val newPort = getFreePort
    val server = baseWrite(baseDF(port = newPort)
      .parseRequest(apiName, new StructType().add("value", IntegerType), parsingCheck = "full")
      .makeReply("value"))
      .start()

    using(server) {
      waitForServer(server)
      val r1 = (1 to 10).map(i =>
        sendStringRequest(payload = """{"value": 1}""", url = url(newPort))
      )

      val r2 = (1 to 10).map(i =>
        sendStringRequest(payload = """{"valu111e": 1}""", targetCode = 400, url = url(newPort))
      )

      val r3 = (1 to 10).map(i =>
        sendStringRequest(payload = """jskdfjkdhdjfdjkh""", targetCode = 400, url = url(newPort))
      )

      assertLatency(r1, 60)
      assertLatency(r2, 60)
      assertLatency(r3, 60)
      r3.foreach(p => assert(Option(p._1).isEmpty))
    }
  }

  test("can reply to bad requests immediately") {
    waitForBuild()
    val newPort = getFreePort
    val server = baseWrite(baseDF(port = newPort)
      .parseRequest(apiName, new StructType().add("value", IntegerType), parsingCheck = "full")
      .makeReply("value"))
      .start()

    using(server) {
      waitForServer(server)
      val r1 = (1 to 10).map(i =>
        sendStringRequest(payload = """{"value": 1}""", url = url(newPort))
      )

      val r2 = (1 to 10).map(i =>
        sendStringRequest(payload = """{"valu111e": 1}""", targetCode = 400, url = url(newPort))
      )

      val r3 = (1 to 10).map(i =>
        sendStringRequest(payload = """jskdfjkdhdjfdjkh""", targetCode = 400, url = url(newPort))
      )
      assertLatency(r1, 60)
      assertLatency(r2, 60)
      assertLatency(r3, 60)

      (r2 ++ r3).foreach(p => assert(Option(p._1).isEmpty))
    }
  }

  test("can reply from the middle of the pipeline") {
    waitForBuild()
    val newPort = getFreePort
    val server = baseWrite(baseDF(port = newPort)
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
        sendStringRequest(payload = """{"value": 1}""", url = url(newPort))
      )

      val r2 = (1 to 100).map(i =>
        sendStringRequest(payload = """{"valu111e": 1}""", targetCode = 400, url = url(newPort))
      )

      val r3 = (1 to 100).map(i =>
        sendStringRequest(payload = """jskdfjkdhdjfdjkh""", targetCode = 400, url = url(newPort))
      )
      assertLatency(r1, 60)
      assertLatency(r2, 60)
      assertLatency(r3, 60)

      (r2 ++ r3).foreach(p => assert(Option(p._1).isEmpty))
    }
  }

  test("fault tolerance") {
    tryWithRetries(Array(0, 100, 100)) { () =>
      waitForBuild()
      val newPort = getFreePort
      val r = scala.util.Random
      val flakyUDF = UDFUtils.oldUdf({ x: Int =>
        val d = r.nextDouble()
        if (d < .02 && x == 1) {
          println(s"failing on partition $x")
          throw new RuntimeException()
        } else {
          //println(s"passing on partition $x")
          d
        }
      }, DoubleType)

      Thread.sleep(1000)
      val server = baseWrite(baseDF(epochLength = 1000, port = newPort)
        .withColumn("foo", flakyUDF(col("id.partitionId")))
        .makeReply("foo"))
        .start()

      using(server) {
        waitForServer(server)
        val responsesWithLatencies = (1 to 300).map(i =>
          sendStringRequest(url = url(newPort))
        )
        assertLatency(responsesWithLatencies, 200)
        println(HTTPSourceStateHolder.serviceInfoJson(apiName))
      }
    }
  }

  test("joins") {
    waitForBuild()
    val newPort = getFreePort
    //TODO figure out how to get spark streaming to shuffle for real
    import spark.implicits._

    val df2 = (0 until 1000)
      .map(i => (i, i.toString + "_foo"))
      .toDF("key", "value").cache()

    val df1 = baseDF(1, port = newPort)
      .parseRequest(apiName, new StructType().add("data", IntegerType))

    df1.printSchema()

    val sdf = df2.join(df1, col("key") === col("data"))
      .makeReply("value")

    println(df2.rdd.getNumPartitions)
    //println(df1.rdd.getNumPartitions)
    val server = baseWrite(sdf).start()

    using(server) {
      Thread.sleep(10000)
      val responsesWithLatencies = (1 to 100).map { i =>
        val ret = sendJsonRequest(i, url = url(newPort))
        ret
      }

      val latencies = responsesWithLatencies.drop(3).map(_._2.toInt).toList
      val meanLatency = mean(latencies)
      val stdLatency = stddev(latencies, meanLatency)
      println(s"Latency = $meanLatency +/- $stdLatency")
      assert(meanLatency < 30)

      println(HTTPSourceStateHolder.serviceInfoJson(apiName))
    }
  }

  test("flaky connection") {
    waitForBuild()
    val newPort = getFreePort
    val server = basePipeline(port = getFreePort)
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

      val futures = (1 to 100).map(i => Future(sendFileRequest(client = client2, url = url(newPort))))
      val responsesWithLatencies = futures.flatMap(f => Try(Await.result(f, requestDuration)).toOption)
      Thread.sleep(6000)
      assert(server.isActive)
      assert(responsesWithLatencies.length >= 0)
      assertLatency(responsesWithLatencies, 2000)
    }
  }

}
