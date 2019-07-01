// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.io.http

import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit, TimeoutException}

import com.microsoft.ml.spark.core.env.FileUtilities.File
import com.microsoft.ml.spark.io.http.HTTPSchema.string_to_response
import com.microsoft.ml.spark.core.env.FileUtilities
import com.microsoft.ml.spark.core.test.base.TestBase
import org.apache.commons.io.IOUtils
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{FileEntity, StringEntity}
import org.apache.http.impl.client.{BasicResponseHandler, CloseableHttpClient, HttpClientBuilder}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.streaming.{DistributedHTTPSinkProvider, DistributedHTTPSourceProvider}
import org.apache.spark.sql.functions.{col, length}
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.Assertion

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.parsing.json.JSONObject

trait HTTPTestUtils extends WithFreeUrl {

  def waitForServer(server: StreamingQuery, maxTimeWaited: Int = 20000, checkEvery: Int = 100): Unit = {
    var waited = 0
    while (waited < maxTimeWaited) {
      if (!server.isActive) throw server.exception.get
      if (server.recentProgress.length > 1) return
      Thread.sleep(checkEvery.toLong)
      waited += checkEvery
    }
    throw new TimeoutException(s"Server Did not start within $maxTimeWaited ms")
  }

  def sendStringRequest(client: CloseableHttpClient,
                        url: String = url,
                        payload: String = "foo",
                        targetCode: Int = 200): (String, Double) = {
    val post = new HttpPost(url)
    val e = new StringEntity(payload)
    post.setEntity(e)
    println("request sent")
    val t0 = System.nanoTime()
    val res = client.execute(post)
    val t1 = System.nanoTime()

    assert(targetCode == res.getStatusLine.getStatusCode)
    val out = if (targetCode == res.getStatusLine.getStatusCode && !targetCode.toString.startsWith("2")) {
      null
    } else {
      new BasicResponseHandler().handleResponse(res)
    }
    res.close()
    println("request suceeded")
    (out, (t1 - t0) / 1e6)
  }

  def using(c: StreamingQuery)(t: => Unit): Unit = {
    try {
      t
    } finally {
      c.stop()
    }
  }

  lazy val requestTimeout = 600000

  lazy val requestConfig: RequestConfig = RequestConfig.custom()
    .setConnectTimeout(requestTimeout)
    .setConnectionRequestTimeout(requestTimeout)
    .setSocketTimeout(requestTimeout)
    .build()

  lazy val client: CloseableHttpClient = HttpClientBuilder
    .create().setDefaultRequestConfig(requestConfig).build()

  def sendJsonRequest(client: CloseableHttpClient, payload: Int): (String, Long) = {
    val post = new HttpPost(url)
    val e = new StringEntity("{\"data\":" + s"$payload}")
    post.setEntity(e)
    println("request sent")
    val t0 = System.currentTimeMillis()
    val res = client.execute(post)
    val t1 = System.currentTimeMillis()
    val out = new BasicResponseHandler().handleResponse(res)
    res.close()
    println("request suceeded")
    (out, t1 - t0)
  }

  def sendStringRequestAsync(client: CloseableHttpClient): Future[(String, Double)] = {
    Future {
      sendStringRequest(client)
    }
  }

  def sendJsonRequest(client: CloseableHttpClient, map: Map[String, Any]): String = {
    val post = new HttpPost(url)
    val params = new StringEntity(JSONObject(map).toString())
    post.addHeader("content-type", "application/json")
    post.setEntity(params)
    val res = client.execute(post)
    val out = new BasicResponseHandler().handleResponse(res)
    res.close()
    out
  }

  def sendJsonRequestAsync(client: CloseableHttpClient, map: Map[String, Any]): Future[String] = {
    Future {
      sendJsonRequest(client, map)
    }
  }

  def sendFileRequest(client: CloseableHttpClient): (String, Double) = {
    val post = new HttpPost(url)
    val e = new FileEntity(new File(
      s"${sys.env("DATASETS_HOME")}/Images/Grocery/testImages/WIN_20160803_11_28_42_Pro.jpg"))
    post.setEntity(e)
    val t0 = System.nanoTime()
    val res = client.execute(post)
    val out = new BasicResponseHandler().handleResponse(res)
    res.close()
    val t1 = System.nanoTime()
    (out, (t1 - t0)/1e6)
  }

  def mean(xs: List[Int]): Double = xs match {
    case Nil => 0.0
    case ys => ys.sum / ys.size.toDouble
  }

  def stddev(xs: List[Int], avg: Double): Double = xs match {
    case Nil => 0.0
    case ys => math.sqrt((0.0 /: ys) {
      (a, e) => a + math.pow(e - avg, 2.0)
    } / xs.size)
  }

  lazy implicit val ec = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  def assertLatency(responsesWithLatencies: Seq[(String, Double)], cutoff: Double): Unit = {
    val latencies = responsesWithLatencies.drop(3).map(_._2.toInt).toList
    //responsesWithLatencies.foreach(r => println(r._1))
    val meanLatency = mean(latencies)
    val stdLatency = stddev(latencies, meanLatency)
    println(s"Latency = $meanLatency +/- $stdLatency")
    assert(meanLatency < cutoff)
    ()
  }

}

// TODO add tests for shuffles
class DistributedHTTPSuite extends TestBase with HTTPTestUtils {
  import ServingImplicits._

  // Logger.getRootLogger.setLevel(Level.WARN)
  // Logger.getLogger(classOf[DistributedHTTPSource]).setLevel(Level.INFO)
  // Logger.getLogger(classOf[JVMSharedServer]).setLevel(Level.INFO)

  def createServer(): DataStreamWriter[Row] = {
    println(classOf[DistributedHTTPSourceProvider].getName)
    session.readStream.format(classOf[DistributedHTTPSourceProvider].getName)
      .option("host", host)
      .option("port", port.toLong)
      .option("path", "foo")
      .option("maxPartitions", 5)
      .load()
      .withColumn("contentLength", col("request.entity.contentLength"))
      .withColumn("reply", string_to_response(col("contentLength").cast(StringType)))
      .writeStream
      .format(classOf[DistributedHTTPSinkProvider].getName)
      .option("name", "foo")
      .queryName("foo")
      .option("replyCol", "reply")
      .option("checkpointLocation",
        new File(tmpDir.toFile, s"checkpoints-${UUID.randomUUID()}").toString)
  }

  test("standard client", TestBase.Extended) {
    val server = createServer().start()
    using(server){
      waitForServer(server)
      val responses = List(
        sendJsonRequest(client, Map("foo" -> 1, "bar" -> "here")),
        sendJsonRequest(client, Map("foo" -> 2, "bar" -> "heree")),
        sendJsonRequest(client, Map("foo" -> 3, "bar" -> "hereee")),
        sendJsonRequest(client, Map("foo" -> 4, "bar" -> "hereeee"))
      )
      val correctResponses = List(27, 28, 29, 30).map(_.toString)

      assert(responses === correctResponses)

      (1 to 20).map(i => sendJsonRequest(client, Map("foo" -> 1, "bar" -> "here")))
        .foreach(resp => assert(resp === "27"))
    }
  }

  test("test implicits", TestBase.Extended) {
    val server = session.readStream.server
      .address(host, port, "foo")
      .option("maxPartitions", 3)
      .load()
      .withColumn("contentLength", col("request.entity.contentLength"))
      .withColumn("reply", string_to_response(col("contentLength").cast(StringType)))
      .writeStream
      .server
      .option("name", "foo")
      .queryName("foo")
      .option("checkpointLocation",
        new File(tmpDir.toFile, s"checkpoints-${UUID.randomUUID()}").toString)
      .start()

    using(server){
      waitForServer(server)
      val responses = List(
        sendJsonRequest(client, Map("foo" -> 1, "bar" -> "here")),
        sendJsonRequest(client, Map("foo" -> 2, "bar" -> "heree")),
        sendJsonRequest(client, Map("foo" -> 3, "bar" -> "hereee")),
        sendJsonRequest(client, Map("foo" -> 4, "bar" -> "hereeee"))
      )
      val correctResponses = List(27, 28, 29, 30).map(_.toString)

      assert(responses === correctResponses)

      (1 to 20).map(i => sendJsonRequest(client, Map("foo" -> 1, "bar" -> "here")))
        .foreach(resp => assert(resp === "27"))
    }

  }

  test("test implicits 2", TestBase.Extended) {

    val server = session.readStream.server
      .address(host, port, "foo")
      .option("maxPartitions", 5)
      .load()
      .parseRequest(apiName, BinaryType)
      .withColumn("length", length(col("bytes")))
      .makeReply("length")
      .writeStream
      .server
      .replyTo("foo")
      .queryName("foo")
      .option("checkpointLocation",
        new File(tmpDir.toFile, s"checkpoints-${UUID.randomUUID()}").toString)
      .start()

    using(server){
      waitForServer(server)
      val responsesWithLatencies = (1 to 100).map(i => sendFileRequest(client))

      val latencies = responsesWithLatencies.drop(3).map(_._2.toInt).toList
      val meanLatency = mean(latencies)
      val stdLatency = stddev(latencies, meanLatency)
      println(s"Latency = $meanLatency +/- $stdLatency")

      responsesWithLatencies.foreach(s => assert(s._1 === "{\"length\":279186}"))
    }

  }

  test("test implicits 2 distributed", TestBase.Extended) {
    import ServingImplicits._

    val server = session.readStream.distributedServer
      .address(host, port, "foo")
      .option("maxPartitions", 5)
      .load()
      .parseRequest(apiName, BinaryType)
      .withColumn("length", length(col("bytes")))
      .makeReply("length")
      .writeStream
      .distributedServer
      .replyTo("foo")
      .queryName("foo")
      .option("checkpointLocation",
        new File(tmpDir.toFile, s"checkpoints-${UUID.randomUUID()}").toString)
      .start()

    using(server){
      waitForServer(server)
      val responsesWithLatencies = (1 to 100).map(i =>
        sendFileRequest(client)
      )

      val latencies = responsesWithLatencies.drop(3).map(_._2.toInt).toList
      val meanLatency = mean(latencies)
      val stdLatency = stddev(latencies, meanLatency)
      println(s"Latency = $meanLatency +/- $stdLatency")

      responsesWithLatencies.foreach(s => assert(s._1 === "{\"length\":279186}"))
    }

  }

  test("python client", TestBase.Extended) {
    val server = createServer().start()

    using(server){
      waitForServer(server)

      val pythonClientCode =
        s"""import requests
           |import threading
           |import time
           |
         |exitFlag = 0
           |s = requests.Session()
           |
         |class myThread(threading.Thread):
           |    def __init__(self, threadID):
           |        threading.Thread.__init__(self)
           |        self.threadID = threadID
           |
         |    def run(self):
           |        print("Starting " + str(self.threadID))
           |        r = s.post("$url",
           |                          data={"number": 12524, "type": "issue", "action": "show"},
           |                          headers = {"content-type": "application/json"},
           |                          timeout=15)
           |
         |        assert r.status_code==200
           |        print("Exiting {} with code {}".format(self.threadID, r.status_code))
           |
         |
         |threads = []
           |for i in range(4):
           |    # Create new threads
           |    t = myThread(i)
           |    t.start()
           |    threads.append(t)
           |
         |""".stripMargin

      val pythonFile = new File(tmpDir.toFile, "pythonClient.py")
      FileUtilities.writeFile(pythonFile, pythonClientCode)

      val processes = (1 to 50).map(_ =>
        Runtime.getRuntime.exec(s"python ${pythonFile.getAbsolutePath}")
      )

      processes.foreach { p =>
        p.waitFor
        val output = IOUtils.toString(p.getInputStream)
        val error = IOUtils.toString(p.getErrorStream)
        assert(error === "")
      }
    }

  }

  test("async client", TestBase.Extended) {
    val server = createServer().start()

    using(server){
      waitForServer(server)

      val futures = List(
        sendJsonRequestAsync(client, Map("foo" -> 1, "bar" -> "here")),
        sendJsonRequestAsync(client, Map("foo" -> 2, "bar" -> "heree")),
        sendJsonRequestAsync(client, Map("foo" -> 3, "bar" -> "hereee")),
        sendJsonRequestAsync(client, Map("foo" -> 4, "bar" -> "hereeee"))
      )

      val responses = futures.map(f => Await.result(f, Duration.Inf))
      val correctResponses = List(27, 28, 29, 30).map(_.toString)

      assert(responses === correctResponses)

      (1 to 20).map(i => sendJsonRequestAsync(client, Map("foo" -> 1, "bar" -> "here")))
        .foreach { f =>
          val resp = Await.result(f, Duration(5, TimeUnit.SECONDS))
          assert(resp === "27")
        }
    }
  }

  test("State can be saved in a Shared singleton") {
    object Holder extends Serializable {

      class FooHolder {
        var state = 0

        def increment(): Unit = synchronized {
          state += 1
        }
      }

      val foo = SharedSingleton {
        new FooHolder
      }

      import session.sqlContext.implicits._

      val df: DataFrame = session.sparkContext.parallelize(Seq(Tuple1("placeholder")))
        .toDF("plcaholder")
        .mapPartitions { _ =>
          foo.get.increment()
          Iterator(Row(foo.get.state))
        }(RowEncoder(new StructType().add("state", IntegerType))).cache()
      val states1: Array[Row] = df.collect()

      val df2: DataFrame = df.mapPartitions { _ =>
        Iterator(Row(foo.get.state))
      }(RowEncoder(new StructType().add("state", IntegerType)))
      val states2: Array[Row] = df2.collect()
      assert(states2.forall(_.getInt(0) === states2.length))
    }
  }

}
