// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}

import com.microsoft.ml.spark.FileUtilities.File
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{BasicResponseHandler, CloseableHttpClient, HttpClientBuilder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.functions.{col, length}
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery}
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.parsing.json.JSONObject

//TODO add tests for shuffles
class DistributedHTTPSuite extends TestBase with FileReaderUtils {

  Logger.getLogger(classOf[DistributedHTTPSource]).setLevel(Level.DEBUG)
  Logger.getLogger(classOf[JVMSharedServer]).setLevel(Level.DEBUG)

  def createServer(): DataStreamWriter[Row] = {
    println(classOf[DistributedHTTPSourceProvider].getName)
    session.readStream.format(classOf[DistributedHTTPSourceProvider].getName)
      .option("host", "localhost")
      .option("port", 8889)
      .option("name", "foo")
      .option("maxPartitions",5)
      .load()
      .withColumn("newCol", length(col("value")))
      .writeStream
      .format(classOf[DistributedHTTPSinkProvider].getName)
      .option("name", "foo")
      .queryName("foo")
      .option("replyCol", "newCol")
      .option("checkpointLocation",
        new File(tmpDir.toFile, s"checkpoints-${UUID.randomUUID()}").toString)
  }

  def waitForServer(server: StreamingQuery, checkEvery: Int = 100, maxTimeWaited: Int = 10000): Unit = {
    var waited = 0
    while (waited < maxTimeWaited) {
      if (server.recentProgress.length > 0) return
      Thread.sleep(checkEvery.toLong)
      waited += checkEvery
    }
  }

  test("standard client", TestBase.Extended) {
    val server = createServer().start()
    val client = HttpClientBuilder.create().build()

    def sendRequest(map: Map[String, Any], port: Int): String = {
      val post = new HttpPost(s"http://localhost:$port/foo")
      val params = new StringEntity(JSONObject(map).toString())
      post.addHeader("content-type", "application/json")
      post.setEntity(params)
      val res = client.execute(post)
      val out = new BasicResponseHandler().handleResponse(res)
      res.close()
      out
    }

    waitForServer(server)

    val responses = List(
      sendRequest(Map("foo" -> 1, "bar" -> "here"), 8889),
      sendRequest(Map("foo" -> 2, "bar" -> "heree"), 8889),
      sendRequest(Map("foo" -> 3, "bar" -> "hereee"), 8889),
      sendRequest(Map("foo" -> 4, "bar" -> "hereeee"), 8889)
    )
    val correctResponses = List(27, 28, 29, 30).map(n => "{\"newCol\":" + n + "}")

    assert(responses === correctResponses)

    (1 to 20).map(i => sendRequest(Map("foo" -> 1, "bar" -> "here"), 8889))
      .foreach(resp => assert(resp === "{\"newCol\":27}"))

    server.stop()
    client.close()
  }

  test("spark client", TestBase.Extended) {
    val server = createServer().start()

    def sendRequest(client: CloseableHttpClient, map: Map[String, Any], port: Int): String = {
      val post = new HttpPost(s"http://localhost:$port/foo")
      val params = new StringEntity(JSONObject(map).toString())
      post.addHeader("content-type", "application/json")
      post.setEntity(params)
      val res = client.execute(post)
      val out = new BasicResponseHandler().handleResponse(res)
      res.close()
      out
    }

    waitForServer(server)
    //TODO investigate why this hangs with 8 partitions
    val rdd = sc.parallelize(1 to 80, 5).mapPartitions{it =>
      val client = HttpClientBuilder.create().build()
      val res = it.toList.map(row =>
        sendRequest(client, Map("foo" -> 1, "bar" -> "here"), 8889))
      client.close()
      res.toIterator
    }
    val responses = rdd.collect()
    println(responses.toList)
    server.stop()
  }

  test("async client", TestBase.Extended) {
    val server = createServer().start()
    val client = HttpClientBuilder.create().build()

    implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(30))

    def sendRequest(map: Map[String, Any], port: Int): Future[String] = {
      Future {
        val post = new HttpPost(s"http://localhost:$port/foo")
        val params = new StringEntity(JSONObject(map).toString())
        post.addHeader("content-type", "application/json")
        post.setEntity(params)
        val res = client.execute(post)
        val out = new BasicResponseHandler().handleResponse(res)
        res.close()
        out
      }
    }

    waitForServer(server)

    val futures = List(
      sendRequest(Map("foo" -> 1, "bar" -> "here"), 8889),
      sendRequest(Map("foo" -> 2, "bar" -> "heree"), 8889),
      sendRequest(Map("foo" -> 3, "bar" -> "hereee"), 8889),
      sendRequest(Map("foo" -> 4, "bar" -> "hereeee"), 8889)
    )

    val responses = futures.map(f => Await.result(f, Duration.Inf))
    val correctResponses = List(27, 28, 29, 30).map(n => "{\"newCol\":" + n + "}")

    assert(responses === correctResponses)

    (1 to 20).map(i => sendRequest(Map("foo" -> 1, "bar" -> "here"), 8889))
      .foreach{f =>
        val resp = Await.result(f, Duration(5, TimeUnit.SECONDS))
        assert(resp === "{\"newCol\":27}")
      }

    server.stop()
    client.close()
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
