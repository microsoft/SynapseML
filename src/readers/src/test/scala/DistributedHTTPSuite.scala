// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.util.UUID

import com.microsoft.ml.spark.FileUtilities.File
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{BasicResponseHandler, HttpClientBuilder}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.functions.{col, length}
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery}
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.parsing.json.JSONObject

//TODO add tests for shuffles
class DistributedHTTPSuite extends TestBase with FileReaderUtils {

  def createServer(): DataStreamWriter[Row] = {
    session.readStream.format(classOf[DistributedHTTPSourceProvider].getName)
      .option("host", "localhost")
      .option("port", 8889)
      .option("name", "foo")
      .load()
      .withColumn("newCol", length(col("value")))
      .writeStream
      .format(classOf[DistributedHTTPSinkProvider].getName)
      .option("name", "foo")
      .queryName("foo")
      .option("replyCol", "newCol")
      .option("checkpointLocation", new File(tmpDir.toFile, s"checkpoints-${UUID.randomUUID()}").toString)
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

    server.stop()
    client.close()
  }

  test("async client", TestBase.Extended) {
    val server = createServer().start()
    val client = HttpClientBuilder.create().build()
    implicit val ec: ExecutionContextExecutor = ExecutionContext.global

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
    //.map(_.get().getResponseBody)
    val correctResponses = List(27, 28, 29, 30).map(n => "{\"newCol\":" + n + "}")

    assert(responses === correctResponses)

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
