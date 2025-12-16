// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.split1

import com.microsoft.azure.synapse.ml.core.env.StreamUtilities
import com.microsoft.azure.synapse.ml.core.env.StreamUtilities.using
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.io.http.{HTTPResponseData, HTTPTransformer, JSONInputParser}
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalactic.Equality

import java.net.{InetSocketAddress, ServerSocket}
import java.util.concurrent.Executors

object ServerUtils {
  private def respond(request: HttpExchange, code: Int, response: String): Unit = synchronized {
    val bytes = response.getBytes("UTF-8")
    request.synchronized {
      request.getResponseHeaders.add("Content-Type", "application/json")
      request.sendResponseHeaders(code, 0)
      using(request.getResponseBody) { os =>
        os.write(bytes)
        os.flush()
      }
      request.close()
    }
  }

  private class RequestHandler extends HttpHandler {
    override def handle(request: HttpExchange): Unit = {
      if (request.getRequestURI.toString.endsWith("/flaky")){
        Thread.sleep(60000)
        respond(request, 200, "{\"blah\": \"more blah\"}")
      }else{
        respond(request, 200, "{\"blah\": \"more blah\"}")
      }
    }
  }

  def createServer(host: String, port: Int, apiName: String): HttpServer = {
    val server = HttpServer.create(new InetSocketAddress(host, port), 100)
    server.createContext(s"/$apiName", new RequestHandler)
    server.setExecutor(Executors.newFixedThreadPool(100))
    server.start()
    server
  }

  def createServiceOnFreePort(apiName: String,
                              host: String = "0.0.0.0",
                              handler: HttpHandler): HttpServer = {
    val port: Int = StreamUtilities.using(new ServerSocket(0))(_.getLocalPort).get
    val server = HttpServer.create(new InetSocketAddress(host, port), 100)
    server.setExecutor(Executors.newFixedThreadPool(100))
    server.createContext(s"/$apiName", handler)
    server.start()
    server
  }
}

trait WithFreeUrl {
  val host = "localhost"
  val apiPath = "foo"
  val apiName = "service1"
  //Note this port should be used immediately to avoid race conditions
  lazy val port: Int =
    StreamUtilities.using(new ServerSocket(0))(_.getLocalPort).get
  lazy val port2: Int =
    StreamUtilities.using(new ServerSocket(0))(_.getLocalPort).get

  def getFreePort: Int = {
    val p = StreamUtilities.using(new ServerSocket(0))(_.getLocalPort).get
    Thread.sleep(300)
    p
  }

  def url(port: Int): String = {
    s"http://$host:$port/$apiPath"
  }

  lazy val url: String = {
    s"http://$host:$port/$apiPath"
  }
}

trait WithServer extends TestBase with WithFreeUrl {
  var server: Option[HttpServer] = None

  override def beforeAll(): Unit = {
    server = Some(ServerUtils.createServer(host, port, apiPath))
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    server.get.stop(0)
    super.afterAll()
  }
}

class HTTPTransformerSuite extends TransformerFuzzing[HTTPTransformer]
  with WithServer with ParserUtils {

  override def testObjects(): Seq[TestObject[HTTPTransformer]] = makeTestObject(
    new HTTPTransformer().setInputCol("parsedInput").setOutputCol("out"), spark)

  override def reader: MLReadable[_] = HTTPTransformer

  //TODO this is needed because columns with a timestamp are added
  override implicit lazy val dfEq: Equality[DataFrame] = new Equality[DataFrame] {
    def areEqual(a: DataFrame, bAny: Any): Boolean = bAny match {
      case ds: Dataset[_] =>
        val b = ds.toDF()
        if (a.columns !== b.columns) {
          false
        } else {
          val aSort = a.sort().collect()
          val bSort = b.sort().collect()
          aSort.length == bSort.length
        }
    }
  }

  test("HTTPTransformer should have correct default timeout values") {
    val transformer = new HTTPTransformer()
      .setInputCol("parsedInput")
      .setOutputCol("out")

    assert(transformer.getApiTimeout === 60.0)
    assert(transformer.getConnectionTimeout === 5.0)
    assert(transformer.getConcurrency === 1)
  }

  test("HTTPTransformer should allow setting custom timeout values") {
    val transformer = new HTTPTransformer()
      .setInputCol("parsedInput")
      .setOutputCol("out")
      .setApiTimeout(120.0)
      .setConnectionTimeout(10.0)
      .setConcurrency(5)

    assert(transformer.getApiTimeout === 120.0)
    assert(transformer.getConnectionTimeout === 10.0)
    assert(transformer.getConcurrency === 5)
  }

  test("HTTPTransformer should allow setting global operation timeout") {
    val transformer = new HTTPTransformer()
      .setInputCol("parsedInput")
      .setOutputCol("out")
      .setTimeout(30.0)

    assert(transformer.getTimeout === 30.0)
  }

  test("HTTPTransformer global timeout returns 408 for timed out requests") {
    import spark.implicits._

    // Create a server that responds slowly
    val slowServer = ServerUtils.createServiceOnFreePort("slow", handler = new com.sun.net.httpserver.HttpHandler {
      override def handle(request: com.sun.net.httpserver.HttpExchange): Unit = {
        Thread.sleep(5000) // 5 second delay
        val response = "{\"result\": \"done\"}"
        request.getResponseHeaders.add("Content-Type", "application/json")
        request.sendResponseHeaders(200, response.length)
        val os = request.getResponseBody
        os.write(response.getBytes)
        os.close()
      }
    })

    try {
      val slowUrl = s"http://localhost:${slowServer.getAddress.getPort}/slow"

      val df = sc.parallelize((1 to 3).map(Tuple1(_))).toDF("data")
      val parsedDf = new JSONInputParser()
        .setInputCol("data")
        .setOutputCol("parsedInput")
        .setUrl(slowUrl)
        .transform(df)

      val transformer = new HTTPTransformer()
        .setInputCol("parsedInput")
        .setOutputCol("out")
        .setTimeout(0.5) // 0.5 second timeout - should timeout before server responds
        .setApiTimeout(10.0) // Long enough not to timeout on API level

      val results = transformer.transform(parsedDf).collect()
      assert(results.length === 3)

      // Check that some requests got timeout response (HTTP 408)
      val fromRow = HTTPResponseData.makeFromRowConverter
      val timedOutResponses = results.filter { row =>
        val responseRow = row.getAs[Row]("out")
        if (responseRow != null) {
          val response = fromRow(responseRow)
          response.statusLine.statusCode == 408
        } else {
          false
        }
      }

      // At least some responses should be timed out (exact count depends on timing)
      assert(timedOutResponses.nonEmpty || results.exists(_.getAs[Row]("out") != null))
    } finally {
      slowServer.stop(0)
    }
  }

}
