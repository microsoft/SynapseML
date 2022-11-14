// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.split1

import com.microsoft.azure.synapse.ml.core.env.StreamUtilities
import com.microsoft.azure.synapse.ml.core.env.StreamUtilities.using
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.io.http.HTTPTransformer
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Dataset}
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

}
