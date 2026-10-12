// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.vision

import com.microsoft.azure.synapse.ml.core.spark.FluentAPI._
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.commons.io.IOUtils
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.Outcome
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.net.{InetSocketAddress, URI}
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._

class RecognizeTextSuite extends TransformerFuzzing[RecognizeText] {

  import spark.implicits._

  private case class Request(method: String, uri: URI, contentType: String, body: Array[Byte])

  private val requests = new ConcurrentLinkedQueue[Request]()
  // Generated constructor tests also call testObjects, outside the ScalaTest HTTP fixture.
  private var endpoint = "http://127.0.0.1:0"
  private val imageUrl = "https://example.org/quote.jpg"
  private val imageBytes = Array[Byte](1, 2, 3)
  private val response =
    """{"status":"Succeeded","recognitionResult":{"lines":[
      |{"boundingBox":[0,0,20,0,20,10,0,10],"text":"hello world",
      |"words":[{"boundingBox":[0,0,20,0,20,10,0,10],"text":"hello world"}]}]}}""".stripMargin
  private val retired = """{"error":{"code":"ApiVersionRetired","message":"This API version is retired."}}"""

  override protected def withFixture(test: NoArgTest): Outcome = {
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    val baseUrl = s"http://127.0.0.1:${server.getAddress.getPort}"
    server.createContext("/", new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        try {
          val uri = exchange.getRequestURI
          requests.add(Request(exchange.getRequestMethod, uri,
            exchange.getRequestHeaders.getFirst("Content-Type"), IOUtils.toByteArray(exchange.getRequestBody)))
          val (status, body) = (exchange.getRequestMethod, uri.getPath) match {
            case ("POST", "/vision/v2.0/recognizeText" | "/explicit/recognizeText") =>
              exchange.getResponseHeaders.add("Operation-Location", baseUrl + "/operations/legacy-result")
              (202, "{}")
            case ("GET", "/operations/legacy-result") => (200, response)
            case _ => (410, retired)
          }
          val bytes = body.getBytes(StandardCharsets.UTF_8)
          exchange.getResponseHeaders.add("Content-Type", "application/json")
          exchange.sendResponseHeaders(status, bytes.length)
          exchange.getResponseBody.write(bytes)
        } finally {
          exchange.close()
        }
      }
    })
    requests.clear()
    val previousEndpoint = endpoint
    endpoint = baseUrl
    server.start()
    try {
      super.withFixture(test)
    } finally {
      server.stop(0)
      endpoint = previousEndpoint
    }
  }

  private def baseStage: RecognizeText = new RecognizeText()
    .setEndpoint(endpoint + "/")
    .setMode("Printed")
    .setOutputCol("ocr")
    .setErrorCol("error")
    .setConcurrency(1)
    .setInitialPollingDelay(0)
    .setPollingDelay(0)
    .setMaxPollingRetries(1)

  private def rt: RecognizeText = baseStage.setImageUrlCol("url")

  private def bytesRT: RecognizeText = baseStage.setImageBytesCol("imageBytes")

  private def df: DataFrame = Seq(imageUrl).toDF("url").coalesce(1)

  private def bytesDF: DataFrame = Seq(imageBytes).toDF("imageBytes").coalesce(1)

  private def assertLegacyResult(stage: RecognizeText, input: DataFrame): Unit = {
    val row = input.mlTransform(stage, RecognizeText.flatten("ocr", "text")).head()
    assert(row.isNullAt(row.fieldIndex("error")), row.toString)
    assert(row.getAs[Row]("ocr").schema.simpleString == RTResponse.schema.simpleString)
    assert(row.getAs[String]("text") == "hello world")
    assert(requests.size() == 2)
    val sent = requests.asScala.toSeq
    assert(sent.head.method == "POST")
    assert(sent.head.uri.getQuery == "mode=Printed")
    assert(sent.last.method == "GET")
    assert(sent.last.uri.getPath == "/operations/legacy-result")
  }

  test("legacy URL input polls and flattens recognitionResult") {
    assertLegacyResult(rt, df)
    val sent = requests.peek()
    assert(sent.uri.getPath == "/vision/v2.0/recognizeText")
    assert(sent.contentType.startsWith("application/json"))
    assert(new String(sent.body, StandardCharsets.UTF_8).parseJson == Map("url" -> imageUrl).toJson)
  }

  test("legacy byte input polls and flattens recognitionResult") {
    assertLegacyResult(bytesRT, bytesDF)
    val sent = requests.peek()
    assert(sent.uri.getPath == "/vision/v2.0/recognizeText")
    assert(sent.contentType == "application/octet-stream")
    assert(sent.body.sameElements(imageBytes))
  }

  test("an explicit legacy URL survives copy after setting location") {
    val stage = rt.setLocation("eastus").setUrl(endpoint + "/explicit/recognizeText")
      .copy(ParamMap.empty).asInstanceOf[RecognizeText]
    assertLegacyResult(stage, df)
    assert(requests.peek().uri.getPath == "/explicit/recognizeText")
  }

  test("a retired legacy endpoint keeps its 410 error visible without polling or retry") {
    val row = rt.setUrl(endpoint + "/retired/recognizeText").transform(df).head()
    assert(row.isNullAt(row.fieldIndex("ocr")))
    val error = row.getAs[Row]("error")
    assert(error.getAs[Row]("status").getAs[Int]("statusCode") == 410)
    assert(error.getAs[String]("response").parseJson.asJsObject.fields("error")
      .asJsObject.fields("code") == JsString("ApiVersionRetired"))
    assert(requests.size() == 1)
    assert(requests.peek().method == "POST")
    assert(requests.peek().uri.getQuery == "mode=Printed")
  }

  override def testObjects(): Seq[TestObject[RecognizeText]] =
    Seq(new TestObject(rt, df), new TestObject(bytesRT, bytesDF))

  override def reader: MLReadable[_] = RecognizeText
}
