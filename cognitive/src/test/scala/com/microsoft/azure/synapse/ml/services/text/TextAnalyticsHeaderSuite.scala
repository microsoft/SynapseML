// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.text

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Locale
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import scala.collection.JavaConverters._

class TextAnalyticsHeaderSuite extends TestBase {

  import spark.implicits._

  private case class Request(method: String, headers: Map[String, String], body: JsObject)

  private val keyHeader = "ocp-apim-subscription-key"

  private def responseFor(body: JsObject, health: Boolean): JsObject = {
    val documents = body.fields("documents").convertTo[Vector[JsValue]].map { value =>
      val document = value.asJsObject
      val common = Map(
        "id" -> document.fields("id"),
        "warnings" -> JsArray()
      )
      val fields = if (health) {
        Map(
          "entities" -> JsArray(JsObject(
            "offset" -> JsNumber(0),
            "length" -> JsNumber(document.fields("text").convertTo[String].length),
            "text" -> document.fields("text"),
            "category" -> JsString("test"),
            "confidenceScore" -> JsNumber(1)
          )),
          "relations" -> JsArray()
        )
      } else {
        Map(
          "sentiment" -> document.fields("text"),
          "confidenceScores" -> JsObject(
            "positive" -> JsNumber(1), "neutral" -> JsNumber(0), "negative" -> JsNumber(0)
          ),
          "sentences" -> JsArray()
        )
      }
      JsObject(common ++ fields)
    }
    val results = JsObject("documents" -> JsArray(documents), "errors" -> JsArray(),
      "modelVersion" -> JsString("local-test"))
    if (health) {
      JsObject("status" -> JsString("succeeded"), "results" -> results, "errors" -> JsArray())
    } else {
      results
    }
  }

  private def withServer(testCode: (String, ConcurrentLinkedQueue[Request]) => Unit): Unit = {
    val requests = new ConcurrentLinkedQueue[Request]()
    val jobs = new ConcurrentHashMap[String, String]()
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    val baseUrl = s"http://127.0.0.1:${server.getAddress.getPort}"
    server.createContext("/", new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        try {
          val method = exchange.getRequestMethod
          val body = if (method == "POST") {
            IOUtils.toString(exchange.getRequestBody, StandardCharsets.UTF_8).parseJson.asJsObject
          } else {
            JsObject()
          }
          val headers = exchange.getRequestHeaders.asScala.map { case (name, values) =>
            name.toLowerCase(Locale.ROOT) -> values.get(0)
          }.toMap
          requests.add(Request(method, headers, body))
          val health = exchange.getRequestURI.getPath == "/health"
          val response = if (method == "GET") {
            jobs.get(exchange.getRequestURI.getPath)
          } else if (health) {
            val job = s"/jobs/${requests.size()}"
            jobs.put(job, responseFor(body, health = true).compactPrint)
            exchange.getResponseHeaders.add("operation-location", baseUrl + job)
            "{}"
          } else {
            responseFor(body, health = false).compactPrint
          }
          val bytes = response.getBytes(StandardCharsets.UTF_8)
          exchange.getResponseHeaders.add("Content-Type", "application/json")
          exchange.sendResponseHeaders(if (health) 202 else 200, bytes.length)
          exchange.getResponseBody.write(bytes)
        } finally {
          exchange.close()
        }
      }
    })
    server.start()
    try {
      testCode(baseUrl, requests)
    } finally {
      server.stop(0)
    }
  }

  private def sentiment(baseUrl: String): TextSentiment =
    new TextSentiment().setUrl(baseUrl + "/sentiment")
      .setTextCol("text").setLanguageCol("language")
      .setOutputCol("response").setErrorCol("error").setConcurrency(1)

  private def assertSentiment(rows: Array[Row], expected: Seq[String]): Unit = {
    assert(rows.map(_.getAs[String]("text")).toSeq == expected)
    rows.foreach { row =>
      assert(row.isNullAt(row.fieldIndex("error")))
      assert(row.getAs[Row]("response").getAs[Row]("document").getAs[String]("sentiment") ==
        row.getAs[String]("text"))
    }
  }

  private def documents(request: Request): Vector[JsObject] =
    request.body.fields("documents").convertTo[Vector[JsValue]].map(_.asJsObject)

  test("automatic batching resolves a key column and preserves documents and partial batches") {
    withServer { (url, requests) =>
      val input = Seq(
        ("first", "en", Option.empty[String]),
        ("second", "fr", Some("batch-key")),
        ("third", "de", Some("last-key"))
      ).toDF("text", "language", "key").coalesce(1)
      val rows = sentiment(url).setSubscriptionKeyCol("key").setBatchSize(2)
        .transform(input).collect()

      assertSentiment(rows, Seq("first", "second", "third"))
      val sent = requests.asScala.toSeq
      assert(sent.map(_.headers(keyHeader)) == Seq("batch-key", "last-key"))
      assert(sent.map(documents(_).size) == Seq(2, 1))
      assert(sent.flatMap(documents).map(_.fields("text")) == Seq("first", "second", "third").map(JsString(_)))
      assert(sent.flatMap(documents).map(_.fields("language")) == Seq("en", "fr", "de").map(JsString(_)))
      assert(rows.map(row => Option(row.getAs[String]("key"))).toSeq ==
        Seq(None, Some("batch-key"), Some("last-key")))
    }
  }

  test("batch size one sends each row with its own credential") {
    withServer { (url, requests) =>
      val input = Seq(("first", "en", "key-one"), ("second", "fr", "key-two"))
        .toDF("text", "language", "key").coalesce(1)
      val rows = sentiment(url).setSubscriptionKeyCol("key").setBatchSize(1).transform(input).collect()

      assertSentiment(rows, Seq("first", "second"))
      assert(requests.asScala.map(_.headers(keyHeader)).toSeq == Seq("key-one", "key-two"))
      assert(requests.asScala.forall(documents(_).size == 1))
    }
  }

  test("manual batches keep scalar credentials and array payloads") {
    withServer { (url, requests) =>
      val input = Seq((Seq("first", "second"), Seq("en", "fr"), "scalar-key"))
        .toDF("text", "language", "key").coalesce(1)
      val rows = sentiment(url).setSubscriptionKeyCol("key").transform(input).collect()

      assert(rows.length == 1)
      assert(rows.head.isNullAt(rows.head.fieldIndex("error")))
      val output = rows.head.getAs[scala.collection.Seq[Row]]("response")
      assert(output.map(_.getAs[Row]("document").getAs[String]("sentiment")) == Seq("first", "second"))
      assert(requests.size() == 1)
      assert(requests.peek().headers(keyHeader) == "scalar-key")
      assert(documents(requests.peek()).map(_.fields("language")) == Seq(JsString("en"), JsString("fr")))
    }
  }

  test("copied and reloaded stages preserve key column bindings and first-key-per-batch selection") {
    withServer { (url, requests) =>
      val original = sentiment(url).setSubscriptionKeyCol("key").setBatchSize(10)
      val directory = Files.createTempDirectory("text-header-roundtrip").toFile
      try {
        val path = new java.io.File(directory, "model").toString
        original.write.save(path)
        val stages = Seq(original.copy(ParamMap.empty), TextSentiment.load(path))
        val input = Seq(("first", "en", "key-one"), ("second", "fr", "key-two"))
          .toDF("text", "language", "key").coalesce(1)
        stages.foreach { stage =>
          assertSentiment(stage.transform(input).collect(), Seq("first", "second"))
        }
        assert(requests.size() == 2)
        assert(requests.asScala.forall(_.headers(keyHeader) == "key-one"))
        assert(requests.asScala.forall(documents(_).size == 2))
      } finally {
        FileUtils.deleteDirectory(directory)
      }
    }
  }

  test("AnalyzeHealthText uses the selected batch credential for submission and polling") {
    withServer { (url, requests) =>
      val input = Seq(("first", "en", "key-one"), ("second", "fr", "key-two"))
        .toDF("text", "language", "key").coalesce(1)
      val stage = new AnalyzeHealthText().setUrl(url + "/health")
        .setSubscriptionKeyCol("key").setTextCol("text").setLanguageCol("language")
        .setBatchSize(10).setConcurrency(1).setInitialPollingDelay(0).setPollingDelay(0)
        .setMaxPollingRetries(1).setOutputCol("response").setErrorCol("error")
      val rows = stage.transform(input).collect()

      assert(rows.length == 2)
      rows.foreach { row =>
        assert(row.isNullAt(row.fieldIndex("error")))
        val document = row.getAs[Row]("response").getAs[Row]("document")
        val entities = document.getAs[scala.collection.Seq[Row]]("entities")
        assert(entities.head.getAs[String]("text") == row.getAs[String]("text"))
      }
      val sent = requests.asScala.toSeq
      assert(sent.map(_.method) == Seq("POST", "GET"))
      assert(sent.forall(_.headers(keyHeader) == "key-one"))
      assert(documents(sent.head).map(_.fields("language")) == Seq(JsString("en"), JsString("fr")))
    }
  }
}
