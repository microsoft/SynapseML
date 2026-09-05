// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.form.contentunderstanding

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.http.ErrorUtils
import com.microsoft.azure.synapse.ml.services.contentunderstanding.{
  ContentUnderstanding, ContentUnderstandingException, ContentUnderstandingResponse}
import org.apache.commons.io.FileUtils
import org.apache.http.HttpStatus
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.io.File
import java.util.UUID
import scala.collection.JavaConverters._

class ContentUnderstandingSuite extends TestBase {
  import ContentUnderstandingFixtures._
  import ContentUnderstandingStub.withReplies

  private val locationPath = ResultsPath + "op-1?api-version=" + DefaultApiVersion
  private val running = """{"id":"op-1","status":"Running","result":{"contents":[]}}"""
  private val succeeded =
    """{
      |  "id":"op-1",
      |  "status":"Succeeded",
      |  "result":{"contents":[{"metadata":{"preview":true},"fields":{"A":{"type":"string","confidence":0.8}}}]},
      |  "usage":{"documentPagesBasic":2,"gpt-5.2-input":51},
      |  "warnings":[{"code":"ExampleWarning"}],
      |  "futureProperty":{"nested":[1,true,null]}
      |}""".stripMargin
  private val failed =
    """{"id":"op-1","status":"Failed","result":{"contents":[]},"error":{"code":"ResourceError","innererror":""" +
      """{"code":"DeploymentNotFound","message":"Missing model deployment."}}}"""
  private val accepted = ContentUnderstandingStubReply(HttpStatus.SC_ACCEPTED, running,
    Map("Operation-Location" -> ("$ROOT" + locationPath), "Retry-After" -> "0"))

  private def withJournal(test: String => Unit): Unit = {
    val destination = new File("cu-journal-test-" + UUID.randomUUID().toString)
    try {
      test(destination.getAbsolutePath)
    } finally {
      FileUtils.deleteDirectory(destination)
    }
  }

  private def stage(server: ContentUnderstandingStub): ContentUnderstanding =
    new ContentUnderstanding().setEndpoint(server.endpoint).setOutputCol("result").setErrorCol("error")
      .setPollingDelay(0).setMaxPollAttempts(3)

  private def dataFrame(rows: Seq[Row], schema: StructType): DataFrame =
    spark.createDataFrame(rows.asJava, schema).coalesce(1)

  private def input: DataFrame =
    dataFrame(Seq(Row("doc")), new StructType().add("id", StringType))

  private def response(row: Row): ContentUnderstandingResponse =
    ContentUnderstandingResponse.makeFromRowConverter(row.getAs[Row]("result"))

  private def resultOf(transformer: ContentUnderstanding, frame: DataFrame): ContentUnderstandingResponse = {
    val row = transformer.transform(frame).collect().head
    ContentUnderstandingResponse.makeFromRowConverter(row.getAs[Row](transformer.getOutputCol))
  }

  private def submit(transformer: ContentUnderstanding): ContentUnderstandingResponse =
    resultOf(transformer.copy(ParamMap.empty).setOperationMode("submit"), input)

  private def poll(transformer: ContentUnderstanding, location: String): ContentUnderstandingResponse =
    resultOf(transformer.copy(ParamMap.empty).setOperationMode("poll").setOperationLocation(location), input)

  private def exceptionContains(error: Throwable, text: String): Boolean =
    Option(error).exists(value => Option(value.getMessage).exists(_.contains(text)) ||
      exceptionContains(value.getCause, text))

  private class PythonSourceStage extends ContentUnderstanding("python-source") {
    def pythonSource: String = pythonClass()
  }

  private class HeaderGuard extends ContentUnderstanding("header-guard") {
    override protected def getCustomAuthHeader(row: Row): Option[String] =
      throw new IllegalStateException("Headers must not be prepared for an unsafe operation URL.")
  }

  test("public transform is lazy and only Succeeded completes a response with empty running contents") {
    withReplies(Seq(accepted, ContentUnderstandingStubReply(HttpStatus.SC_OK, running),
      ContentUnderstandingStubReply(HttpStatus.SC_OK, succeeded))) { server =>
      val transformer = stage(server).setDocumentBytes(Array[Byte](1, 2, 3))
      val transformed = transformer.transform(input)
      assert(server.requests.isEmpty)
      val row = transformed.collect().head
      val result = response(row)
      assert(server.requests.map(_.method) == Seq("POST", "GET", "GET"))
      assert(result.status == "Succeeded")
      assert(result.rawResponse == succeeded)
      assert(result.operationLocation.contains(server.endpoint + locationPath))
      assert(row.getAs[Row]("error") == None.orNull)
      assert(result.error.isEmpty)
      assert(result.rawResponse.parseJson.asJsObject.fields("usage").asJsObject.fields("gpt-5.2-input") == JsNumber(51))
      assert(transformer.transformSchema(input.schema) == transformed.schema)
    }
  }

  test("column inputs preserve binary data, options, preview query parameters, and explicit key authentication") {
    withReplies(Seq(accepted)) { server =>
      val bytes = Array[Byte](0, 1, -1, -128)
      val schema = new StructType().add("source.bytes", BinaryType).add("analyzer", StringType)
        .add("name", StringType).add("mime", StringType).add("pages", StringType)
        .add("models", MapType(StringType, StringType)).add("key", StringType)
        .add("version", StringType).add("encoding", StringType).add("processing", StringType)
      val models = Map("prebuilt-analyzer-embedding" -> "text-embedding-3-large",
        "prebuilt-analyzer-completion" -> "gpt-5.2")
      val frame = dataFrame(Seq(Row(bytes, "prebuilt-invoice", "invoice.pdf", "application/pdf", "319-320",
        models, "test-key", "2026-06-01-preview", "utf16", "geography")), schema)
      val transformer = stage(server).setOperationMode("submit")
        .setDocumentBytesCol("source.bytes").setAnalyzerIdCol("analyzer").setDocumentNameCol("name")
        .setMimeTypeCol("mime").setRangeCol("pages").setModelDeploymentsCol("models")
        .setSubscriptionKeyCol("key").setApiVersionCol("version").setStringEncodingCol("encoding")
        .setProcessingLocationCol("processing")
      assert(response(transformer.transform(frame).collect().head).status == "Running")
      val sent = server.requests.head
      assert(sent.path.endsWith("/prebuilt-invoice:analyze"))
      assert(sent.query == "api-version=2026-06-01-preview&processingLocation=geography&stringEncoding=utf16")
      assert(sent.headers("ocp-apim-subscription-key") == "test-key")
      assert(!sent.headers.contains("authorization"))
      val json = sent.body.parseJson.asJsObject
      val item = json.fields("inputs").asInstanceOf[JsArray].elements.head.asJsObject
      assert(item.fields("data") == JsString(java.util.Base64.getEncoder.encodeToString(bytes)))
      assert(!item.fields.contains("dataBase64"))
      assert(!item.fields.contains("url"))
      assert(item.fields("name") == JsString("invoice.pdf"))
      assert(item.fields("mimeType") == JsString("application/pdf"))
      assert(item.fields("range") == JsString("319-320"))
      assert(json.fields("modelDeployments").convertTo[Map[String, String]] == models)
    }
  }

  test("URL requests use singleton inputs and AAD bearer authentication") {
    withReplies(Seq(accepted)) { server =>
      val transformer = stage(server).setDocumentUrl("https://example.invalid/document.pdf?signature=test")
        .setAADToken("test-token").setDocumentName("document.pdf")
      val submitted = submit(transformer)
      assert(submitted.status == "Running")
      assert(server.requests.size == 1)
      val sent = server.requests.head
      assert(sent.headers("authorization") == "Bearer test-token")
      assert(!sent.headers.contains("ocp-apim-subscription-key"))
      val items = sent.body.parseJson.asJsObject.fields("inputs").asInstanceOf[JsArray].elements
      assert(items.size == 1)
      assert(items.head.asJsObject.fields("url") == JsString(transformer.getDocumentUrl))
      assert(!items.head.asJsObject.fields.contains("data"))
    }
  }

  test("public output names remain literal and cannot collide with internal HTTP columns") {
    withReplies(Seq(accepted)) { server =>
      val transformer = stage(server).setOperationMode("submit").setDocumentBytes(Array[Byte](1))
        .setOutputCol("contentUnderstandingInput").setErrorCol("service.error")
      val output = transformer.transform(input)
      val row = output.collect().head
      val result = ContentUnderstandingResponse.makeFromRowConverter(row.getAs[Row]("contentUnderstandingInput"))
      assert(result.status == "Running")
      assert(output.columns.toSet == Set("id", "contentUnderstandingInput", "service.error"))
      assert(output.schema == transformer.transformSchema(input.schema))
    }
  }

  test("HTTP 200 Failed operations retain the nested service error and fill errorCol") {
    withReplies(Seq(accepted, ContentUnderstandingStubReply(HttpStatus.SC_OK, failed))) { server =>
      val row = stage(server).setDocumentBytes(Array[Byte](1)).transform(input).collect().head
      val result = response(row)
      assert(result.status == "Failed")
      assert(result.httpStatus == HttpStatus.SC_OK)
      assert(result.rawResponse == failed)
      assert(result.error.contains(failed.parseJson.asJsObject.fields("error").compactPrint))
      val error = row.getAs[Row]("error")
      assert(error.getAs[String]("response") == result.error.get)
      assert(error.getAs[Row]("status").getAs[Int]("statusCode") == HttpStatus.SC_OK)
    }
  }

  test("initial HTTP failures remain useful output and are never automatically resubmitted") {
    val body = """{"error":{"code":"InvalidPagesOutOfRange","message":"Out of range."}}"""
    val statuses = Seq(HttpStatus.SC_BAD_REQUEST -> "Failed",
      HttpStatus.SC_INTERNAL_SERVER_ERROR -> "Unknown", TooManyRequests -> "Rejected")
    statuses.foreach { case (code, status) =>
      withReplies(Seq(ContentUnderstandingStubReply(code, body, Map("Retry-After" -> "0")))) { server =>
        val row = stage(server).setDocumentBytes(Array[Byte](1)).transform(input).collect().head
        val result = response(row)
        assert(result.status == status)
        assert(result.httpStatus == code)
        assert(result.rawResponse == body)
        assert(result.error.exists(_.contains("InvalidPagesOutOfRange")))
        assert(Option(row.getAs[Row]("error")).isDefined)
        assert(server.requests.size == 1)
      }
    }
  }

  test("malformed JSON and missing operation status leave submission outcomes unknown") {
    Seq("not-json", """{"result":{"contents":[]}}""", """{"status":"unexpected"}""").foreach { body =>
      withReplies(Seq(ContentUnderstandingStubReply(HttpStatus.SC_OK, body))) { server =>
        val result = submit(stage(server).setDocumentBytes(Array[Byte](1)))
        assert(result.status == "Unknown")
        assert(result.rawResponse == body)
        assert(result.error.exists(_.contains("InvalidResponse")))
      }
    }
  }

  test("missing or unsafe service operation locations do not trigger polling") {
    val unsafe = Seq(
      Map.empty[String, String],
      Map("Operation-Location" -> ("https://example.invalid" + locationPath)),
      Map("Operation-Location" -> ("$ROOT" + locationPath + "&redirect=other")),
      Map("Operation-Location" -> ("$ROOT" + ResultsPath + "../analyzers?api-version=" + DefaultApiVersion)))
    unsafe.foreach { headers =>
      withReplies(Seq(ContentUnderstandingStubReply(HttpStatus.SC_ACCEPTED, running, headers))) { server =>
        val result = response(stage(server).setDocumentBytes(Array[Byte](1)).transform(input).collect().head)
        assert(result.status == "Unknown")
        assert(result.operationLocation.isEmpty)
        assert(result.error.exists(_.contains("OperationLocation")))
        assert(result.rawResponse == running)
        assert(server.requests.size == 1)
      }
    }
  }

  test("poll URLs are validated before credential resolution") {
    val transformer = new HeaderGuard().setEndpoint("https://example.invalid").setSubscriptionKey("test-key")
      .setOperationMode("poll")
    val bad = Seq(
      "http://example.invalid" + locationPath,
      "https://other.invalid" + locationPath,
      "https://example.invalid:444" + locationPath,
      "https://user:password@example.invalid" + locationPath,
      "https://example.invalid" + locationPath + "#fragment",
      locationPath,
      "https://example.invalid" + ResultsPath + "%2e%2e?api-version=" + DefaultApiVersion,
      "https://example.invalid" + ResultsPath + "op%2F1?api-version=" + DefaultApiVersion,
      "https://example.invalid" + ResultsPath + "op-1/child?api-version=" + DefaultApiVersion,
      "https://example.invalid" + ResultsPath + "op-1?other=value",
      "https://example.invalid" + locationPath + "&api-version=" + DefaultApiVersion)
    bad.foreach { location =>
      intercept[IllegalArgumentException] {
        transformer.setOperationLocation(location).transformSchema(StructType(Nil))
      }
    }
    val valid = new ContentUnderstanding().setEndpoint("https://example.invalid").setOperationMode("poll")
      .setOperationLocation("https://example.invalid:443" + locationPath)
    assert(valid.transformSchema(StructType(Nil)).fieldNames.contains(valid.getOutputCol))
  }

  test("HTTP redirects from submission and polling are never followed") {
    val redirect = ContentUnderstandingStubReply(HttpStatus.SC_MOVED_TEMPORARILY, """{"redirect":true}""",
      Map("Location" -> ("$ROOT" + locationPath)))
    Seq(Seq(redirect), Seq(accepted, redirect)).foreach { replies =>
      withReplies(replies) { server =>
        val result = response(stage(server).setDocumentBytes(Array[Byte](1)).transform(input).collect().head)
        assert(result.status == "Failed")
        assert(result.httpStatus == HttpStatus.SC_MOVED_TEMPORARILY)
        assert(result.error.isDefined)
        assert(server.requests.size == replies.size)
      }
    }
  }

  test("poll budgets retain the last Running response and allow resumption without a new POST") {
    withReplies(Seq(accepted, ContentUnderstandingStubReply(HttpStatus.SC_OK, running),
      ContentUnderstandingStubReply(HttpStatus.SC_OK, running),
      ContentUnderstandingStubReply(HttpStatus.SC_OK, succeeded))) { server =>
      val transformer = stage(server).setDocumentBytes(Array[Byte](1)).setMaxPollAttempts(2)
      val pending = response(transformer.transform(input).collect().head)
      assert(pending.status == "Running")
      assert(pending.rawResponse == running)
      assert(pending.error.isEmpty)
      assert(pending.operationLocation.contains(server.endpoint + locationPath))
      val resumed = poll(transformer, pending.operationLocation.get)
      assert(resumed.status == "Succeeded")
      assert(server.requests.count(_.method == "POST") == 1)
      assert(server.requests.count(_.method == "GET") == 3)
    }
  }

  test("GET transient retries including 429 consume the poll budget") {
    val throttled = ContentUnderstandingStubReply(TooManyRequests,
      """{"error":{"code":"TooManyRequests"}}""", Map("Retry-After" -> "0"))
    withReplies(Seq(accepted, throttled)) { server =>
      val result = response(stage(server).setDocumentBytes(Array[Byte](1)).setMaxPollAttempts(2)
        .transform(input).collect().head)
      assert(server.requests.size == 3)
      assert(result.status == "Running")
      assert(result.rawResponse == running)
      assert(result.httpStatus == TooManyRequests)
      assert(result.error.exists(_.contains("TooManyRequests")))
      assert(result.operationLocation.isDefined)
    }
    withReplies(Seq(throttled, ContentUnderstandingStubReply(HttpStatus.SC_SERVICE_UNAVAILABLE,
      """{"error":{"code":"Unavailable"}}"""), ContentUnderstandingStubReply(HttpStatus.SC_OK, succeeded))) { server =>
      val result = poll(stage(server), server.endpoint + locationPath)
      assert(result.status == "Succeeded")
      assert(server.requests.size == 3)
      assert(server.requests.forall(_.method == "GET"))
    }
  }

  test("Retry-After seconds, past HTTP dates, and malformed values allow bounded public polling") {
    Seq("0", "Thu, 01 Jan 1970 00:00:00 GMT", "not a date", "-1").foreach { retryAfter =>
      val submission = accepted.copy(headers = accepted.headers + ("Retry-After" -> retryAfter))
      withReplies(Seq(submission, ContentUnderstandingStubReply(HttpStatus.SC_OK, succeeded))) { server =>
        assert(resultOf(stage(server).setDocumentBytes(Array[Byte](1)), input).status == "Succeeded")
        assert(server.requests.map(_.method) == Seq("POST", "GET"))
      }
    }
  }

  test("response bounds apply to declared and chunked bodies before JSON parsing") {
    val limit = 64
    Seq(false, true).foreach { chunked =>
      withReplies(Seq(ContentUnderstandingStubReply(HttpStatus.SC_OK, "x" * (limit + 1),
        chunked = chunked))) { server =>
        val result = submit(stage(server).setDocumentBytes(Array[Byte](1)).setMaxResponseBytes(limit))
        assert(result.status == "Unknown")
        assert(result.httpStatus == HttpStatus.SC_OK)
        assert(result.error.exists(_.contains("ResponseTooLarge")))
        assert(result.rawResponse.isEmpty)
        assert(server.requests.size == 1)
      }
    }
  }

  test("ambiguous POST transport failures are marked Unknown and are not retried") {
    withReplies(Seq(ContentUnderstandingStubReply(0, "", disconnect = true))) { server =>
      val result = submit(stage(server).setDocumentBytes(Array[Byte](1)))
      assert(result.status == "Unknown")
      assert(result.httpStatus == 0)
      assert(result.error.exists(_.contains("TransportError")))
      assert(server.requests.size == 1)
    }
  }

  test("exhausted polling transport retries retain the last operation and expose the I/O error") {
    withReplies(Seq(ContentUnderstandingStubReply(HttpStatus.SC_OK, running),
      ContentUnderstandingStubReply(0, "", disconnect = true))) { server =>
      val result = poll(stage(server).setMaxPollAttempts(2), server.endpoint + locationPath)
      assert(result.status == "Running")
      assert(result.rawResponse == running)
      assert(result.operationLocation.contains(server.endpoint + locationPath))
      assert(result.error.exists(_.contains("TransportError")))
      assert(server.requests.size == 2)
    }
  }

  test("writer polling leaves oversized results resumable after the response cap is raised") {
    withReplies(Seq(accepted, ContentUnderstandingStubReply(HttpStatus.SC_OK, succeeded))) { server =>
      val transformer = stage(server).setDocumentBytes(Array[Byte](1)).setMaxResponseBytes(128)
        .setMaxPollAttempts(1)
      val destination = new File("cu-cap-resume-test-" + UUID.randomUUID().toString)
      try {
        val failure = intercept[ContentUnderstandingException] {
          transformer.writeToPath(input, "id", destination.getAbsolutePath, "parquet")
        }
        assert(failure.response.error.exists(_.contains("ResponseTooLarge")))
        assert(failure.response.operationLocation.contains(server.endpoint + locationPath))
        val pending = transformer.readPath(spark, destination.getAbsolutePath, "parquet").collect().head
        assert(pending.getAs[String]("status") == "Running")
        assert(pending.getAs[String]("operationLocation") == server.endpoint + locationPath)
        val resumed = transformer.setMaxResponseBytes(4096)
          .writeToPath(input, "id", destination.getAbsolutePath, "parquet").collect().head
        assert(resumed.getAs[String]("status") == "Succeeded")
        assert(server.requests.count(_.method == "POST") == 1)
        assert(server.requests.count(_.method == "GET") == 2)
        assert(server.requests.filter(_.method == "GET").map(_.path).distinct.size == 1)
      } finally {
        FileUtils.deleteDirectory(destination)
      }
    }
  }

  test("an accepted handle survives an oversized or malformed submission response") {
    val complete = """{"id":"op-1","status":"Succeeded","result":{"contents":[]}}"""
    Seq("x" * 129, "not-json").foreach { body =>
      withReplies(Seq(accepted.copy(body = body),
        ContentUnderstandingStubReply(HttpStatus.SC_OK, complete))) { server =>
        withJournal { path =>
          val transformer = stage(server).setDocumentBytes(Array[Byte](1)).setMaxResponseBytes(128)
          val result = transformer.writeToPath(input, "id", path, "parquet").head()
          assert(result.getAs[String]("status") == "Succeeded")
          assert(result.getAs[String]("operationLocation") == server.endpoint + locationPath)
          assert(server.requests.map(_.method) == Seq("POST", "GET"))
          val history = spark.read.parquet(path).orderBy("sequence").select("status").collect()
          assert(history.map(_.getString(0)).toSeq == Seq("Unknown", "Succeeded"))
        }
      }
    }
  }

  test("an unknown submission is journaled and is not resubmitted without an operation handle") {
    val replies = Seq(
      ContentUnderstandingStubReply(0, "", disconnect = true) -> "TransportError",
      ContentUnderstandingStubReply(HttpStatus.SC_OK, "not-json") -> "InvalidResponse",
      accepted.copy(headers = Map.empty) -> "MissingOperationLocation",
      ContentUnderstandingStubReply(HttpStatus.SC_INTERNAL_SERVER_ERROR, "{}",
        Map("Operation-Location" -> "https://example.invalid/unsafe")) -> "InvalidOperationLocation")
    replies.foreach { case (reply, errorCode) =>
      withReplies(Seq(reply)) { server =>
        withJournal { path =>
          val transformer = stage(server).setDocumentBytes(Array[Byte](1))
          val first = intercept[ContentUnderstandingException] {
            transformer.writeToPath(input, "id", path, "parquet")
          }
          assert(first.response.status == "Unknown")
          val recorded = transformer.readPath(spark, path, "parquet").head()
          assert(recorded.getAs[String]("status") == "Unknown")
          assert(recorded.getAs[String]("error").contains(errorCode))
          val retry = intercept[IllegalArgumentException] {
            transformer.writeToPath(input, "id", path, "parquet")
          }
          assert(retry.getMessage.contains("unknown"))
          assert(server.requests.map(_.method) == Seq("POST"))
        }
      }
    }
  }

  test("exhausted transient polling errors leave the committed handle available for retry") {
    val throttled = ContentUnderstandingStubReply(TooManyRequests,
      """{"error":{"code":"TooManyRequests"}}""", Map("Retry-After" -> "0"))
    withReplies(Seq(accepted, throttled)) { server =>
      withJournal { path =>
        val transformer = stage(server).setDocumentBytes(Array[Byte](1)).setMaxPollAttempts(1)
        val failure = intercept[ContentUnderstandingException] {
          transformer.writeToPath(input, "id", path, "parquet")
        }
        assert(failure.response.httpStatus == TooManyRequests)
        val pending = transformer.readPath(spark, path, "parquet").head()
        assert(pending.getAs[String]("status") == "Running")
        assert(pending.getAs[String]("operationLocation") == server.endpoint + locationPath)
        assert(server.requests.map(_.method) == Seq("POST", "GET"))
      }
    }
  }

  test("polling configuration and credential errors do not terminalize an accepted operation") {
    Seq(HttpStatus.SC_BAD_REQUEST, HttpStatus.SC_UNAUTHORIZED, HttpStatus.SC_FORBIDDEN).foreach { code =>
      val rejected = ContentUnderstandingStubReply(code, """{"error":{"code":"PollingRejected"}}""")
      withReplies(Seq(accepted, rejected, ContentUnderstandingStubReply(HttpStatus.SC_OK, succeeded))) { server =>
        withJournal { path =>
          val transformer = stage(server).setDocumentBytes(Array[Byte](1)).setAADToken("expired-test-token")
          val failure = intercept[ContentUnderstandingException] {
            transformer.writeToPath(input, "id", path, "parquet")
          }
          assert(failure.response.httpStatus == code)
          assert(transformer.readPath(spark, path, "parquet").head().getAs[String]("status") == "Running")
          val resumed = transformer.setAADToken("refreshed-test-token").writeToPath(input, "id", path, "parquet")
          assert(resumed.head().getAs[String]("status") == "Succeeded")
          assert(server.requests.count(_.method == "POST") == 1)
          assert(server.requests.last.headers("authorization") == "Bearer refreshed-test-token")
        }
      }
    }
  }

  test("interruption stops requests without clearing the thread interruption") {
    withReplies(Seq(accepted)) { server =>
      val transformer = stage(server).setAnalyzerId("custom")
      Thread.currentThread().interrupt()
      try {
        intercept[InterruptedException] {
          transformer.createAnalyzer("{}", allowReplace = false)
        }
        assert(Thread.currentThread().isInterrupted)
      } finally {
        Thread.interrupted()
      }
      assert(server.requests.isEmpty)
    }
  }

  test("poll mode accepts operationLocation columns without document input") {
    withReplies(Seq(ContentUnderstandingStubReply(HttpStatus.SC_OK, succeeded))) { server =>
      val frame = dataFrame(Seq(Row(server.endpoint + locationPath, "test-token")),
        new StructType().add("handle", StringType).add("token", StringType))
      val transformer = stage(server).setOperationMode("poll").setOperationLocationCol("handle")
        .setAADTokenCol("token")
      val result = response(transformer.transform(frame).collect().head)
      assert(result.status == "Succeeded")
      assert(server.requests.map(_.method) == Seq("GET"))
      assert(server.requests.head.headers("authorization") == "Bearer test-token")
      assert(server.requests.head.body.isEmpty)
    }
  }

  test("public submit and poll modes reuse configured document columns without resubmission") {
    withReplies(Seq(accepted, ContentUnderstandingStubReply(HttpStatus.SC_OK, succeeded))) { server =>
      val schema = new StructType().add("id", StringType).add("bytes", BinaryType).add("pages", StringType)
      val frame = dataFrame(Seq(Row("document-1", Array[Byte](1), "1-2")), schema)
      val transformer = stage(server).setOperationMode("submit").setDocumentBytesCol("bytes").setRangeCol("pages")
      val submitted = resultOf(transformer, frame)
      assert(submitted.status == "Running")
      val resumed = transformer.setOperationMode("poll").setOperationLocation(submitted.operationLocation.get)
      assert(resultOf(resumed, input).status == "Succeeded")
      assert(server.requests.size == 2)
      assert(server.requests.head.body.parseJson.asJsObject.fields("inputs")
        .asInstanceOf[JsArray].elements.head.asJsObject.fields("range") == JsString("1-2"))
    }
  }

  test("canonical analyze URL and body do not depend on authentication, execution controls, or map ordering") {
    withReplies(Seq(accepted)) { server =>
      val first = stage(server).setDocumentBytes(Array[Byte](1)).setSubscriptionKey("first-key")
        .setModelDeployments(Map("z" -> "last", "a" -> "first"))
      val second = first.copy(ParamMap.empty).setSubscriptionKey("other-key").setAADToken("other-token")
        .setCustomHeaders(Map("x-test-header" -> "value")).setConcurrency(3)
        .setTimeout(3).setPollingDelay(0).setMaxPollAttempts(1).setMaxResponseBytes(1024)
        .setOperationMode("submit").setOutputCol("other-output").setErrorCol("other-error")
        .setModelDeployments(scala.collection.immutable.ListMap("a" -> "first", "z" -> "last"))
      assert(submit(first).status == "Running")
      assert(submit(second).status == "Running")
      val original = server.requests.head
      val equivalent = server.requests(1)
      assert(original.path == equivalent.path)
      assert(original.query == equivalent.query)
      assert(original.body == equivalent.body)
      assert(original.headers("ocp-apim-subscription-key") != equivalent.headers("ocp-apim-subscription-key"))
      submit(second.setRange("1-2"))
      assert(server.requests.last.body != original.body)
    }
  }

  test("schema validation rejects missing and incorrectly typed inputs and invalid scalar configuration") {
    val schema = new StructType().add("text", StringType).add("number", IntegerType)
      .add("bytes", BinaryType).add("models", MapType(StringType, IntegerType))
    val base = new ContentUnderstanding().setEndpoint("https://example.invalid").setOutputCol("result")
      .setErrorCol("error")
    val invalid = Seq(
      base.copy(ParamMap.empty),
      base.copy(ParamMap.empty).setDocumentBytes(Array[Byte](1)).setDocumentUrl("https://example.invalid/doc"),
      base.copy(ParamMap.empty).setDocumentBytesCol("text"),
      base.copy(ParamMap.empty).setDocumentUrlCol("number"),
      base.copy(ParamMap.empty).setDocumentBytesCol("missing"),
      base.copy(ParamMap.empty).setDocumentBytesCol("bytes").setModelDeploymentsCol("models"),
      base.copy(ParamMap.empty).setDocumentBytes(Array.emptyByteArray),
      base.copy(ParamMap.empty).setDocumentUrl(""),
      base.copy(ParamMap.empty).setDocumentUrl("file:///document.pdf"),
      base.copy(ParamMap.empty).setDocumentBytes(Array[Byte](1)).setAnalyzerId("../other"),
      base.copy(ParamMap.empty).setDocumentBytes(Array[Byte](1)).setApiVersion("not-a-version"),
      base.copy(ParamMap.empty).setDocumentBytes(Array[Byte](1)).setStringEncoding("bad"),
      base.copy(ParamMap.empty).setDocumentBytes(Array[Byte](1)).setProcessingLocation("bad"),
      base.copy(ParamMap.empty).setDocumentBytes(Array[Byte](1)).setConcurrency(0),
      base.copy(ParamMap.empty).setDocumentBytes(Array[Byte](1)).setTimeout(Double.PositiveInfinity),
      base.copy(ParamMap.empty).setOperationMode("poll"))
    invalid.foreach(transformer => intercept[IllegalArgumentException](transformer.transformSchema(schema)))
    intercept[IllegalArgumentException](base.setPollingDelay(-1))
    intercept[IllegalArgumentException](base.setMaxPollAttempts(0))
    intercept[IllegalArgumentException](base.setMaxResponseBytes(0))
    intercept[IllegalArgumentException](base.setOperationMode("invalid"))
  }

  test("null configured row inputs fail explicitly rather than being silently skipped") {
    val schema = new StructType().add("document", BinaryType)
    val frame = dataFrame(Seq(Row(None.orNull)), schema)
    val transformer = new ContentUnderstanding().setEndpoint("https://example.invalid").setDocumentBytesCol("document")
    transformer.transformSchema(schema)
    val error = intercept[Exception](transformer.transform(frame).collect())
    assert(exceptionContains(error, "selected document input cannot be null"))
    val scalar = transformer.copy(ParamMap.empty).setDocumentBytes(None.orNull)
    intercept[IllegalArgumentException](scalar.transformSchema(schema))
  }

  test("endpoint joining requires explicit configuration and rejects insecure or ambiguous authorities") {
    val unset = new ContentUnderstanding().setDocumentBytes(Array[Byte](1))
      .setDefaultInternalEndpoint("https://fabric.invalid")
    intercept[IllegalArgumentException](unset.transformSchema(StructType(Nil)))
    val base = new ContentUnderstanding().setEndpoint("https://example.invalid")
    assert(base.getUrl == "https://example.invalid" + AnalyzersPath)
    assert(base.setEndpoint("https://example.invalid/").getUrl == "https://example.invalid" + AnalyzersPath)
    assert(base.setEndpoint("https://example.invalid" + AnalyzersPath + "/").getUrl ==
      "https://example.invalid" + AnalyzersPath)
    Seq("http://example.invalid", "http://localhost", "https://user:password@example.invalid",
      "https://example.invalid?api-version=other", "https://example.invalid/prefix",
      "https://example.invalid#fragment").foreach(value =>
      intercept[IllegalArgumentException](base.setEndpoint(value)))
    assert(base.setEndpoint("http://127.0.0.1:1234").getUrl.startsWith("http://127.0.0.1:1234/"))
  }

  test("copy and persistence retain service parameters, binary scalars, and response schemas") {
    val original = new ContentUnderstanding("saved-cu").setEndpoint("https://example.invalid")
      .setDocumentBytes(Array[Byte](0, -1, -128, 127)).setAnalyzerId("custom.invoice")
      .setApiVersion("2026-06-01-preview").setModelDeployments(Map("prebuilt-analyzer-completion" -> "gpt-5.2"))
      .setOperationMode("submit").setPollingDelay(0).setOutputCol("result").setErrorCol("error")
    val copied = original.copy(ParamMap(original.outputCol -> "copied"))
    assert(copied.uid == original.uid)
    assert(copied.getOutputCol == "copied")
    assert(copied.getDocumentBytes.sameElements(original.getDocumentBytes))
    val destination = new File("cu-stage-test-" + UUID.randomUUID().toString)
    try {
      original.write.save(destination.getAbsolutePath)
      val restored = ContentUnderstanding.load(destination.getAbsolutePath)
      assert(restored.uid == original.uid)
      assert(restored.getUrl == original.getUrl)
      assert(restored.isSet(restored.url))
      assert(restored.getDocumentBytes.sameElements(original.getDocumentBytes))
      assert(restored.getModelDeployments == original.getModelDeployments)
      assert(restored.getApiVersion == original.getApiVersion)
      assert(restored.getAnalyzerId == original.getAnalyzerId)
      assert(restored.getOperationMode == original.getOperationMode)
      val schema = restored.transformSchema(new StructType().add("id", StringType))
      assert(schema("result").dataType ==
        StructType(ContentUnderstandingResponse.schema.fields.map(_.copy(nullable = true))))
      assert(schema("error").dataType == ErrorUtils.ErrorSchema)
    } finally {
      FileUtils.deleteDirectory(destination)
    }
  }

  test("generated Python source supports unsigned binary scalars and composes provisioning and writer methods") {
    val transformer = new PythonSourceStage
    val source = transformer.pythonSource
    assert(source.contains("self._java_obj.setDocumentBytes(bytearray(value))"))
    assert(source.contains("def setDocumentBytesCol(self, value)"))
    assert(source.contains("def setParams(self, **kwargs)"))
    assert(source.contains("def _transfer_params_from_java(self)"))
    assert(source.contains("self._paramMap.pop(param, None)"))
    assert(source.contains("json.dumps(definition) if isinstance(definition, dict)"))
    assert(source.contains("def writeToTable("))
    assert(source.contains("def writeToPath("))
    assert(source.contains("def getAnalyzer("))
    assert(transformer.documentBytes.pyValue(Left(Array[Byte](-1, -128))) == "bytearray([255, 128])")
    assert(transformer.documentBytes.jsonDecode(transformer.documentBytes.jsonEncode(Left(Array[Byte](-1))))
      .left.get.sameElements(Array[Byte](-1)))
  }

  test("explicit provisioning awaits only management operations and returns the raw analyzer definition") {
    val analyzer = """{"analyzerId":"custom","status":"ready","config":{"returnDetails":true},"future":{"value":1}}"""
    val operation = AnalyzersPath + "/custom/operations/create-1?api-version=" + DefaultApiVersion
    val creating = ContentUnderstandingStubReply(HttpStatus.SC_CREATED,
      """{"analyzerId":"custom","status":"creating"}""",
      Map("Operation-Location" -> ("$ROOT" + operation), "Retry-After" -> "0"))
    withReplies(Seq(creating, ContentUnderstandingStubReply(HttpStatus.SC_OK, running),
      ContentUnderstandingStubReply(HttpStatus.SC_OK, """{"id":"create-1","status":"Succeeded","result":{}}"""),
      ContentUnderstandingStubReply(HttpStatus.SC_OK, analyzer))) { server =>
      val transformer = stage(server).setAnalyzerId("custom").setSubscriptionKey("test-key")
      val definition = """{"baseAnalyzerId":"prebuilt-document","config":{"returnDetails":true}}"""
      assert(transformer.createAnalyzer(definition, allowReplace = false) == analyzer)
      assert(server.requests.map(_.method) == Seq("PUT", "GET", "GET", "GET"))
      assert(server.requests.head.query == "allowReplace=false&api-version=" + DefaultApiVersion)
      assert(server.requests.head.body == definition)
      assert(server.requests.slice(1, 3).forall(_.path == operation.takeWhile(_ != '?')))
      assert(server.requests.last.path == AnalyzersPath + "/custom")
      assert(server.requests.forall(_.headers("ocp-apim-subscription-key") == "test-key"))
      assert(!server.requests.exists(_.path.contains("/defaults")))
    }
  }

  test("provisioning preserves DefaultsNotSet and never changes shared resource defaults") {
    val body = """{"error":{"code":"InvalidRequest","innererror":""" +
      """{"code":"DefaultsNotSet","message":"Set defaults."}}}"""
    withReplies(Seq(ContentUnderstandingStubReply(HttpStatus.SC_BAD_REQUEST, body))) { server =>
      val error = intercept[ContentUnderstandingException] {
        stage(server).setAnalyzerId("custom").createAnalyzer("""{"baseAnalyzerId":"prebuilt-document"}""",
          allowReplace = false)
      }
      assert(error.response.rawResponse == body)
      assert(error.response.error.exists(_.contains("DefaultsNotSet")))
      assert(error.getMessage.contains("DefaultsNotSet"))
      assert(server.requests.map(_.method) == Seq("PUT"))
    }
  }

  test("provisioning rejects analysis-result locations and column-based management configuration") {
    withReplies(Seq(ContentUnderstandingStubReply(HttpStatus.SC_CREATED,
      """{"analyzerId":"custom","status":"creating"}""",
      Map("Operation-Location" -> ("$ROOT" + locationPath))))) { server =>
      val transformer = stage(server).setAnalyzerId("custom")
      val error = intercept[ContentUnderstandingException] {
        transformer.createAnalyzer("{}", allowReplace = true)
      }
      assert(error.response.error.exists(_.contains("InvalidOperationLocation")))
      assert(server.requests.size == 1)
      intercept[IllegalArgumentException](transformer.setAnalyzerIdCol("analyzer").getAnalyzer())
    }
  }
}
