// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.contentunderstanding

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.http.HTTPRequestData
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.conn.DnsResolver
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{BinaryType, StringType, StructType}

import java.net.{InetAddress, ServerSocket}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import spray.json.{JsArray, JsObject, JsString, JsValue}

class ContentUnderstandingConcurrencySuite extends TestBase {
  private val invalidResponseBodies = Seq(
    """{"status":"Running","n":1e9999999999}""",
    """{"status":"Running","nested":""" + ("[" * 1001) + "0" + ("]" * 1001) + "}",
    """{"status":"Running","n":""" + ("9" * 101) + "}")

  test("bounded JSON submission decoding preserves malformed and sibling handles") {
    for (body <- invalidResponseBodies; concurrency <- Seq(1, 2)) {
      val malformed = ContentUnderstandingFixtures.Accepted.copy(body = body)
      ContentUnderstandingStub.withReplies(Seq(ContentUnderstandingFixtures.Accepted, malformed,
        ContentUnderstandingFixtures.Accepted)) { server =>
        val stage = new ContentUnderstanding().setEndpoint(server.endpoint).setOutputCol("result")
          .setDocumentBytes(Array[Byte](1)).setOperationMode("submit").setConcurrency(concurrency)
        val rows = stage.transform(spark.range(3).coalesce(1)).collect().map(_.getAs[Row]("result"))
        assert(rows.map(_.getAs[String]("status")).sorted.toSeq == Seq("Running", "Running", "Unknown"))
        assert(rows.forall(_.getAs[String]("operationLocation").contains(ContentUnderstandingFixtures.LocationPath)))
        val unknown = rows.find(_.getAs[String]("status") == "Unknown").get
        assert(unknown.getAs[String]("error").contains("InvalidResponse"))
        assert(unknown.getAs[String]("rawResponse") == body)
        assert(server.requests.map(_.method) == Seq("POST", "POST", "POST"))
      }
    }
  }

  test("bounded JSON polling retains the accepted state and raw submission") {
    invalidResponseBodies.foreach { body =>
      ContentUnderstandingStub.withReplies(Seq(ContentUnderstandingFixtures.Accepted,
        ContentUnderstandingStubReply(200, body))) { server =>
        val stage = new ContentUnderstanding().setEndpoint(server.endpoint).setOutputCol("result")
          .setDocumentBytes(Array[Byte](1)).setPollingDelay(0).setMaxPollAttempts(1)
        val row = stage.transform(spark.range(1).coalesce(1)).head().getAs[Row]("result")
        assert(row.getAs[String]("status") == "Running")
        assert(row.getAs[String]("error").contains("InvalidResponse"))
        assert(row.getAs[String]("operationLocation").contains(ContentUnderstandingFixtures.LocationPath))
        assert(row.getAs[String]("rawResponse") == ContentUnderstandingFixtures.Accepted.body)
        assert(server.requests.map(_.method) == Seq("POST", "GET"))
      }
    }
  }

  test("bounded JSON management failures retain raw responses and do not replay requests") {
    for (body <- invalidResponseBodies; create <- Seq(false, true)) {
      ContentUnderstandingStub.withReplies(Seq(ContentUnderstandingStubReply(200, body))) { server =>
        val stage = new ContentUnderstanding().setEndpoint(server.endpoint).setAnalyzerId("custom")
        val error = intercept[ContentUnderstandingException] {
          if (create) stage.createAnalyzer("""{"baseAnalyzerId":"prebuilt-document"}""", allowReplace = false)
          else stage.getAnalyzer()
        }
        assert(error.response.error.exists(_.contains("InvalidResponse")))
        assert(error.response.rawResponse == body)
        assert(server.requests.map(_.method) == Seq(if (create) "PUT" else "GET"))
      }
    }
  }

  test("bounded analyzer definitions are rejected before provisioning") {
    ContentUnderstandingStub.withReplies(Seq(ContentUnderstandingFixtures.Accepted)) { server =>
      val stage = new ContentUnderstanding().setEndpoint(server.endpoint).setAnalyzerId("custom")
      invalidResponseBodies.foreach { body =>
        val error = intercept[IllegalArgumentException](stage.createAnalyzer(body, allowReplace = false))
        assert(error.getMessage.contains("definitionJson"))
      }
      assert(server.requests.isEmpty)
    }
  }

  test("invalid analyzer creation bodies recover through the accepted management handle") {
    invalidResponseBodies.foreach { body =>
      val location = "/contentunderstanding/analyzers/custom/operations/create-1?api-version=2025-11-01"
      val ready = """{"analyzerId":"custom","status":"ready"}"""
      ContentUnderstandingStub.withReplies(Seq(
        ContentUnderstandingStubReply(202, body, Map("Operation-Location" -> ("$ROOT" + location))),
        ContentUnderstandingStubReply(200, """{"status":"Succeeded"}"""),
        ContentUnderstandingStubReply(200, ready))) { server =>
        val stage = new ContentUnderstanding().setEndpoint(server.endpoint).setAnalyzerId("custom").setPollingDelay(0)
        assert(stage.createAnalyzer("""{"baseAnalyzerId":"prebuilt-document"}""", allowReplace = false) == ready)
        assert(server.requests.map(_.method) == Seq("PUT", "GET", "GET"))
        assert(server.requests(1).path == location.takeWhile(_ != '?'))
      }
    }
  }

  test("JSON limits ignore escaped string contents and allow a 100-character number") {
    val literal = JsString(("[\\\"" * 1001) + ("9" * 101)).compactPrint
    val body = s"""{"status":"Running","n":${"9" * 100},"text":$literal}"""
    ContentUnderstandingStub.withReplies(Seq(ContentUnderstandingFixtures.Accepted.copy(body = body))) { server =>
      val stage = new ContentUnderstanding().setEndpoint(server.endpoint).setOutputCol("result")
        .setDocumentBytes(Array[Byte](1)).setOperationMode("submit")
      val row = stage.transform(spark.range(1).coalesce(1)).head().getAs[Row]("result")
      assert(row.getAs[String]("status") == "Running")
      assert(row.getAs[String]("rawResponse") == body)
      assert(server.requests.size == 1)
    }
  }

  test("nested service error-code inspection is stack independent") {
    var error: JsValue = JsObject("code" -> JsString("Deep"))
    (1 to 1000).foreach { _ =>
      error = JsObject("innererror" -> error, "details" -> JsArray(JsObject("code" -> JsString("Side"))))
    }
    assert(ContentUnderstandingProtocol.FailureClassifier.errorCodes(error) == Set("Deep", "Side"))
  }

  test("deep service errors preserve accepted handles even when recursive decoding cannot complete") {
    val nested = ("""{"innererror":""" * 995) + """{"code":"InvalidContent"}""" + ("}" * 995)
    val body = s"""{"status":"Failed","error":$nested}"""
    ContentUnderstandingStub.withReplies(Seq(ContentUnderstandingFixtures.Accepted.copy(body = body))) { server =>
      val stage = new ContentUnderstanding().setEndpoint(server.endpoint).setOutputCol("result")
        .setDocumentBytes(Array[Byte](1)).setOperationMode("submit").setConcurrency(2)
      val row = stage.transform(spark.range(1).coalesce(1)).head().getAs[Row]("result")
      assert(Set("Failed", "Unknown").contains(row.getAs[String]("status")))
      val error = row.getAs[String]("error")
      assert(error.contains("InvalidResponse") ||
        (row.getAs[String]("status") == "Failed" && error.contains("InvalidContent")))
      assert(row.getAs[String]("operationLocation").contains(ContentUnderstandingFixtures.LocationPath))
      assert(row.getAs[String]("rawResponse") == body)
      assert(server.requests.map(_.method) == Seq("POST"))
    }
  }

  test("oversized response headers are ambiguous and never replay a submission") {
    val excessiveHeaders = Seq(Map("X-Large" -> ("x" * 65536)),
      (1 to 201).map(index => s"X-Header-$index" -> "value").toMap)
    excessiveHeaders.foreach { headers =>
      val reply = ContentUnderstandingFixtures.Accepted.copy(headers =
        ContentUnderstandingFixtures.Accepted.headers ++ headers)
      ContentUnderstandingStub.withReplies(Seq(reply)) { server =>
        val stage = new ContentUnderstanding().setEndpoint(server.endpoint).setOutputCol("result")
          .setDocumentBytes(Array[Byte](1)).setOperationMode("submit")
        val row = stage.transform(spark.range(1).coalesce(1)).head().getAs[Row]("result")
        assert(row.getAs[String]("status") == "Unknown")
        assert(row.getAs[String]("error").contains("TransportError"))
        assert(Option(row.getAs[String]("operationLocation")).isEmpty)
        assert(server.requests.map(_.method) == Seq("POST"))
      }
    }
  }

  test("overlong numeric Retry-After uses the configured polling fallback") {
    val accepted = ContentUnderstandingFixtures.Accepted.copy(headers =
      ContentUnderstandingFixtures.Accepted.headers + ("Retry-After" -> ("9" * 200)))
    ContentUnderstandingStub.withReplies(Seq(accepted,
      ContentUnderstandingStubReply(200, ContentUnderstandingFixtures.Succeeded))) { server =>
      val stage = new ContentUnderstanding().setEndpoint(server.endpoint).setOutputCol("result")
        .setDocumentBytes(Array[Byte](1)).setPollingDelay(0).setMaxOperationDuration(1000)
      val row = stage.transform(spark.range(1).coalesce(1)).head().getAs[Row]("result")
      assert(row.getAs[String]("status") == "Succeeded")
      assert(server.requests.map(_.method) == Seq("POST", "GET"))
    }
  }

  private def withSessions(inputSensitive: Boolean, activeSensitive: Boolean)(test: SparkSession => Unit): Unit = {
    val input = spark.newSession()
    input.conf.set("spark.sql.caseSensitive", inputSensitive.toString)
    val active = spark.newSession()
    active.conf.set("spark.sql.caseSensitive", activeSensitive.toString)
    val previous = SparkSession.getActiveSession
    SparkSession.setActiveSession(active)
    try {
      test(input)
      assert(SparkSession.getActiveSession.contains(active))
      assert(input.conf.get("spark.sql.caseSensitive") == inputSensitive.toString)
      assert(active.conf.get("spark.sql.caseSensitive") == activeSensitive.toString)
    } finally {
      previous match {
        case Some(session) => SparkSession.setActiveSession(session)
        case None => SparkSession.clearActiveSession()
      }

    }
  }

  test("management retry budget preserves failures before and after analyzer creation") {
    val failures = Seq(
      ContentUnderstandingStubReply(503, """{"error":{"code":"Unavailable"}}""") -> "Unavailable",
      ContentUnderstandingStubReply(429, """{"error":{"code":"Throttled"}}""") -> "Throttled",
      ContentUnderstandingStubReply(200, "", disconnect = true) -> "TransportError")
    for ((failure, code) <- failures; create <- Seq(false, true)) {
      val operation = "/contentunderstanding/analyzers/custom/operations/create-1?api-version=2025-11-01"
      val prefix = if (create) Seq(
        ContentUnderstandingStubReply(202, "{}",
          Map("Operation-Location" -> ("$ROOT" + operation), "Retry-After" -> "0")),
        ContentUnderstandingStubReply(200, """{"status":"Succeeded"}""")) else Seq.empty
      ContentUnderstandingStub.withReplies(prefix :+ failure) { server =>
        val stage = new ContentUnderstanding().setEndpoint(server.endpoint).setAnalyzerId("custom")
          .setMaxOperationDuration(5000).setPollingDelay(10000)
        val error = intercept[ContentUnderstandingException] {
          if (create) stage.createAnalyzer("""{"baseAnalyzerId":"prebuilt-document"}""", allowReplace = false)
          else stage.getAnalyzer()
        }.response
        assert(error.error.exists(_.contains(code)))
        assert(!error.error.exists(_.contains("OperationTimedOut")))
        assert(error.rawResponse == failure.body)
        assert(error.httpStatus == (if (failure.disconnect) 0 else failure.status))
        assert(error.operationLocation == (if (create) Some(server.endpoint + operation) else None))
        assert(server.requests.map(_.method) == (if (create) Seq("PUT", "GET", "GET") else Seq("GET")))
      }
    }
  }

  private def bindingFrame(session: SparkSession, endpoint: String, mode: String,
                           binary: Boolean, sensitive: Boolean): DataFrame = {
    val value = if (mode == "poll") endpoint + ContentUnderstandingFixtures.LocationPath
      else if (binary) Array[Byte](1, 2) else "https://example.test/document"
    val other = if (binary) Array[Byte](9) else "wrong-column"
    val sourceType = if (binary) BinaryType else StringType
    val schema = new StructType().add("document", sourceType)
      .add(if (sensitive) "DOCUMENT" else "otherDocument", sourceType)
      .add("mime", StringType).add(if (sensitive) "MIME" else "otherMime", StringType)
      .add("key", StringType).add(if (sensitive) "KEY" else "otherKey", StringType)
      .add("PARSEDINPUT", StringType)
    session.createDataFrame(Seq(Row(value, other, "text/plain", "wrong-mime",
      "synthetic-key", "wrong-key", "preserved")).asJava, schema).coalesce(1)
  }

  private def pipelineSchema(stage: ContentUnderstanding, frame: DataFrame): StructType = {
    val previous = SparkSession.getActiveSession
    SparkSession.setActiveSession(frame.sparkSession)
    try {
      val pipeline = new Pipeline().setStages(Array(stage))
      val planned = pipeline.transformSchema(frame.schema)
      assert(pipeline.fit(frame).transform(frame).schema == planned)
      planned
    } finally {
      previous match {
        case Some(session) => SparkSession.setActiveSession(session)
        case None => SparkSession.clearActiveSession()
      }
    }
  }

  private def assertSessionBindings(mode: String, binary: Boolean,
                                    inputSensitive: Boolean, activeSensitive: Boolean): Unit = {
    withSessions(inputSensitive, activeSensitive) { input =>
      val succeeded = ContentUnderstandingStubReply(200, ContentUnderstandingFixtures.Succeeded)
      val replies = if (mode == "poll") Seq(succeeded) else Seq(ContentUnderstandingFixtures.Accepted, succeeded)
      ContentUnderstandingStub.withReplies(replies) { server =>
        val frame = bindingFrame(input, server.endpoint, mode, binary, inputSensitive)
        val stage = new ContentUnderstanding().setEndpoint(server.endpoint).setOutputCol("result")
          .setOperationMode(mode).setPollingDelay(0).setSubscriptionKeyCol("key").setMimeTypeCol("mime")
        if (mode == "poll") stage.setOperationLocationCol("document")
        else if (binary) stage.setDocumentBytesCol("document")
        else stage.setDocumentUrlCol("document")
        val result = stage.transform(frame)
        assert(result.schema == pipelineSchema(stage, frame))
        assert(result.schema.fields.take(frame.schema.length).sameElements(frame.schema.fields))
        assert(server.requests.isEmpty)
        val row = result.head()
        assert(row.getAs[String]("PARSEDINPUT") == "preserved")
        assert(row.getAs[Row]("result").getAs[String]("status") ==
          (if (mode == "submit") "Running" else "Succeeded"))
        val methods = if (mode == "analyze") Seq("POST", "GET") else Seq(if (mode == "poll") "GET" else "POST")
        assert(server.requests.map(_.method) == methods)
        assert(server.requests.forall(_.headers("ocp-apim-subscription-key") == "synthetic-key"))
        if (mode != "poll") {
          val source = if (binary) """"data":"AQI="""" else """"url":"https://example.test/document""""
          assert(server.requests.head.body.contains(source))
          assert(server.requests.head.body.contains(""""mimeType":"text/plain""""))
        } else {
          assert(server.requests.head.path == "/contentunderstanding/analyzerResults/op-1")
        }
      }
    }
  }

  test("direct schema planning honors Dataset sessions for bound inputs and poll handles") {
    for (inputSensitive <- Seq(true, false); activeSensitive <- Seq(true, false);
         mode <- Seq("submit", "analyze", "poll"); binary <- Seq(false, true) if mode != "poll" || !binary) {
      withClue(s"mode=$mode binary=$binary input=$inputSensitive active=$activeSensitive: ") {
        assertSessionBindings(mode, binary, inputSensitive, activeSensitive)
      }
    }
  }

  test("ambiguous bindings are rejected using Dataset rules before HTTP dispatch") {
    for (sensitive <- Seq(true, false); duplicate <- Seq(true, false) if duplicate || !sensitive) {
      withSessions(sensitive, !sensitive) { input =>
        val schema = new StructType().add("document", StringType)
          .add(if (duplicate) "document" else "DOCUMENT", StringType)
        val frame = input.createDataFrame(Seq(Row("https://example.test/a", "https://example.test/b")).asJava,
          schema).coalesce(1)
        ContentUnderstandingStub.withReplies(Seq(ContentUnderstandingFixtures.Accepted)) { server =>
          val stage = new ContentUnderstanding().setEndpoint(server.endpoint).setDocumentUrlCol("document")
          val error = intercept[IllegalArgumentException](stage.transform(frame))
          assert(error.getMessage.contains("ambiguous"))
          assert(server.requests.isEmpty)
        }
      }
    }
  }

  test("Dataset validation rejects conflicting outputs and invalid bindings without requests") {
    Seq(true, false).foreach { sensitive =>
      withSessions(sensitive, !sensitive) { input =>
        val frame = input.createDataFrame(Seq(Row("https://example.test/document", "preserved")).asJava,
          new StructType().add("document", StringType).add("analysis", StringType)).coalesce(1)
        ContentUnderstandingStub.withReplies(Seq(ContentUnderstandingFixtures.Accepted)) { server =>
          val stage = new ContentUnderstanding().setEndpoint(server.endpoint).setDocumentUrlCol("document")
          val collision = if (sensitive) "analysis" else "ANALYSIS"
          val invalid = Seq(
            stage.copy(org.apache.spark.ml.param.ParamMap.empty).setOutputCol(collision),
            stage.copy(org.apache.spark.ml.param.ParamMap.empty).setErrorCol(collision),
            stage.copy(org.apache.spark.ml.param.ParamMap.empty).setOutputCol("result")
              .setErrorCol(if (sensitive) "result" else "RESULT"),
            stage.copy(org.apache.spark.ml.param.ParamMap.empty).setDocumentUrlCol("Document"))
          invalid.foreach(candidate => intercept[IllegalArgumentException](candidate.transform(frame)))
          val wrongType = frame.withColumn("document", org.apache.spark.sql.functions.lit(1))
          val error = intercept[IllegalArgumentException](stage.transform(wrongType))
          assert(error.getMessage.contains("must have type string"))
          assert(server.requests.isEmpty)
        }
      }
    }
  }

  test("missing or unsafe handles preserve the original response-read diagnostic") {
    val truncated = ContentUnderstandingStubReply(202, ContentUnderstandingFixtures.Running, truncate = true)
    val oversized = ContentUnderstandingStubReply(202, ContentUnderstandingFixtures.Running + (" " * 1024))
    for ((reply, code) <- Seq(truncated -> "TransportError", oversized -> "ResponseTooLarge");
         headers <- Seq(Map.empty[String, String], Map("Operation-Location" -> "https://other.invalid/result"));
         mode <- Seq("submit", "analyze")) {
      ContentUnderstandingStub.withReplies(Seq(reply.copy(headers = headers))) { server =>
        val stage = new ContentUnderstanding().setEndpoint(server.endpoint).setOutputCol("result")
          .setDocumentBytes(Array[Byte](1)).setOperationMode(mode).setTimeout(1).setMaxResponseBytes(128)
        val response = stage.transform(spark.range(1).coalesce(1)).head().getAs[Row]("result")
        assert(response.getAs[String]("status") == "Unknown")
        assert(response.getAs[String]("error").contains(code))
        assert(Option(response.getAs[String]("operationLocation")).isEmpty)
        assert(server.requests.map(_.method) == Seq("POST"))
      }
    }
  }

  test("admission rejection headers remain definitive when reading the error body fails") {
    for (status <- Seq(401, 403, 429); columnAuth <- Seq(true, false); oversized <- Seq(true, false)) {
      val body = if (oversized) " " * 1024 else """{"error":{"code":"Rejected"}}"""
      val reply = ContentUnderstandingStubReply(status, body, truncate = !oversized)
      ContentUnderstandingStub.withReplies(Seq(reply, ContentUnderstandingFixtures.Accepted)) { server =>
        val frame = spark.createDataFrame(Seq(Row("synthetic-key"), Row("synthetic-key")).asJava,
          new StructType().add("key", StringType)).coalesce(1)
        val stage = new ContentUnderstanding().setEndpoint(server.endpoint).setOutputCol("result")
          .setDocumentBytes(Array[Byte](1)).setOperationMode("submit").setConcurrency(1)
          .setTimeout(1).setMaxResponseBytes(128)
        if (columnAuth) stage.setSubscriptionKeyCol("key") else stage.setSubscriptionKey("synthetic-key")
        val rows = stage.transform(frame).collect().map(_.getAs[Row]("result"))
        assert(rows.head.getAs[String]("status") == "Rejected")
        assert(rows.head.getAs[Int]("httpStatus") == status)
        assert(Option(rows.head.getAs[String]("operationLocation")).isEmpty)
        val code = if (oversized) "ResponseTooLarge" else "ResponseBodyReadFailed"
        assert(rows.head.getAs[String]("error").contains(code))
        assert(!rows.head.getAs[String]("error").contains("may have been accepted"))
        val stopped = !columnAuth && status != 429
        assert(rows(1).getAs[String]("status") == (if (stopped) "Rejected" else "Running"))
        assert(rows(1).getAs[Int]("httpStatus") == (if (stopped) 0 else 202))
        assert(server.requests.size == (if (stopped) 1 else 2))
        if (stopped) assert(rows.head.getAs[String]("error").contains("SharedConfigurationFailure"))
      }
    }
  }

  test("shared failures do not expose service messages that imitate connector diagnostics") {
    Seq("TransportError", "OperationTimedOut", "ResponseTooLarge", "ResponseBodyReadFailed").foreach { code =>
      val body = s"""{"error":{"code":"$code","message":"synthetic-sensitive-detail"}}"""
      ContentUnderstandingStub.withReplies(Seq(ContentUnderstandingStubReply(401, body))) { server =>
        val stage = new ContentUnderstanding().setEndpoint(server.endpoint).setOutputCol("result")
          .setDocumentBytes(Array[Byte](1)).setSubscriptionKey("synthetic-key").setOperationMode("submit")
        val result = stage.transform(spark.range(1).coalesce(1)).head().getAs[Row]("result")
        assert(result.getAs[String]("error").contains("SharedConfigurationFailure"))
        assert(!result.getAs[String]("error").contains("synthetic-sensitive-detail"))
        assert(result.getAs[String]("rawResponse").contains("synthetic-sensitive-detail"))
      }
    }
  }

  test("unreadable 404 responses preserve diagnostics without stopping subsequent submissions") {
    val truncated = ContentUnderstandingStubReply(404, """{"error":{"code":"AnalyzerNotFound"}}""", truncate = true)
    val oversized = truncated.copy(body = " " * 1024, truncate = false)
    for ((reply, code) <- Seq(truncated -> "TransportError", oversized -> "ResponseTooLarge");
         columnAnalyzer <- Seq(false, true)) {
      ContentUnderstandingStub.withReplies(Seq(reply, ContentUnderstandingFixtures.Accepted)) { server =>
        val frame = spark.createDataFrame(Seq(Row("prebuilt-read"), Row("prebuilt-read")).asJava,
          new StructType().add("analyzer", StringType)).coalesce(1)
        val stage = new ContentUnderstanding().setEndpoint(server.endpoint).setOutputCol("result")
          .setDocumentBytes(Array[Byte](1)).setOperationMode("submit").setConcurrency(1)
          .setTimeout(1).setMaxResponseBytes(128)
        if (columnAnalyzer) stage.setAnalyzerIdCol("analyzer")
        val rows = stage.transform(frame).collect().map(_.getAs[Row]("result"))
        assert(rows.head.getAs[String]("status") == "Unknown")
        assert(rows.head.getAs[String]("error").contains(code))
        assert(!rows.head.getAs[String]("error").contains("SharedConfigurationFailure"))
        assert(Option(rows.head.getAs[String]("operationLocation")).isEmpty)
        assert(rows(1).getAs[String]("status") == "Running")
        assert(server.requests.map(_.method) == Seq("POST", "POST"))
      }
    }
  }

  test("unsafe analyzer creation handles preserve response-read diagnostics") {
    val truncated = ContentUnderstandingStubReply(202, ContentUnderstandingFixtures.Running, truncate = true)
    val oversized = truncated.copy(body = " " * 1024, truncate = false)
    for ((reply, code) <- Seq(truncated -> "TransportError", oversized -> "ResponseTooLarge");
         status <- Seq(200, 201, 202); location <- Seq("https://other.invalid/result", "/relative")) {
      val response = reply.copy(status = status, headers = Map("Operation-Location" -> location))
      ContentUnderstandingStub.withReplies(Seq(response)) { server =>
        val stage = new ContentUnderstanding().setEndpoint(server.endpoint).setAnalyzerId("custom")
          .setTimeout(1).setMaxResponseBytes(128)
        val error = intercept[ContentUnderstandingException] {
          stage.createAnalyzer("""{"baseAnalyzerId":"prebuilt-document"}""", allowReplace = false)
        }
        assert(error.response.error.exists(_.contains(code)))
        assert(error.response.httpStatus == status)
        assert(error.response.operationLocation.isEmpty)
        assert(server.requests.map(_.method) == Seq("PUT"))
      }
    }
  }

  test("a transient last poll retains its diagnostic and resumable handle at the attempt limit") {
    val body = """{"error":{"code":"ServiceBusy"}}"""
    val replies = Seq(ContentUnderstandingStubReply(503, body) -> "ServiceBusy",
      ContentUnderstandingStubReply(429, body) -> "ServiceBusy",
      ContentUnderstandingStubReply(200, ContentUnderstandingFixtures.Running, truncate = true) -> "TransportError")
    for ((reply, code) <- replies; mode <- Seq("analyze", "poll")) {
      val responses = if (mode == "analyze") Seq(ContentUnderstandingFixtures.Accepted, reply) else Seq(reply)
      ContentUnderstandingStub.withReplies(responses) { server =>
        val stage = new ContentUnderstanding().setEndpoint(server.endpoint).setOutputCol("result")
          .setOperationMode(mode).setPollingDelay(0).setMaxPollAttempts(1).setTimeout(1)
        if (mode == "poll") stage.setOperationLocation(server.endpoint + ContentUnderstandingFixtures.LocationPath)
        else stage.setDocumentBytes(Array[Byte](1))
        val row = stage.transform(spark.range(1).coalesce(1)).head().getAs[Row]("result")
        assert(row.getAs[String]("status") == (if (mode == "analyze") "Running" else "Unknown"))
        assert(row.getAs[String]("error").contains(code))
        assert(!row.getAs[String]("error").contains("PollAttemptsExhausted"))
        assert(!row.getAs[String]("error").contains("OperationTimedOut"))
        assert(row.getAs[String]("operationLocation").contains(ContentUnderstandingFixtures.LocationPath))
        assert(server.requests.map(_.method) == (if (mode == "analyze") Seq("POST", "GET") else Seq("GET")))
      }
    }
  }

  @tailrec
  private def awaitUntilReleased(release: CountDownLatch): Unit = {
    val released = try {
      release.await(10, TimeUnit.MILLISECONDS)
    } catch {
      case _: InterruptedException => false
    }
    if (!released) awaitUntilReleased(release)
  }

  test("DNS resolver bounds blocked lookups, reports saturation, and waits for released capacity") {
    val lookupCount = BudgetedDnsResolver.MaxConcurrentLookups
    val loopback = InetAddress.getByAddress(Array[Byte](127, 0, 0, 1))
    val entered = new CountDownLatch(lookupCount)
    val release = new CountDownLatch(1)
    val completed = new CountDownLatch(lookupCount)
    val delegated = new AtomicInteger(0)
    val timedOut = new AtomicInteger(0)
    val failure = new AtomicReference[Option[Throwable]](None)
    val waitingStarted = new CountDownLatch(1)
    val waitingCompleted = new CountDownLatch(1)
    val recovered = new AtomicInteger(0)
    val delegate = new DnsResolver {
      override def resolve(host: String): Array[InetAddress] = {
        delegated.incrementAndGet()
        entered.countDown()
        awaitUntilReleased(release)
        Array(loopback)
      }
    }
    val resolver = new BudgetedDnsResolver(() => 500L, delegate)
    val waiting = new Thread(new Runnable {
      override def run(): Unit = {
        waitingStarted.countDown()
        try {
          val addresses = new BudgetedDnsResolver(() => 5000L, delegate).resolve("dns-waiting.test")
          assert(addresses.sameElements(Array(loopback)))
          recovered.incrementAndGet()
        } catch {
          case error: Throwable => failure.compareAndSet(None, Some(error))
        } finally {
          waitingCompleted.countDown()
        }
      }
    })
    val workers = (0 until lookupCount).map { index =>
      new Thread(new Runnable {
        override def run(): Unit = {
          try {
            resolver.resolve(s"dns-$index.test")
            failure.compareAndSet(None, Some(new AssertionError("Blocked DNS should time out.")))
          } catch {
            case _: OperationDeadlineExceeded => timedOut.incrementAndGet()
            case error: Throwable => failure.compareAndSet(None, Some(error))
          } finally {
            completed.countDown()
          }
        }
      })
    }

    workers.foreach(_.start())
    try {
      assert(entered.await(5, TimeUnit.SECONDS), "All bounded DNS workers should start.")
      assert(completed.await(5, TimeUnit.SECONDS), "Every caller should meet its operation budget.")
      assert(timedOut.get() == lookupCount)
      assert(delegated.get() == lookupCount)
      val saturated = intercept[DnsCapacityExceeded](resolver.resolve("dns-saturated.test"))
      assert(saturated.getMessage.contains("DnsCapacityExceeded"))
      waiting.start()
      assert(waitingStarted.await(5, TimeUnit.SECONDS))
      assert(!waitingCompleted.await(100, TimeUnit.MILLISECONDS),
        "A caller with remaining budget should wait for DNS capacity.")
      assert(delegated.get() == lookupCount)
      release.countDown()
      assert(waitingCompleted.await(5, TimeUnit.SECONDS))
      assert(recovered.get() == 1)
      assert(delegated.get() == lookupCount + 1)
    } finally {
      release.countDown()
      waiting.join(6000)
      workers.foreach(_.join(1000))
    }
    failure.get().foreach(error => throw error)
  }

  test("one blocked deadline abort does not delay another operation deadline") {
    val firstAbortStarted = new CountDownLatch(1)
    val releaseFirstAbort = new CountDownLatch(1)
    val secondAborted = new CountDownLatch(1)
    val firstRequest = new HttpGet("https://first.example.invalid") {
      override def abort(): Unit = {
        firstAbortStarted.countDown()
        releaseFirstAbort.await()
      }
      override def releaseConnection(): Unit = {
        assert(releaseFirstAbort.getCount == 0, "Do not reset a request while cancellation owns it.")
        super.releaseConnection()
      }
    }
    val secondRequest = new HttpGet("https://second.example.invalid") {
      override def abort(): Unit = secondAborted.countDown()
    }
    val firstDeadline = ContentUnderstandingProtocol.DeadlineControl.schedule(firstRequest, 0)
    val secondDeadline = ContentUnderstandingProtocol.DeadlineControl.schedule(secondRequest, 0)
    try {
      assert(firstAbortStarted.await(5, TimeUnit.SECONDS))
      assert(secondAborted.await(5, TimeUnit.SECONDS))
      ContentUnderstandingProtocol.DeadlineControl.release(firstRequest, firstDeadline)
    } finally {
      releaseFirstAbort.countDown()
      ContentUnderstandingProtocol.DeadlineControl.release(firstRequest, firstDeadline)
      ContentUnderstandingProtocol.DeadlineControl.release(secondRequest, secondDeadline)
    }
  }

  test("HTTP timeout respects remaining budget when deadline abort workers are occupied") {
    val abortsStarted = new CountDownLatch(4)
    val releaseAborts = new CountDownLatch(1)
    val releaseResponse = new CountDownLatch(1)
    val server = new ContentUnderstandingStub(_ => {
      releaseResponse.await(10, TimeUnit.SECONDS)
      ContentUnderstandingFixtures.Accepted
    })
    val deadlines = (1 to 4).map { _ =>
      val request = new HttpGet("https://blocked-abort.example.invalid") {
        override def abort(): Unit = {
          abortsStarted.countDown()
          releaseAborts.await()
        }
      }
      request -> ContentUnderstandingProtocol.DeadlineControl.schedule(request, 0)
    }
    try {
      assert(abortsStarted.await(5, TimeUnit.SECONDS))
      val settings = ContentUnderstandingProtocol.Settings(server.endpoint, 5000, 1, 0, 1024, 1000,
        sharedAnalyzer = false, sharedModels = false)
      val request = new HTTPRequestData(new HttpPost(server.endpoint + "/analyze"))
      val started = System.nanoTime()
      val response = ContentUnderstandingProtocol.execute(settings, request, "submit")
      assert(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < 4000)
      assert(response.status == "Unknown")
      assert(response.operationLocation.isEmpty)
      assert(server.requests.size == 1)
    } finally {
      releaseAborts.countDown()
      releaseResponse.countDown()
      deadlines.foreach { case (request, deadline) =>
        ContentUnderstandingProtocol.DeadlineControl.release(request, deadline)
      }
      server.close()
    }
  }

  test("connection refusal is reported as an unsent submission") {
    val listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress)
    val endpoint = s"http://127.0.0.1:${listener.getLocalPort}"
    listener.close()
    val settings = ContentUnderstandingProtocol.Settings(endpoint, 1000, 1, 0, 1024, 30000,
      sharedAnalyzer = false, sharedModels = false)
    val response = ContentUnderstandingProtocol.execute(settings,
      new HTTPRequestData(new HttpPost(endpoint + "/analyze")), "submit")
    assert(response.status == "Rejected")
    assert(response.operationLocation.isEmpty)
    assert(response.error.exists(_.contains("RequestNotSent")))
    assert(response.error.exists(_.contains("HttpHostConnectException")))
  }

  test("a deadline during TLS connection setup is an unsent submission") {
    val listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress)
    listener.setSoTimeout(5000)
    val release = new CountDownLatch(1)
    val connected = new CountDownLatch(1)
    val worker = new Thread(() => {
      val socket = listener.accept()
      try {
        connected.countDown()
        release.await(5, TimeUnit.SECONDS)
      } finally {
        socket.close()
      }
    })
    worker.setDaemon(true)
    worker.start()
    try {
      val endpoint = s"https://127.0.0.1:${listener.getLocalPort}"
      val settings = ContentUnderstandingProtocol.Settings(endpoint, 5000, 1, 0, 1024, 1000,
        sharedAnalyzer = false, sharedModels = false)
      val response = ContentUnderstandingProtocol.execute(settings,
        new HTTPRequestData(new HttpPost(endpoint + "/analyze")), "submit")
      assert(connected.getCount == 0)
      assert(response.status == "Rejected")
      assert(response.operationLocation.isEmpty)
      assert(response.error.exists(_.contains("RequestNotSent")))
    } finally {
      release.countDown()
      listener.close()
      worker.join(6000)
    }
    assert(!worker.isAlive)
  }

  test("ambiguous rejection responses keep their diagnostic and do not stop subsequent submissions") {
    val replies = Seq(
      ContentUnderstandingStubReply(404, """{"error":{"code":"Rejected"}}""", truncate = true),
      ContentUnderstandingStubReply(401, """{"error":{"code":"Unauthorized"}}""",
        ContentUnderstandingFixtures.Accepted.headers))
    replies.foreach { reply =>
      ContentUnderstandingStub.withReplies(Seq(reply, ContentUnderstandingFixtures.Accepted)) { server =>
        val transformer = new ContentUnderstanding().setEndpoint(server.endpoint).setOutputCol("result")
          .setDocumentBytes(Array[Byte](1)).setSubscriptionKey("synthetic-key").setAnalyzerId("missing")
          .setOperationMode("submit").setConcurrency(1).setTimeout(1)
        val rows = transformer.transform(spark.range(0L, 2L, 1L, 1)).collect()
          .map(_.getAs[org.apache.spark.sql.Row]("result"))
        assert(rows.head.getAs[String]("status") == "Unknown")
        assert(!rows.head.getAs[String]("error").contains("SharedConfigurationFailure"))
        if (reply.truncate) {
          assert(rows.head.getAs[String]("error").contains("TransportError"))
          assert(rows.head.getAs[String]("operationLocation") == null)
        }
        assert(rows(1).getAs[String]("status") == "Running")
        assert(rows(1).getAs[String]("operationLocation") ==
          server.endpoint + ContentUnderstandingFixtures.LocationPath)
        assert(server.requests.size == 2)
      }
    }
  }

  test("scalar fallback rejection stops the partition including subsequent column credentials") {
    ContentUnderstandingStub.withReplies(Seq(
      ContentUnderstandingStubReply(401, """{"error":{"code":"Unauthorized"}}"""))) { server =>
      val frame = spark.createDataFrame(Seq(Row(Option.empty[String].orNull), Row("valid-column-key")).asJava,
        new StructType().add("key", StringType)).coalesce(1)
      val stage = new ContentUnderstanding().setEndpoint(server.endpoint).setOutputCol("result")
        .setDocumentBytes(Array[Byte](1)).setSubscriptionKeyCol("key").setAADToken("synthetic-token")
        .setOperationMode("submit").setConcurrency(1)
      val results = stage.transform(frame).collect().map(_.getAs[Row]("result"))
      assert(results.map(_.getAs[Int]("httpStatus")).toSeq == Seq(401, 0))
      assert(results.forall(_.getAs[String]("error").contains("SharedConfigurationFailure")))
      assert(server.requests.size == 1)
      assert(server.requests.head.headers("authorization") == "Bearer synthetic-token")
    }
  }

  test("the unused base handler fails explicitly instead of creating per-request circuit state") {
    val method = classOf[ContentUnderstanding].getDeclaredMethod("handlingFunc",
      classOf[org.apache.http.impl.client.CloseableHttpClient], classOf[HTTPRequestData])
    method.setAccessible(true)
    val stage = new ContentUnderstanding().setEndpoint("http://127.0.0.1:1")
    val error = intercept[java.lang.reflect.InvocationTargetException] {
      method.invoke(stage, Option.empty[org.apache.http.impl.client.CloseableHttpClient].orNull,
        Option.empty[HTTPRequestData].orNull)
    }
    assert(error.getCause.isInstanceOf[UnsupportedOperationException])
  }

  test("schema-only pipeline planning requires the matching active session") {
    val inputSession = spark.newSession()
    inputSession.conf.set("spark.sql.caseSensitive", "true")
    val otherSession = spark.newSession()
    otherSession.conf.set("spark.sql.caseSensitive", "false")
    val frame = inputSession.createDataFrame(Seq(Row("keep")).asJava,
      new StructType().add("analysis", StringType)).coalesce(1)
    val previousSession = SparkSession.getActiveSession
    try {
      ContentUnderstandingStub.withReplies(Seq(ContentUnderstandingFixtures.Accepted)) { server =>
        val stage = new ContentUnderstanding().setEndpoint(server.endpoint).setOutputCol("ANALYSIS")
          .setDocumentBytes(Array[Byte](1)).setOperationMode("submit")
        val pipeline = new Pipeline().setStages(Array(stage))
        SparkSession.setActiveSession(otherSession)
        intercept[IllegalArgumentException](pipeline.fit(frame))
        SparkSession.setActiveSession(inputSession)
        val model = pipeline.fit(frame)
        val result = model.transform(frame)
        assert(result.schema == model.transformSchema(frame.schema))
        assert(server.requests.isEmpty)
        val row = result.head()
        assert(row.getAs[String]("analysis") == "keep")
        assert(row.getAs[Row]("ANALYSIS").getAs[String]("status") == "Running")
        assert(server.requests.size == 1)
      }
    } finally {
      previousSession match {
        case Some(session) => SparkSession.setActiveSession(session)
        case None => SparkSession.clearActiveSession()
      }
    }
  }

  test("service input bindings require exact column names regardless of case sensitivity") {
    Seq(true, false).foreach { caseSensitive =>
      val inputSession = spark.newSession()
      inputSession.conf.set("spark.sql.caseSensitive", caseSensitive.toString)
      val frame = inputSession.createDataFrame(Seq(Row("https://example.test/document")).asJava,
        new StructType().add("document", StringType)).coalesce(1)
      ContentUnderstandingStub.withReplies(Seq(ContentUnderstandingFixtures.Accepted)) { server =>
        val stage = new ContentUnderstanding().setEndpoint(server.endpoint).setOutputCol("result")
          .setDocumentUrlCol("Document").setOperationMode("submit")
        val error = intercept[IllegalArgumentException](stage.transform(frame))
        assert(error.getMessage.contains("column configured for documentUrl is missing"))
        assert(server.requests.isEmpty)
        val result = stage.setDocumentUrlCol("document").transform(frame).head().getAs[Row]("result")
        assert(result.getAs[String]("status") == "Running")
        assert(server.requests.size == 1)
      }
    }
  }

  test("shared failure circuit state is isolated per partition and action") {
    val server = new ContentUnderstandingStub(_ =>
      ContentUnderstandingStubReply(401, """{"error":{"code":"Unauthorized"}}"""))
    try {
      val transformer = new ContentUnderstanding().setEndpoint(server.endpoint).setOutputCol("result")
        .setDocumentBytes(Array[Byte](1)).setSubscriptionKey("synthetic-key").setOperationMode("submit")
        .setConcurrency(1)
      val results = transformer.transform(spark.range(0L, 4L, 1L, 2))
      (1 to 2).foreach { action =>
        val rows = results.collect().map(_.getAs[org.apache.spark.sql.Row]("result"))
        assert(rows.forall(_.getAs[String]("status") == "Rejected"))
        assert(rows.count(_.getAs[Int]("httpStatus") == 401) == 2)
        assert(rows.count(_.getAs[Int]("httpStatus") == 0) == 2)
        assert(server.requests.size == action * 2)
      }
    } finally {
      server.close()
    }
  }
}
