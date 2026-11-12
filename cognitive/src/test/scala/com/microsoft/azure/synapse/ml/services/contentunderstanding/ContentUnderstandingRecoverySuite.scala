// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.contentunderstanding

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.http.HTTPRequestData
import com.microsoft.azure.synapse.ml.services.ServiceAuthHeaders
import org.apache.http.{HttpHost, HttpRequest, HttpStatus, HttpVersion}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpPost, HttpRequestBase}
import org.apache.http.conn.ClientConnectionManager
import org.apache.http.conn.DnsResolver
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.message.BasicHttpResponse
import org.apache.http.params.HttpParams
import org.apache.http.protocol.HttpContext
import org.apache.spark.sql.types.{BinaryType, MapType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import spray.json._

import java.io.{ByteArrayInputStream, IOException, InputStream}
import java.lang.reflect.InvocationTargetException
import java.net.{InetAddress, ServerSocket, SocketTimeoutException, UnknownHostException}
import java.nio.charset.StandardCharsets
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.collection.JavaConverters._

class TestableDefaultAADTokenStage(uid: String) extends ContentUnderstanding(uid) {
  def this() = this("default-aad-token")

  setDefault(AADToken -> Left("synthetic-token"))
}

class ContentUnderstandingRecoverySuite extends TestBase {
  import ContentUnderstandingFixtures.{
    Accepted => accepted, AnalyzersPath, DefaultApiVersion, Failed => failed, LocationPath => locationPath,
    Running => running, Succeeded => succeeded}
  import ContentUnderstandingStub.withReplies

  private def stage(server: ContentUnderstandingStub): ContentUnderstanding =
    new ContentUnderstanding().setEndpoint(server.endpoint).setOutputCol("result").setErrorCol("error")
      .setDocumentBytes(Array[Byte](1)).setPollingDelay(0).setMaxPollAttempts(2)

  private def input: DataFrame =
    spark.createDataFrame(Seq(Row("doc")).asJava, new StructType().add("id", StringType)).coalesce(1)

  private val unicodeHeaderInjection = Seq(0x010d, 0x010a).map(_.toChar).mkString + "Injected: value"

  private def result(transformer: ContentUnderstanding): ContentUnderstandingResponse =
    ContentUnderstandingResponse.makeFromRowConverter(
      transformer.transform(input).head().getAs[Row]("result"))

  private def exceptionContains(error: Throwable, text: String): Boolean =
    Option(error).exists(value => Option(value.getMessage).exists(_.contains(text)) ||
      exceptionContains(value.getCause, text))

  test("large chunked and compressed responses retain handles and resume without resubmission") {
    val bytes = new Array[Byte](65536)
    new scala.util.Random(1234).nextBytes(bytes)
    val body = JsObject(succeeded.parseJson.asJsObject.fields +
      ("padding" -> JsString(java.util.Base64.getEncoder.encodeToString(bytes)))).compactPrint
    Seq(false, true).foreach { gzip =>
      withReplies(Seq(accepted, ContentUnderstandingStubReply(HttpStatus.SC_OK, body,
        chunked = true, gzip = gzip))) { server =>
        val transformer = stage(server).setMaxResponseBytes(1024)
        val limited = result(transformer)
        assert(limited.status == "Running")
        assert(limited.error.exists(_.contains("ResponseTooLarge")))
        assert(limited.operationLocation.contains(server.endpoint + locationPath))
        val resumed = result(transformer.setMaxResponseBytes(body.getBytes(StandardCharsets.UTF_8).length)
          .setOperationMode("poll").setOperationLocation(limited.operationLocation.get))
        assert(resumed.status == "Succeeded")
        assert(resumed.rawResponse == body)
        assert(server.requests.map(_.method) == Seq("POST", "GET", "GET"))
      }
    }
  }

  test("shared authentication rejection is a row result and preserves an accepted handle") {
    val unauthorized = ContentUnderstandingStubReply(HttpStatus.SC_UNAUTHORIZED,
      """{"error":{"code":"Unauthorized"}}""")
    withReplies(Seq(accepted, unauthorized)) { server =>
      val response = result(stage(server).setSubscriptionKey("synthetic-key"))
      assert(response.status == "Running")
      assert(response.httpStatus == HttpStatus.SC_UNAUTHORIZED)
      assert(response.operationLocation.contains(server.endpoint + locationPath))
      assert(server.requests.map(_.method) == Seq("POST", "GET"))
    }
    withReplies(Seq(unauthorized)) { server =>
      val response = result(stage(server).setSubscriptionKey("synthetic-key"))
      assert(response.status == "Rejected")
      assert(response.error.exists(_.contains("SharedConfigurationFailure")))
      assert(server.requests.map(_.method) == Seq("POST"))
    }
  }

  test("an accepted handle survives an oversized or malformed submission response") {
    val complete = """{"id":"op-1","status":"Succeeded","result":{"contents":[]}}"""
    Seq("x" * 129, "not-json").foreach { body =>
      withReplies(Seq(accepted.copy(body = body),
        ContentUnderstandingStubReply(HttpStatus.SC_OK, complete))) { server =>
        val response = result(stage(server).setMaxResponseBytes(128))
        assert(response.status == "Succeeded")
        assert(response.operationLocation.contains(server.endpoint + locationPath))
        assert(server.requests.map(_.method) == Seq("POST", "GET"))
      }
    }
  }

  test("successful nonstandard submissions without an operation handle remain unresumable") {
    val response = ContentUnderstandingStubReply(HttpStatus.SC_NON_AUTHORITATIVE_INFORMATION, running)
    withReplies(Seq(response)) { server =>
      val outcome = result(stage(server))
      assert(outcome.status == "Unknown")
      assert(outcome.error.exists(_.contains("MissingOperationLocation")))
      assert(server.requests.map(_.method) == Seq("POST"))
    }
  }

  test("malformed operation locations do not replace terminal service failures") {
    val invalidLocation = "$ROOT" + locationPath + "&unexpected=value"
    val reply = ContentUnderstandingStubReply(HttpStatus.SC_OK, failed,
      Map("Operation-Location" -> invalidLocation))
    withReplies(Seq(reply)) { server =>
      val response = result(stage(server).setOperationMode("submit"))
      assert(response.status == "Failed")
      assert(response.error.exists(_.contains("InvalidContent")))
      assert(!response.error.exists(_.contains("InvalidOperationLocation")))
      assert(server.requests.map(_.method) == Seq("POST"))
    }
  }

  test("unavailable results retain the handle and never resubmit") {
    Seq(HttpStatus.SC_NOT_FOUND, HttpStatus.SC_GONE).foreach { code =>
      withReplies(Seq(ContentUnderstandingStubReply(code, """{"error":{"code":"ResultNotFound"}}"""))) { server =>
        val response = result(stage(server).setOperationMode("poll")
          .setOperationLocation(server.endpoint + locationPath))
        assert(response.status == "ResultUnavailable")
        assert(response.operationLocation.contains(server.endpoint + locationPath))
        assert(server.requests.map(_.method) == Seq("GET"))
      }
    }
  }

  test("poll delay that exceeds the budget preserves the actionable service error") {
    val reply = ContentUnderstandingStubReply(HttpStatus.SC_SERVICE_UNAVAILABLE,
      """{"error":{"code":"ResponseTooLarge"}}""", Map("Retry-After" -> "120"))
    withReplies(Seq(reply)) { server =>
      val response = result(stage(server).setOperationMode("poll")
        .setOperationLocation(server.endpoint + locationPath).setMaxOperationDuration(1000))
      assert(response.status == "Unknown")
      assert(response.error.exists(_.contains("ResponseTooLarge")))
      assert(!response.error.exists(_.contains("OperationTimedOut")))
      assert(response.operationLocation.contains(server.endpoint + locationPath))
      assert(server.requests.map(_.method) == Seq("GET"))
    }
  }

  private def assertDatasetResolver(caseSensitive: Boolean): Unit = {
    val inputSession = spark.newSession()
    inputSession.conf.set("spark.sql.caseSensitive", caseSensitive.toString)
    val frame = inputSession.createDataFrame(Seq(Row("doc", "keep-input", "keep-output")).asJava,
      new StructType().add("analysis", StringType).add("PARSEDINPUT", StringType)
        .add("UNPARSEDOUTPUT", StringType)).coalesce(1)
    val activeSession = spark.newSession()
    activeSession.conf.set("spark.sql.caseSensitive", (!caseSensitive).toString)
    val previousSession = org.apache.spark.sql.SparkSession.getActiveSession
    org.apache.spark.sql.SparkSession.setActiveSession(activeSession)
    try {
      withReplies(Seq(ContentUnderstandingStubReply(HttpStatus.SC_OK, succeeded))) { server =>
        val outputCol = if (caseSensitive) "ANALYSIS" else "result"
        val transformed = stage(server).setOutputCol(outputCol).transform(frame)
        assert(activeSession.conf.get("spark.sql.caseSensitive") == (!caseSensitive).toString)
        val row = transformed.head()
        assert(row.getAs[String]("analysis") == "doc")
        assert(row.getAs[String]("PARSEDINPUT") == "keep-input")
        assert(row.getAs[String]("UNPARSEDOUTPUT") == "keep-output")
        val response = row.getAs[Row](outputCol)
        assert(response.getAs[String]("status") == "Succeeded")
        assert(server.requests.size == 1)
      }
    } finally {
      previousSession match {
        case Some(session) => org.apache.spark.sql.SparkSession.setActiveSession(session)
        case None => org.apache.spark.sql.SparkSession.clearActiveSession()
      }
    }
  }

  test("transform uses the input dataset session's column resolver") {
    Seq(true, false).foreach(assertDatasetResolver)
  }

  test("malformed polling responses do not turn a known Running operation into a terminal failure") {
    withReplies(Seq(accepted, ContentUnderstandingStubReply(HttpStatus.SC_OK, "not-json"))) { server =>
      val response = result(stage(server).setMaxPollAttempts(1))
      assert(response.status == "Running")
      assert(response.error.exists(_.contains("InvalidResponse")))
      assert(response.operationLocation.contains(server.endpoint + locationPath))
      assert(server.requests.map(_.method) == Seq("POST", "GET"))
      assert(!server.requests.last.headers.contains("content-type"))
    }
  }

  test("non-transient polling HTTP errors retain a known handle without reporting operation failure") {
    withReplies(Seq(accepted, ContentUnderstandingStubReply(HttpStatus.SC_BAD_REQUEST,
      """{"error":{"code":"InvalidRequest"}}"""))) { server =>
      val response = result(stage(server).setMaxPollAttempts(1))
      assert(response.status == "Running")
      assert(response.httpStatus == HttpStatus.SC_BAD_REQUEST)
      assert(response.error.exists(_.contains("InvalidRequest")))
      assert(response.operationLocation.contains(server.endpoint + locationPath))
      assert(server.requests.map(_.method) == Seq("POST", "GET"))
    }

    withReplies(Seq(ContentUnderstandingStubReply(HttpStatus.SC_FORBIDDEN,
      """{"error":{"code":"Unauthorized"}}"""))) { server =>
      val frame = spark.createDataFrame(Seq(Row(server.endpoint + locationPath, "bad-key")).asJava,
        new StructType().add("handle", StringType).add("key", StringType)).coalesce(1)
      val row = stage(server).setOperationMode("poll").setOperationLocationCol("handle")
        .setSubscriptionKeyCol("key").transform(frame).head()
      val response = ContentUnderstandingResponse.makeFromRowConverter(row.getAs[Row]("result"))
      assert(response.status == "Unknown")
      assert(response.httpStatus == HttpStatus.SC_FORBIDDEN)
      assert(response.error.exists(_.contains("Unauthorized")))
      assert(response.operationLocation.contains(server.endpoint + locationPath))
      assert(server.requests.map(_.method) == Seq("GET"))
    }

    Seq(HttpStatus.SC_NOT_IMPLEMENTED, HttpStatus.SC_HTTP_VERSION_NOT_SUPPORTED).foreach { code =>
      val reply = ContentUnderstandingStubReply(code, """{"error":{"code":"ServerError"}}""")
      withReplies(Seq(accepted, reply)) { server =>
        val response = result(stage(server))
        assert(response.status == "Running")
        assert(response.httpStatus == code)
        assert(response.error.exists(_.contains("ServerError")))
        assert(server.requests.map(_.method) == Seq("POST", "GET"))
      }
      withReplies(Seq(reply)) { server =>
        val response = result(stage(server).setOperationMode("poll")
          .setOperationLocation(server.endpoint + locationPath))
        assert(response.status == "Unknown")
        assert(response.operationLocation.contains(server.endpoint + locationPath))
        assert(server.requests.map(_.method) == Seq("GET"))
      }
    }
  }

  test("poll-mode analyzer failures remain row results even when analyzer settings are scalar") {
    val missing = """{"status":"Failed","error":{"code":"ResourceError","innererror":""" +
      """{"code":"DeploymentNotFound"}}}"""
    withReplies(Seq(ContentUnderstandingStubReply(HttpStatus.SC_OK, missing),
      ContentUnderstandingStubReply(HttpStatus.SC_OK, succeeded))) { server =>
      val handles = spark.createDataFrame(Seq(Row(server.endpoint + locationPath),
        Row(server.endpoint + locationPath)).asJava, new StructType().add("handle", StringType)).coalesce(1)
      val rows = stage(server).setOperationMode("poll").setOperationLocationCol("handle")
        .transform(handles).collect()
      assert(rows.map(_.getAs[Row]("result").getAs[String]("status")).toSeq == Seq("Failed", "Succeeded"))
      assert(server.requests.map(_.method) == Seq("GET", "GET"))
    }
  }

  test("provisioning does not return an analyzer that is not ready") {
    val operation = AnalyzersPath + "/custom/operations/create-1?api-version=" + DefaultApiVersion
    val creating = ContentUnderstandingStubReply(HttpStatus.SC_CREATED,
      """{"analyzerId":"custom","status":"creating"}""",
      Map("Operation-Location" -> ("$ROOT" + operation), "Retry-After" -> "0"))
    val completed = """{"id":"create-1","status":"Succeeded","result":{}}"""
    val notReady = """{"analyzerId":"custom","status":"creating"}"""
    withReplies(Seq(creating, ContentUnderstandingStubReply(HttpStatus.SC_OK, completed),
      ContentUnderstandingStubReply(HttpStatus.SC_OK, notReady))) { server =>
      val error = intercept[ContentUnderstandingException] {
        stage(server).setAnalyzerId("custom").createAnalyzer("{}", allowReplace = false)
      }
      assert(error.response.error.exists(_.contains("AnalyzerNotReady")))
      assert(server.requests.map(_.method) == Seq("PUT", "GET", "GET"))
    }
  }

  test("shared deployment errors stop submissions but document access errors remain row failures") {
    val missing = """{"status":"Failed","error":{"code":"ResourceError","innererror":""" +
      """{"code":"DeploymentNotFound"}}}"""
    withReplies(Seq(ContentUnderstandingStubReply(HttpStatus.SC_OK, missing))) { server =>
      assert(result(stage(server)).error.exists(_.contains("SharedConfigurationFailure")))
    }
    val denied = """{"status":"Failed","error":{"code":"DocumentAccessDenied","message":"HTTP 403"}}"""
    withReplies(Seq(ContentUnderstandingStubReply(HttpStatus.SC_OK, denied))) { server =>
      val response = result(stage(server))
      assert(response.status == "Failed")
      assert(response.error.exists(_.contains("DocumentAccessDenied")))
    }
  }

  test("per-row API versions keep analyzer and deployment errors row-specific") {
    val analyzerMissing =
      """{"status":"Failed","error":{"code":"AnalyzerNotFound","message":"Unavailable in this API version."}}"""
    val deploymentMissing =
      """{"status":"Failed","error":{"code":"DeploymentNotFound","message":"Unavailable in this API version."}}"""
    val server = new ContentUnderstandingStub(request => {
      if (request.query.contains("api-version=2025-10-01")) {
        ContentUnderstandingStubReply(HttpStatus.SC_OK, analyzerMissing)
      } else if (request.query.contains("api-version=2025-11-02")) {
        ContentUnderstandingStubReply(HttpStatus.SC_OK, deploymentMissing)
      } else {
        ContentUnderstandingStubReply(HttpStatus.SC_OK, succeeded)
      }
    })
    try {
      val versions = Seq("2025-10-01", "2025-11-02", DefaultApiVersion)
      val frame = spark.createDataFrame(versions.map(Row(_)).asJava,
        new StructType().add("version", StringType)).coalesce(1)
      val rows = stage(server).setApiVersionCol("version")
        .setModelDeployments(Map("prebuilt-read" -> "deployment"))
        .transform(frame).collect()
      val responsesByVersion = rows.map { row =>
        row.getAs[String]("version") ->
          ContentUnderstandingResponse.makeFromRowConverter(row.getAs[Row]("result")).status
      }.toMap
      assert(responsesByVersion == Map(
        "2025-10-01" -> "Failed",
        "2025-11-02" -> "Failed",
        DefaultApiVersion -> "Succeeded"))
      assert(server.requests.map(_.query).toSet == versions.map("api-version=" + _).toSet)
    } finally {
      server.close()
    }
  }

  test("long Retry-After delays preserve state without polling early") {
    Seq("120", "9999999999999999999").foreach { delay =>
      val submission = accepted.copy(headers = accepted.headers + ("Retry-After" -> delay))
      withReplies(Seq(submission)) { server =>
        val response = result(stage(server).setMaxOperationDuration(1000))
        assert(response.status == "Running")
        assert(response.error.exists(_.contains("OperationTimedOut")))
        assert(response.operationLocation.contains(server.endpoint + locationPath))
        assert(server.requests.map(_.method) == Seq("POST"))
      }
    }
  }

  test("an operation deadline aborts an in-flight response and keeps the accepted handle") {
    val server = new ContentUnderstandingStub(request => {
      if (request.method == "POST") accepted else {
        Thread.sleep(2000)
        ContentUnderstandingStubReply(HttpStatus.SC_OK, succeeded)
      }
    })
    try {
      val response = result(stage(server).setMaxOperationDuration(500).setTimeout(10))
      assert(response.status == "Running")
      assert(response.error.exists(_.contains("OperationTimedOut")))
      assert(response.operationLocation.contains(server.endpoint + locationPath))
      assert(server.requests.map(_.method) == Seq("POST", "GET"))
    } finally {
      server.close()
    }
  }

  test("binary input bounds reject oversized rows before encoding or submission") {
    withReplies(Seq(ContentUnderstandingStubReply(HttpStatus.SC_OK, succeeded))) { server =>
      val frame = spark.createDataFrame(Seq(Row(Array[Byte](1)), Row(Array[Byte](1, 2))).asJava,
        new StructType().add("bytes", BinaryType)).coalesce(1)
      val transformer = stage(server).setDocumentBytesCol("bytes").setMaxDocumentBytes(1)
      val rows = transformer.transform(frame).collect()
      val responses = rows.map(row => ContentUnderstandingResponse.makeFromRowConverter(row.getAs[Row]("result")))
      assert(responses.count(_.status == "Succeeded") == 1)
      assert(responses.exists(_.error.exists(_.contains("maxDocumentBytes"))))
      assert(server.requests.size == 1)
      intercept[IllegalArgumentException] {
        transformer.setDocumentBytes(Array[Byte](1, 2)).transformSchema(frame.schema)
      }
    }
  }

  test("shared authentication failures use generic diagnostics and preserve raw service errors") {
    val canary = "synthetic-sensitive-error"
    Seq(HttpStatus.SC_UNAUTHORIZED, HttpStatus.SC_FORBIDDEN).foreach { code =>
      withReplies(Seq(ContentUnderstandingStubReply(code,
        s"""{"error":{"code":"Unauthorized","message":"$canary"}}"""))) { server =>
        val response = result(stage(server).setSubscriptionKey("synthetic-key"))
        assert(response.error.exists(_.contains("SharedConfigurationFailure")))
        assert(!response.error.exists(_.contains(canary)))
        assert(response.rawResponse.contains(canary))
      }
    }
  }

  test("telemetry columns do not make shared authentication failures row-specific") {
    withReplies(Seq(ContentUnderstandingStubReply(HttpStatus.SC_UNAUTHORIZED,
      """{"error":{"code":"Unauthorized"}}"""))) { server =>
      val frame = spark.createDataFrame(
        Seq(Row(Map("x-client" -> "synthetic"))).asJava,
        new StructType().add("telemetry", MapType(StringType, StringType))).coalesce(1)
      val transformer = stage(server).setSubscriptionKey("synthetic-key")
        .setVectorParam("telemHeaders", "telemetry")
      val response = transformer.transform(frame).head().getAs[Row]("result")
      assert(response.getAs[String]("error").contains("SharedConfigurationFailure"))
      assert(server.requests.size == 1)
      assert(server.requests.head.headers("x-client") == "synthetic")
    }
  }

  test("non-auth custom header columns do not hide shared authentication failures") {
    withReplies(Seq(ContentUnderstandingStubReply(HttpStatus.SC_UNAUTHORIZED,
      """{"error":{"code":"Unauthorized"}}"""))) { server =>
      val frame = spark.createDataFrame(Seq(Row(Map("x-correlation-id" -> "synthetic"))).asJava,
        new StructType().add("headers", MapType(StringType, StringType))).coalesce(1)
      val transformer = stage(server).setSubscriptionKey("synthetic-key")
        .setVectorParam("customHeaders", "headers")
      val response = transformer.transform(frame).head().getAs[Row]("result")
      assert(response.getAs[String]("error").contains("SharedConfigurationFailure"))
      assert(server.requests.size == 1)
    }
  }

  test("authentication values in custom header columns remain row-specific") {
    val server = new ContentUnderstandingStub(request => {
      if (request.headers.get("ocp-apim-subscription-key").contains("bad-key")) {
        ContentUnderstandingStubReply(HttpStatus.SC_UNAUTHORIZED, """{"error":{"code":"Unauthorized"}}""")
      } else if (!request.headers.contains("ocp-apim-subscription-key")) {
        ContentUnderstandingStubReply(HttpStatus.SC_UNAUTHORIZED, """{"error":{"code":"Unauthorized"}}""")
      } else {
        ContentUnderstandingStubReply(HttpStatus.SC_OK, succeeded)
      }
    })
    try {
      val headers = Seq(Map("Ocp-Apim-Subscription-Key" -> "bad-key"),
        Map.empty[String, String], Map("Ocp-Apim-Subscription-Key" -> "good-key"))
      val frame = spark.createDataFrame(headers.map(Row(_)).asJava,
        new StructType().add("headers", MapType(StringType, StringType))).coalesce(1)
      val rows = stage(server).setVectorParam("customHeaders", "headers").transform(frame).collect()
      assert(rows.map(_.getAs[Row]("result").getAs[String]("status")).toSeq ==
        Seq("Rejected", "Rejected", "Succeeded"))
      assert(server.requests.size == 3)
    } finally {
      server.close()
    }
  }

  test("unused credential columns do not hide rejection of the scalar credential sent") {
    withReplies(Seq(ContentUnderstandingStubReply(HttpStatus.SC_UNAUTHORIZED,
      """{"error":{"code":"Unauthorized"}}"""))) { server =>
      val frame = spark.createDataFrame(Seq(Row("unused-row-token")).asJava,
        new StructType().add("token", StringType)).coalesce(1)
      val transformer = stage(server).setSubscriptionKey("synthetic-key").setAADTokenCol("token")
      val response = transformer.transform(frame).head().getAs[Row]("result")
      assert(response.getAs[String]("error").contains("SharedConfigurationFailure"))
      assert(server.requests.size == 1)
      assert(server.requests.head.headers("ocp-apim-subscription-key") == "synthetic-key")
      assert(!server.requests.head.headers.contains("authorization"))
    }
  }

  test("a row credential equal to a lower-priority scalar credential remains row-specific") {
    val unauthorized = ContentUnderstandingStubReply(HttpStatus.SC_UNAUTHORIZED,
      """{"error":{"code":"Unauthorized"}}""")
    withReplies(Seq(unauthorized)) { server =>
      val frame = spark.createDataFrame(Seq(Row("synthetic-token")).asJava,
        new StructType().add("token", StringType)).coalesce(1)
      val response = ContentUnderstandingResponse.makeFromRowConverter(
        stage(server).setAADTokenCol("token").setCustomAuthHeader("Bearer synthetic-token")
          .transform(frame).head().getAs[Row]("result"))
      assert(response.status == "Rejected")
      assert(server.requests.size == 1)
    }
  }

  test("default scalar credentials are classified as shared authentication") {
    val unauthorized = ContentUnderstandingStubReply(HttpStatus.SC_UNAUTHORIZED,
      """{"error":{"code":"Unauthorized"}}""")
    withReplies(Seq(unauthorized)) { server =>
      val schema = new StructType().add("id", StringType)
        .add("headers", MapType(StringType, StringType))
      val headers = Map("x-request" -> "value").asJava
      val frame = spark.createDataFrame(Seq(Row("doc", headers)).asJava, schema).coalesce(1)
      val transformer = new TestableDefaultAADTokenStage().setEndpoint(server.endpoint).setOutputCol("result")
        .setErrorCol("error").setDocumentBytes(Array[Byte](1)).setCustomHeadersCol("headers")
        .setPollingDelay(0).setMaxPollAttempts(2)
      val response = transformer.transform(frame).head().getAs[Row]("result")
      assert(response.getAs[String]("error").contains("SharedConfigurationFailure"))
      assert(server.requests.size == 1)
    }
  }

  test("column authentication failures remain row errors and valid rows continue") {
    val server = new ContentUnderstandingStub(request =>
      if (request.headers.get("ocp-apim-subscription-key").contains("bad-key")) {
        ContentUnderstandingStubReply(HttpStatus.SC_UNAUTHORIZED, """{"error":{"code":"Unauthorized"}}""")
      } else {
        ContentUnderstandingStubReply(HttpStatus.SC_OK, succeeded)
      })
    try {
      val frame = spark.createDataFrame(Seq(Row("bad-key"), Row("good-key")).asJava,
        new StructType().add("key", StringType)).coalesce(1)
      val rows = stage(server).setSubscriptionKeyCol("key").transform(frame).collect()
      assert(rows.map(_.getAs[Row]("result").getAs[String]("status")).toSeq == Seq("Rejected", "Succeeded"))
      assert(server.requests.size == 2)
    } finally {
      server.close()
    }
  }

  test("header injection in column credentials is a row error that does not echo the value") {
    withReplies(Seq(ContentUnderstandingStubReply(HttpStatus.SC_OK, succeeded))) { server =>
      val frame = spark.createDataFrame(Seq(
        Row("bad\r\nInjected: value"), Row(unicodeHeaderInjection), Row("good-key")).asJava,
        new StructType().add("key", StringType)).coalesce(1)
      val responses = stage(server).setSubscriptionKeyCol("key").transform(frame).collect()
        .map(row => ContentUnderstandingResponse.makeFromRowConverter(row.getAs[Row]("result")))
      assert(responses.map(_.status).toSeq == Seq("Failed", "Failed", "Succeeded"))
      assert(responses.take(2).forall(_.error.exists(error =>
        error.contains("InvalidInput") && !error.contains("Injected"))))
      assert(server.requests.size == 1)
      assert(!server.requests.head.headers.contains("injected"))
    }
  }

  test("Unicode header injection is rejected in scalar credentials and custom header columns") {
    val injected = unicodeHeaderInjection
    withReplies(Seq(ContentUnderstandingStubReply(HttpStatus.SC_OK, succeeded))) { server =>
      val failure = intercept[IllegalArgumentException] {
        stage(server).setSubscriptionKey(injected).transform(input)
      }
      assert(!failure.getMessage.contains("Injected"))
      val frame = spark.createDataFrame(Seq(
        Row(Map("x-client" -> injected)), Row(Map("x-client" -> "valid"))).asJava,
        new StructType().add("headers", MapType(StringType, StringType))).coalesce(1)
      val rows = stage(server).setCustomHeadersCol("headers").transform(frame).collect()
      assert(rows.map(_.getAs[Row]("result").getAs[String]("status")).toSeq == Seq("Failed", "Succeeded"))
      assert(server.requests.size == 1)
      assert(!server.requests.head.headers.contains("injected"))
    }
  }

  test("shared analyzer errors stop submissions while column-specific analyzer errors remain row-specific") {
    withReplies(Seq(ContentUnderstandingStubReply(HttpStatus.SC_NOT_FOUND,
      """{"error":{"code":"NotFound"}}"""))) { server =>
      assert(result(stage(server).setAnalyzerId("missing")).error.exists(_.contains("SharedConfigurationFailure")))
      val frame = spark.createDataFrame(Seq(Row("missing")).asJava,
        new StructType().add("analyzer", StringType)).coalesce(1)
      val row = stage(server).setAnalyzerIdCol("analyzer").transform(frame).head()
      assert(row.getAs[Row]("result").getAs[String]("status") == "Failed")
    }
  }

  test("shared error codes nested in details are recognized") {
    withReplies(Seq(ContentUnderstandingStubReply(HttpStatus.SC_OK,
      """{"status":"Failed","error":{"code":"ResourceError","details":[{"code":"AnalyzerNotFound"}]}}"""))) {
      server =>
        assert(result(stage(server).setAnalyzerId("missing")).error.exists(_.contains("SharedConfigurationFailure")))
    }
  }

  test("shared failures retain accepted sibling handles and skip remaining submissions") {
    Seq(HttpStatus.SC_UNAUTHORIZED, HttpStatus.SC_NOT_FOUND).foreach { code =>
      withReplies(Seq(accepted, ContentUnderstandingStubReply(code,
        """{"error":{"code":"NotFound"}}"""))) { server =>
        val frame = spark.createDataFrame(Seq(Row("one"), Row("two"), Row("three")).asJava,
          new StructType().add("id", StringType)).coalesce(1)
        val output = stage(server).setSubscriptionKey("synthetic-key").setOperationMode("submit")
          .setConcurrency(1).transform(frame)
        val materialized = output.collect()
        val rows = materialized.map(_.getAs[Row]("result"))
        assert(rows.head.getAs[String]("operationLocation") == server.endpoint + locationPath)
        assert(rows(1).getAs[String]("error").contains("SharedConfigurationFailure"))
        assert(rows(1).getAs[String]("status") == (if (code == HttpStatus.SC_UNAUTHORIZED) "Rejected" else "Failed"))
        assert(rows(2).getAs[String]("status") == "Rejected")
        assert(rows(2).getAs[Int]("httpStatus") == 0)
        val saved = spark.createDataFrame(materialized.toSeq.asJava, output.schema)
        val retryable = saved.where("""result.operationLocation IS NULL AND (result.status = 'Rejected' OR (
          result.status = 'Failed' AND get_json_object(result.error, '$.code') = 'SharedConfigurationFailure'))""")
        assert(retryable.count() == 2)
        assert(server.requests.map(_.method) == Seq("POST", "POST"))
      }
    }
  }

  test("management retries honor the same operation budget") {
    withReplies(Seq(ContentUnderstandingStubReply(HttpStatus.SC_SERVICE_UNAVAILABLE,
      """{"error":{"code":"Unavailable"}}""", Map("Retry-After" -> "120")))) { server =>
      val failure = intercept[ContentUnderstandingException] {
        stage(server).setMaxOperationDuration(1000).getAnalyzer()
      }
      assert(failure.response.error.exists(_.contains("Unavailable")))
      assert(server.requests.map(_.method) == Seq("GET"))
    }
  }

  test("content type custom headers are rejected case-insensitively and connector JSON wins") {
    Seq("Content-Type", "content-type", "CONTENT-TYPE").foreach { headerName =>
      intercept[IllegalArgumentException] {
        ContentUnderstandingHeaderValidation.validateSetting("customHeaders", Map(headerName -> "text/plain"))
      }
    }

    withReplies(Seq(ContentUnderstandingStubReply(HttpStatus.SC_OK, succeeded))) { server =>
      val error = intercept[Exception] {
        result(stage(server).setCustomHeaders(Map("content-type" -> "text/plain")))
      }
      assert(exceptionContains(error, "connector-managed header name"))
      assert(server.requests.isEmpty)
    }

    val headers = ServiceAuthHeaders.build(None, "api-key", "Authorization", None, None,
      Some(Map("content-type" -> "text/plain", "x-correlation-id" -> "synthetic")),
      None, None, Some("application/json"))
    assert(headers.keys.count(_.equalsIgnoreCase("Content-Type")) == 1)
    assert(headers("Content-Type") == "application/json")
    assert(headers("x-correlation-id") == "synthetic")
  }

  private def assertStalledDns(mode: String): Unit = {
    val loopback = InetAddress.getByAddress(Array[Byte](127, 0, 0, 1))
    val listener = new ServerSocket(0, 1, loopback)
    listener.setSoTimeout(2000)
    val connections = new AtomicInteger(0)
    val listenerFinished = new CountDownLatch(1)
    val listenerThread = new Thread(new Runnable {
      override def run(): Unit = {
        try {
          val connection = listener.accept()
          connections.incrementAndGet()
          connection.close()
        } catch {
          case _: SocketTimeoutException => ()
          case _: IOException => ()
        } finally {
          listenerFinished.countDown()
        }
      }
    })
    listenerThread.setDaemon(true)
    listenerThread.start()

    val host = "dns-blocked.test"
    val endpoint = s"https://$host:${listener.getLocalPort}$AnalyzersPath"
    val location = s"https://$host:${listener.getLocalPort}$locationPath"
    val settings = ContentUnderstandingProtocol.Settings(
      endpoint, 1000, 1, 0, 1024, 500, sharedAnalyzer = false, sharedModels = false)
    val request = new HTTPRequestData(if (mode == "poll") new HttpGet(location) else new HttpPost(endpoint))
    try {
      assertDnsTimeoutResult(settings, request, mode, loopback, location)
    } finally {
      listenerThread.join(2500)
      listener.close()
      listenerThread.join(1000)
    }
    assert(listenerFinished.getCount == 0)
    assert(!listenerThread.isAlive)
    assert(connections.get() == 0)
  }

  private def assertDnsTimeoutResult(settings: ContentUnderstandingProtocol.Settings,
                                     request: HTTPRequestData,
                                     mode: String, loopback: InetAddress, location: String): Unit = {
    val dnsStarted = new CountDownLatch(1)
    val dnsRelease = new CountDownLatch(1)
    val dnsFinished = new CountDownLatch(1)
    val resolver = new DnsResolver {
      override def resolve(requestedHost: String): Array[InetAddress] = {
        dnsStarted.countDown()
        try {
          try dnsRelease.await() catch { case _: InterruptedException => dnsRelease.await() }
          Array(loopback)
        } finally {
          dnsFinished.countDown()
        }
      }
    }
    val response = new AtomicReference[ContentUnderstandingResponse]()
    val failure = new AtomicReference[Exception]()
    val worker = new Thread(() => {
      try {
        response.set(ContentUnderstandingProtocol.execute(settings, request, mode, resolver))
      } catch {
        case error: Exception => failure.set(error)
      }
    })
    worker.setDaemon(true)
    worker.start()

    try {
      assert(dnsStarted.await(2, TimeUnit.SECONDS))
      worker.join(2000)
      assert(!worker.isAlive, "The operation waited beyond its deadline for DNS.")
      if (failure.get() != null) {
        throw failure.get()
      }
      val result = Option(response.get()).getOrElse(fail("The operation returned no response."))
      assert(result.status == (if (mode == "poll") "Unknown" else "Rejected"))
      assert(result.error.exists(_.contains(if (mode == "poll") "OperationTimedOut" else "RequestNotSent")))
      assert(result.operationLocation == (if (mode == "poll") Some(location) else None))
    } finally {
      dnsRelease.countDown()
      worker.join(1000)
      assert(dnsFinished.await(1, TimeUnit.SECONDS))
    }
    assert(!worker.isAlive)
  }

  Seq("submit", "analyze", "poll").foreach { mode =>
    test(s"operation deadline bounds stalled DNS in $mode mode without dispatch") {
      assertStalledDns(mode)
    }
  }

  test("DNS failures are reported as safe pre-send failures") {
    val endpoint = "https://dns-failed.test" + AnalyzersPath
    val request = new HTTPRequestData(new HttpPost(endpoint))
    val resolver = new DnsResolver {
      override def resolve(host: String): Array[InetAddress] =
        throw new UnknownHostException("Synthetic DNS failure")
    }
    val settings = ContentUnderstandingProtocol.Settings(
      endpoint, 1000, 1, 0, 1024, 500, sharedAnalyzer = false, sharedModels = false)

    val response = ContentUnderstandingProtocol.execute(settings, request, "submit", resolver)
    assert(response.status == "Rejected")
    assert(response.error.exists(_.contains("DnsResolutionFailed")))
    assert(response.error.exists(_.contains("before the HTTP request was sent")))
    assert(!response.error.exists(_.contains("safely retried")))
    assert(!response.error.exists(_.contains("may have been accepted")))
    assert(response.operationLocation.isEmpty)
  }

  private lazy val protocolClass = Class.forName(
    "com.microsoft.azure.synapse.ml.services.contentunderstanding.ContentUnderstandingProtocol$")
  private lazy val protocol = protocolClass.getField("MODULE$").get(None.orNull)

  test("response stream cleanup cannot replace the size-limit failure") {
    val input = new ByteArrayInputStream(Array[Byte](1, 2, 3)) {
      override def close(): Unit = throw new IOException("Synthetic stream close failure")
    }
    val read = protocolClass.getDeclaredMethod("readBounded", classOf[InputStream],
      java.lang.Integer.TYPE, classOf[HttpRequestBase])
    read.setAccessible(true)
    val request = new HttpGet("http://127.0.0.1/")
    try {
      val failure = intercept[InvocationTargetException] {
        read.invoke(protocol, input, Int.box(2), request)
      }
      assert(failure.getCause.getClass.getSimpleName == "ResponseTooLarge")
      assert(request.isAborted)
    } finally {
      intercept[IOException](input.close())
    }
  }

  test("response close failures cannot replace a completed read or the size-limit diagnostic") {
    ContentUnderstandingFixtures.withLogs { logs =>
      class CloseFailingClient(body: String) extends CloseableHttpClient {
        override def close(): Unit = throw new IOException("Synthetic client close failure")
        override def getConnectionManager: ClientConnectionManager = throw new UnsupportedOperationException
        override def getParams: HttpParams = throw new UnsupportedOperationException
        override protected def doExecute(target: HttpHost,
                                         request: HttpRequest,
                                         context: HttpContext): CloseableHttpResponse = {
          val response = new BasicHttpResponse(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "OK")
            with CloseableHttpResponse {
            override def close(): Unit = throw new IOException("Synthetic response close failure")
          }
          response.setEntity(new StringEntity(body, StandardCharsets.UTF_8))
          response
        }
      }
      val settings = ContentUnderstandingProtocol.Settings("https://example.invalid" + AnalyzersPath, 1000, 1, 0,
        16, 10000, sharedAnalyzer = false, sharedModels = false)
      val request = new HTTPRequestData(new HttpGet("https://example.invalid" + locationPath))
      val closeSafeResult = ContentUnderstandingProtocol.ClientLifecycle
        .withClient(new CloseFailingClient(""))("completed")
      assert(closeSafeResult == "completed")
      def wire(body: String): Seq[Any] = {
        val result = ContentUnderstandingProtocol.exchange(new CloseFailingClient(body), settings, request,
          new ContentUnderstandingBudget(10000), analysis = true, context = new ContentUnderstandingThrottling.Context)
        Seq(result.httpStatus, result.body, result.diagnostic)
      }
      assert(wire("""{"ok":1}""") == Seq(HttpStatus.SC_OK, """{"ok":1}""", None))
      val limited = wire("x" * 64)
      assert(limited.take(2) == Seq(HttpStatus.SC_OK, ""))
      assert(limited(2).asInstanceOf[Option[String]].exists(_.contains("ResponseTooLarge")))
      assert(logs().size == 3)
      assert(logs().forall(event => event.getThrown == null &&
        event.getMessage.getFormattedMessage.contains("IOException") &&
        !event.getMessage.getFormattedMessage.contains("Synthetic")))
      assert(logs().count(_.getMessage.getFormattedMessage.contains("HTTP response")) == 2)
      assert(logs().count(_.getMessage.getFormattedMessage.contains("HTTP client")) == 1)
    }
  }
}
