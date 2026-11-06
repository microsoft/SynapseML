// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.contentunderstanding

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.http.{HeaderData, HTTPRequestData}
import org.apache.http.client.methods.HttpGet
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{MapType, StringType, StructType}

import java.time.{ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

private[contentunderstanding] object ContentUnderstandingSubmissionSuite {
  @volatile var RejectionObserved: CountDownLatch = new CountDownLatch(0)
}

class ContentUnderstandingSubmissionSuite extends TestBase {
  import ContentUnderstandingFixtures.{Accepted, LocationPath, Running, Succeeded}
  import ContentUnderstandingStub.withReplies

  private def stage(server: ContentUnderstandingStub): ContentUnderstanding =
    new ContentUnderstanding().setEndpoint(server.endpoint).setOutputCol("result").setErrorCol("error")
      .setDocumentBytes(Array[Byte](1)).setPollingDelay(0).setMaxPollAttempts(2).setConcurrency(1)

  private def responses(stage: ContentUnderstanding, input: Dataset[_]): Seq[ContentUnderstandingResponse] =
    stage.transform(input).collect().map(row =>
      ContentUnderstandingResponse.makeFromRowConverter(row.getAs[Row]("result"))).toSeq

  private def assertNoProvenanceHeader(server: ContentUnderstandingStub): Unit =
    assert(server.requests.forall(request => !request.headers.contains("x-synapseml-implicit-fabric-auth")))

  test("management logs safe outcomes and timing without changing results or exceptions") {
    val ready = """{"analyzerId":"custom","status":"ready","description":"private-service-value"}"""
    for (create <- Seq(false, true); fail <- Seq(false, true)) {
      val events = ArrayBuffer.empty[(Map[String, String], Option[String])]
      withReplies(Seq(ContentUnderstandingStubReply(if (fail) 400 else 200,
        if (fail) """{"error":{"message":"private-service-value"}}""" else ready))) { server =>
        val transformer = new ContentUnderstanding() {
          override protected def logBase(info: Map[String, String], featureName: Option[String]): Unit =
            events += ((info, featureName))
        }.setEndpoint(server.endpoint).setAnalyzerId("custom")
        def invoke(): String = if (create) transformer.createAnalyzer("{}", allowReplace = false)
          else transformer.getAnalyzer()
        if (fail) {
          val error = intercept[ContentUnderstandingException](invoke())
          assert(error.response.rawResponse.contains("private-service-value"))
        } else {
          assert(invoke() == ready)
        }
        val verb = if (create) "createAnalyzer" else "getAnalyzer"
        val logged = events.filter(_._1.get("method").contains(verb))
        assert(logged.size == 1)
        val (payload, feature) = logged.head
        assert(feature.isEmpty)
        assert(payload("executionSeconds").toDouble >= 0)
        assert(payload("outcome") == (if (fail) "failed" else "succeeded"))
        assert(payload.get("errorType") ==
          (if (fail) Some(classOf[ContentUnderstandingException].getName) else None))
        assert(!payload.values.exists(_.contains("private-service-value")))
        assert(!payload.contains("errorMessage"))
      }
    }
  }

  test("management logging covers validation failure and cannot mask the original outcome") {
    val events = ArrayBuffer.empty[Map[String, String]]
    withReplies(Seq(ContentUnderstandingStubReply(200, """{"status":"ready"}"""))) { server =>
      val transformer = new ContentUnderstanding() {
        override protected def logBase(info: Map[String, String], featureName: Option[String]): Unit = {
          if (info.get("method").exists(Set("createAnalyzer", "getAnalyzer"))) {
            events += info
            throw new IllegalStateException("synthetic log failure")
          }
        }
      }.setEndpoint(server.endpoint).setAnalyzerId("custom")
      intercept[IllegalArgumentException] {
        transformer.createAnalyzer("private-invalid-definition", allowReplace = false)
      }
      assert(events.size == 1)
      assert(events.head("outcome") == "failed")
      assert(!events.head.values.exists(_.contains("private-invalid-definition")))
      assert(server.requests.isEmpty)
      assert(transformer.getAnalyzer() == """{"status":"ready"}""")
      assert(events.size == 2)
      assert(events.last("outcome") == "succeeded")
    }
  }

  test("interrupted management logs once and preserves interruption even when logging is interrupted") {
    // Capturing constructor logs bypasses backend initialization; do it before injecting interruption.
    org.apache.logging.log4j.LogManager.getContext(false)
    for (create <- Seq(false, true); interruptLogger <- Seq(false, true)) {
      val events = ArrayBuffer.empty[Map[String, String]]
      withReplies(Seq(ContentUnderstandingStubReply(200, """{"status":"ready"}"""))) { server =>
        val transformer = new ContentUnderstanding() {
          override protected def logBase(info: Map[String, String], featureName: Option[String]): Unit = {
            if (info.get("method").exists(Set("createAnalyzer", "getAnalyzer"))) {
              events += info
              if (interruptLogger) throw new InterruptedException("synthetic logging interruption")
            }
          }
        }.setEndpoint(server.endpoint).setAnalyzerId("custom")
        Thread.currentThread().interrupt()
        try {
          val error = intercept[InterruptedException] {
            if (create) transformer.createAnalyzer("{}", allowReplace = false) else transformer.getAnalyzer()
          }
          assert(error.getMessage != "synthetic logging interruption")
          assert(Thread.currentThread().isInterrupted)
          assert(events.size == 1)
          assert(events.head("outcome") == "failed")
          assert(events.head("errorType") == classOf[InterruptedException].getName)
          assert(events.head("executionSeconds").toDouble >= 0)
          assert(!events.head.contains("errorMessage"))
          assert(server.requests.isEmpty)
        } finally {
          Thread.interrupted()
        }
      }
    }
  }

  test("shared-stop warnings identify connector categories and available task context") {
    val cases = Seq(
      (401, """{"error":{"message":"private-service-value"}}""", "authentication"),
      (404, """{"error":{"code":"AnalyzerNotFound","message":"private-service-value"}}""", "analyzer"),
      (200, """{"status":"Failed","error":{"code":"DeploymentNotFound"}}""", "deployment"))
    for (concurrency <- Seq(1, 2); (status, body, category) <- cases) {
      ContentUnderstandingFixtures.withLogs { logs =>
        withReplies(Seq(ContentUnderstandingStubReply(status, body))) { server =>
          val output = responses(stage(server).setConcurrency(concurrency).setSubscriptionKey("synthetic-key")
            .setAnalyzerId("custom").setModelDeployments(Map("model" -> "deployment")),
            spark.range(2).coalesce(1))
          assert(output.forall(_.error.exists(_.contains("SharedConfigurationFailure"))))
          assert(logs().size == 1)
          val message = logs().head.getMessage.getFormattedMessage
          assert(message.contains(s"category=$category"))
          assert(message.contains(s"HTTP $status"))
          if (concurrency == 1) {
            assert(message.contains("partitionId=0") && message.contains("stageId="))
          } else {
            assert(message.contains("taskContext=unavailable") || message.contains("partitionId=0"))
          }
          assert(!message.contains("private-service-value") && !message.contains("synthetic-key"))
          assert(logs().head.getThrown == null)
        }
      }
    }
  }

  test("accepted unknown and non-auth outcomes skip credential fingerprint inspection") {
    val reads = new AtomicInteger()
    val fingerprints = new scala.collection.immutable.SetProxy[String] {
      override val self: Set[String] = Set.empty
      override def apply(value: String): Boolean = {
        reads.incrementAndGet()
        self(value)
      }
    }
    val request = new HTTPRequestData(new HttpGet("http://example.invalid/"))
      .copy(headers = Array(HeaderData("Ocp-Apim-Subscription-Key", "synthetic-key")))
    val settings = ContentUnderstandingProtocol.Settings("http://example.invalid/contentunderstanding/analyzers",
      1000, 1, 0, 1024, 10000, sharedAnalyzer = false, sharedModels = false,
      scalarAuthFingerprints = fingerprints, allAuthScalar = true)
    val protocol = ContentUnderstandingProtocol
    val check = protocol.getClass.getDeclaredMethod("checkSharedFailure",
      classOf[ContentUnderstandingProtocol.Settings], classOf[HTTPRequestData],
      classOf[ContentUnderstandingResponse], java.lang.Boolean.TYPE)
    check.setAccessible(true)
    Seq(("Failed", 401, true), ("Unknown", 401, false), ("Succeeded", 200, false)).foreach {
      case (status, http, accepted) =>
        check.invoke(protocol, settings, request, ContentUnderstandingResponse(None, None, status, http, "", None),
          Boolean.box(accepted))
        assert(reads.get() == 0)
    }
    assert(ContentUnderstandingProtocol.FailureClassifier.isSharedAuthFailure(settings, request,
      ContentUnderstandingResponse(None, None, "Rejected", 401, "", None)))
    assert(reads.get() > 0)
  }

  test("shared scalar rejection logs one sanitized stop while row rejection logs none") {
    Seq(false, true).foreach { rowAuth =>
      ContentUnderstandingFixtures.withLogs { logs =>
        withReplies(Seq(ContentUnderstandingStubReply(401,
          """{"error":{"message":"private-service-value"}}"""))) { server =>
          val transformer = stage(server)
          val input = spark.range(3).coalesce(1).withColumn("key", org.apache.spark.sql.functions.lit("synthetic-key"))
          if (rowAuth) transformer.setSubscriptionKeyCol("key") else transformer.setSubscriptionKey("synthetic-key")
          val output = responses(transformer, input)
          assert(output.forall(_.status == "Rejected"))
          assert(server.requests.size == (if (rowAuth) 3 else 1))
          assert(logs().size == (if (rowAuth) 0 else 1))
          logs().foreach { event =>
            val message = event.getMessage.getFormattedMessage
            assert(message.contains("SharedConfigurationFailure") && message.contains("401"))
            assert(!message.contains("private-service-value") && !message.contains("synthetic-key"))
            assert(event.getThrown == null)
          }
        }
      }
    }
  }

  test("management GET transport failures advise retrieval rather than operation polling") {
    Seq(false, true).foreach { create =>
      val operation = ContentUnderstandingFixtures.AnalyzersPath + "/custom/operations/create-1?api-version=" +
        ContentUnderstandingFixtures.DefaultApiVersion
      val creation = if (create) Seq(ContentUnderstandingStubReply(201, """{"status":"creating"}""",
        Map("Operation-Location" -> ("$ROOT" + operation), "Retry-After" -> "0")),
        ContentUnderstandingStubReply(200, """{"status":"Succeeded"}""")) else Seq.empty
      withReplies(creation :+ ContentUnderstandingStubReply(200, "", disconnect = true)) { server =>
        val transformer = stage(server).setAnalyzerId("custom")
        val error = intercept[ContentUnderstandingException] {
          if (create) transformer.createAnalyzer("{}", allowReplace = false) else transformer.getAnalyzer()
        }
        assert(error.response.error.exists(_.contains("HTTP management GET transport failed")))
        assert(error.response.error.exists(_.contains("getAnalyzer")))
        assert(!error.response.error.exists(_.contains("resume polling")))
        assert(error.response.operationLocation == (if (create) Some(server.endpoint + operation) else None))
        assert(server.requests.count(_.method == "PUT") == (if (create) 1 else 0))
        assert(server.requests.exists(_.method == "GET"))
      }
    }
    Seq(false, true).foreach { disconnected =>
      withReplies(Seq(ContentUnderstandingStubReply(200, Running, disconnect = disconnected))) { server =>
        val output = responses(stage(server).setOperationMode("poll").setMaxPollAttempts(1)
          .setOperationLocation(server.endpoint + LocationPath), spark.range(1)).head
        assert(output.error.exists(_.contains("resume polling")))
        assert(output.operationLocation.contains(server.endpoint + LocationPath))
      }
    }
  }

  test("creation poll recovery advice preserves management state without resubmission") {
    val location = ContentUnderstandingFixtures.AnalyzersPath + "/custom/operations/create-1?api-version=" +
      ContentUnderstandingFixtures.DefaultApiVersion
    val creating = """{"id":"create-1","status":"creating","details":"private-service-value"}"""
    val running = """{"id":"create-1","status":"Running","details":"private-service-value"}"""
    Seq(false, true).foreach { disconnected =>
      val replies = Seq(ContentUnderstandingStubReply(201, creating,
        Map("Operation-Location" -> ("$ROOT" + location), "Retry-After" -> "0")),
        ContentUnderstandingStubReply(200, running, disconnect = disconnected))
      withReplies(replies) { server =>
        val transformer = stage(server).setAnalyzerId("custom").setMaxPollAttempts(1)
        val error = intercept[ContentUnderstandingException](transformer.createAnalyzer("{}", allowReplace = false))
        val result = error.response
        val code = if (disconnected) "TransportError" else "PollAttemptsExhausted"
        assert(result.status == "Running")
        assert(result.httpStatus == (if (disconnected) 0 else 200))
        assert(result.rawResponse == (if (disconnected) creating else running))
        assert(result.id.contains("create-1"))
        assert(result.operationLocation.contains(server.endpoint + location))
        assert(result.error.exists(_.contains(code)))
        assert(result.error.exists(_.contains("getAnalyzer")))
        assert(!result.error.exists(_.contains("resume polling")))
        assert(error.getMessage.contains(code))
        assert(!error.getMessage.contains("private-service-value"))
        assert(!error.getMessage.contains("Check the configured"))
        assert(server.requests.map(_.method) == Seq("PUT", "GET"))
      }
    }
  }

  test("management exception messages expose only known diagnostic codes") {
    val privateError = """{"error":{"code":"private-service-code","message":"private-service-value"}}"""
    val largeError = privateError.replace("private-service-value", "private-service-value" * 500)
    Seq((200, "private-service-value", Some("InvalidResponse")),
      (400, privateError, None), (400, largeError, None)).foreach { case (status, body, code) =>
      withReplies(Seq(ContentUnderstandingStubReply(status, body))) { server =>
        val error = intercept[ContentUnderstandingException](stage(server).getAnalyzer())
        assert(error.response.rawResponse == body)
        code.foreach(value => assert(error.getMessage.contains(value)))
        assert(error.getMessage.contains("Inspect the exception response"))
        assert(!error.getMessage.contains("private-service"))
        assert(!error.getMessage.contains("Check the configured"))
      }
    }
  }

  test("concurrent shared rejections log only the first stop transition") {
    ContentUnderstandingFixtures.withLogs { logs =>
      val started = new CountDownLatch(2)
      val server = new ContentUnderstandingStub(_ => {
        started.countDown()
        assert(started.await(30, TimeUnit.SECONDS))
        ContentUnderstandingStubReply(401, """{"error":{"code":"Denied"}}""")
      })
      try {
        val output = responses(stage(server).setConcurrency(2).setSubscriptionKey("synthetic-key"),
          spark.range(3).coalesce(1))
        assert(output.forall(_.status == "Rejected"))
        assert(output.count(_.httpStatus == 401) == 2)
        assert(server.requests.size == 2)
        assert(logs().size == 1)
        assert(logs().head.getMessage.getFormattedMessage.contains("SharedConfigurationFailure"))
      } finally {
        started.countDown()
        server.close()
      }
    }
  }

  test("equal-valued column credentials do not stop subsequent rows") {
    Seq(401, 403).foreach { code =>
      withReplies(Seq(ContentUnderstandingStubReply(code, """{"error":{"code":"Denied"}}"""),
        ContentUnderstandingStubReply(200, Succeeded))) { server =>
        val tokens = Seq("column-token", "different-token")
        val input = spark.createDataFrame(tokens.map(Row(_)).asJava,
          new StructType().add("token", StringType)).coalesce(1)
        val output = responses(stage(server).setAADTokenCol("token")
          .setCustomAuthHeader(s"Bearer ${tokens.head}"), input)
        assert(output.map(_.status) == Seq("Rejected", "Succeeded"))
        assert(output.head.error.exists(_.contains("Denied")))
        assert(!output.head.error.exists(_.contains("SharedConfigurationFailure")))
        assert(server.requests.map(_.method) == Seq("POST", "POST"))
        assert(server.requests.map(_.headers("authorization")) == tokens.map("Bearer " + _))
        assertNoProvenanceHeader(server)
      }
    }
  }

  test("embedded credential columns retain row isolation and never transmit provenance") {
    for (header <- Seq("Authorization", "Ocp-Apim-Subscription-Key"); code <- Seq(401, 403)) {
      withReplies(Seq(ContentUnderstandingStubReply(code, """{"error":{"code":"Denied"}}"""),
        ContentUnderstandingStubReply(200, Succeeded))) { server =>
        val values = if (header == "Authorization") Seq("Bearer bad-token", "Bearer good-token")
          else Seq("bad-key", "good-key")
        val input = spark.createDataFrame(values.map(value => Row(Map(header -> value))).asJava,
          new StructType().add("headers", MapType(StringType, StringType))).coalesce(1)
        val output = responses(stage(server).setCustomHeadersCol("headers"), input)
        assert(output.map(_.status) == Seq("Rejected", "Succeeded"))
        assert(output.head.error.exists(_.contains("Denied")))
        assert(!output.head.error.exists(_.contains("SharedConfigurationFailure")))
        assert(server.requests.map(_.method) == Seq("POST", "POST"))
        assert(server.requests.map(_.headers(header.toLowerCase(java.util.Locale.ROOT))) == values)
        assertNoProvenanceHeader(server)
      }
    }
  }

  test("accepted terminal failures retain handles and do not stop later submissions") {
    for (httpStatus <- Seq(200, 201, 202); code <- Seq("DeploymentNotFound", "AnalyzerNotFound")) {
      val body = s"""{"id":"op-1","status":"Failed","error":{"code":"$code"}}"""
      withReplies(Seq(Accepted.copy(status = httpStatus, body = body),
        ContentUnderstandingStubReply(200, Succeeded))) { server =>
        val output = responses(stage(server).setAnalyzerId("custom")
          .setModelDeployments(Map("model" -> "deployment")), spark.range(2).coalesce(1))
        assert(output.map(_.status) == Seq("Failed", "Succeeded"))
        assert(output.head.httpStatus == httpStatus)
        assert(output.head.rawResponse == body)
        assert(output.head.operationLocation.contains(server.endpoint + LocationPath))
        assert(output.head.error.exists(_.contains(code)))
        assert(!output.head.error.exists(_.contains("SharedConfigurationFailure")))
        assert(server.requests.map(_.method) == Seq("POST", "POST"))
      }
    }
  }

  test("a shared rejection preserves an already dispatched sibling and its row association") {
    val siblingStarted = new CountDownLatch(1)
    val rejectionObserved = new CountDownLatch(1)
    ContentUnderstandingSubmissionSuite.RejectionObserved = rejectionObserved
    val siblingBody = """{"id":"sibling-op","status":"Running","result":{"document":"sibling"}}"""
    val siblingLocation = LocationPath.replace("op-1", "sibling-op")
    val server = new ContentUnderstandingStub(request => {
      if (request.body.contains("/reject")) {
        assert(siblingStarted.await(30, TimeUnit.SECONDS))
        ContentUnderstandingStubReply(401, """{"error":{"code":"Denied"}}""")
      } else {
        assert(request.body.contains("/sibling"))
        siblingStarted.countDown()
        assert(rejectionObserved.await(30, TimeUnit.SECONDS))
        Accepted.copy(body = siblingBody, headers =
          Map("Operation-Location" -> ("$ROOT" + siblingLocation), "Retry-After" -> "0"))
      }
    })
    try {
      val input = spark.createDataFrame(Seq("reject", "sibling", "later", "last")
        .map(id => Row(id, s"https://example.test/$id")).asJava,
        new StructType().add("id", StringType).add("document", StringType)).coalesce(1)
      val transformer = new ContentUnderstanding().setEndpoint(server.endpoint).setOutputCol("result")
        .setErrorCol("error").setDocumentUrlCol("document").setSubscriptionKey("synthetic-key")
        .setOperationMode("submit").setConcurrency(2).setTimeout(30).setMaxOperationDuration(60000)
      // Release the sibling only after the rejection has traversed the connector, not just the stub.
      val observe = udf((error: String) => {
        if (error != null && error.contains("SharedConfigurationFailure")) {
          ContentUnderstandingSubmissionSuite.RejectionObserved.countDown()
        }
        true
      })
      val rows = transformer.transform(input).withColumn("observed", observe(col("result.error"))).collect()
      val output = rows.map(row => row.getAs[String]("id") ->
        ContentUnderstandingResponse.makeFromRowConverter(row.getAs[Row]("result"))).toMap
      assert(rows.length == 4)
      assert(output("reject").error.exists(_.contains("SharedConfigurationFailure")))
      assert(output("sibling").status == "Running")
      assert(output("sibling").id.contains("sibling-op"))
      assert(output("sibling").operationLocation.contains(server.endpoint + siblingLocation))
      assert(output("sibling").rawResponse == siblingBody)
      assert(output("sibling").error.isEmpty)
      Seq("later", "last").foreach { id =>
        assert(output(id).status == "Rejected")
        assert(output(id).httpStatus == 0)
        assert(output(id).operationLocation.isEmpty)
      }
      assert(server.requests.map(_.method) == Seq("POST", "POST"))
      assert(server.requests.count(_.body.contains("/reject")) == 1)
      assert(server.requests.count(_.body.contains("/sibling")) == 1)
    } finally {
      rejectionObserved.countDown()
      server.close()
    }
  }

  test("canceled submissions and polls terminate without replacing the service status") {
    val body = Running.replace("Running", "Canceled")
    val valid = "$ROOT" + LocationPath
    for (mode <- Seq("analyze", "submit", "poll");
         location <- (if (mode == "poll") Seq(None) else
           Seq(None, Some(valid), Some("https://outside.invalid/operation")))) {
      val headers = location.map(value => Map("Operation-Location" -> value)).getOrElse(Map.empty)
      val httpStatus = if (mode == "poll") 200 else 202
      withReplies(Seq(ContentUnderstandingStubReply(httpStatus, body, headers))) { server =>
        val transformer = stage(server).setOperationMode(mode)
        if (mode == "poll") transformer.setOperationLocation(server.endpoint + LocationPath)
        val response = responses(transformer, spark.range(1).coalesce(1)).head
        assert(response.status == "Canceled")
        assert(response.httpStatus == httpStatus)
        assert(response.rawResponse == body)
        assert(response.error.exists(_.contains("OperationFailed")))
        assert(!response.error.exists(error =>
          error.contains("MissingOperationLocation") || error.contains("InvalidOperationLocation")))
        val expected = if (mode == "poll" || location.contains(valid)) Some(server.endpoint + LocationPath) else None
        assert(response.operationLocation == expected)
        assert(server.requests.map(_.method) == Seq(if (mode == "poll") "GET" else "POST"))
      }
    }
  }

  test("NotStarted is submitted, polled to completion, and retained at the poll attempt limit") {
    val body = Running.replace("Running", "NotStarted")
    Seq("analyze", "submit", "poll").foreach { mode =>
      val first = if (mode == "poll") ContentUnderstandingStubReply(200, body) else Accepted.copy(body = body)
      withReplies(Seq(first, ContentUnderstandingStubReply(200, Succeeded))) { server =>
        val transformer = stage(server).setOperationMode(mode).setMaxPollAttempts(1)
        if (mode == "poll") transformer.setOperationLocation(server.endpoint + LocationPath)
        val response = responses(transformer, spark.range(1).coalesce(1)).head
        assert(response.status == (if (mode == "analyze") "Succeeded" else "NotStarted"))
        assert(response.rawResponse == (if (mode == "analyze") Succeeded else body))
        assert(response.operationLocation.contains(server.endpoint + LocationPath))
        if (mode == "poll") assert(response.error.exists(_.contains("PollAttemptsExhausted")))
        else assert(response.error.isEmpty)
        val methods = if (mode == "analyze") Seq("POST", "GET") else Seq(if (mode == "poll") "GET" else "POST")
        assert(server.requests.map(_.method) == methods)
      }
    }
  }

  test("poll entry is immediate while post-submission and subsequent polls honor the fallback delay") {
    val cases = Seq(
      ("poll", ContentUnderstandingStubReply(200, Succeeded), "Succeeded", "GET"),
      ("poll", ContentUnderstandingStubReply(200, Running), "Running", "GET"),
      ("analyze", Accepted.copy(headers = Accepted.headers - "Retry-After"), "Running", "POST"))
    cases.foreach { case (mode, reply, expectedStatus, method) =>
      withReplies(Seq(reply)) { server =>
        val transformer = stage(server).setOperationMode(mode).setPollingDelay(120000)
          .setMaxOperationDuration(60000).setMaxPollAttempts(2)
        if (mode == "poll") transformer.setOperationLocation(server.endpoint + LocationPath)
        val result = responses(transformer, spark.range(1)).head
        assert(result.status == expectedStatus)
        if (expectedStatus == "Succeeded") assert(result.error.isEmpty)
        else assert(result.error.exists(_.contains("OperationTimedOut")))
        assert(result.operationLocation.contains(server.endpoint + LocationPath))
        assert(server.requests.map(_.method) == Seq(method))
      }
    }
  }

  test("delta Retry-After parsing returns milliseconds and rejects unsupported boundary values") {
    val cases = Seq(
      "0" -> Some(0L), "5" -> Some(5000L), ("9" * 19) -> Some(Long.MaxValue),
      ("9" * 20) -> None, "\u0663" -> None, "+5" -> None, " 5 " -> Some(5000L), "" -> None)
    cases.foreach { case (raw, expected) =>
      assert(ContentUnderstandingResponseParsing.retryAfterDeltaMillis(raw) == expected)
      assert(ContentUnderstandingProtocol.retryDelay(Some(raw), 17) == expected.getOrElse(17L))
    }
  }

  test("future HTTP-date Retry-After retains state without an early follow-up request") {
    val date = ZonedDateTime.now(ZoneOffset.UTC).plusYears(10).format(DateTimeFormatter.RFC_1123_DATE_TIME)
    assert(ContentUnderstandingProtocol.retryDelay(Some(date), 7) > TimeUnit.DAYS.toMillis(365))
    assert(ContentUnderstandingProtocol.retryDelay(Some(s" \t$date \t"), 7) > TimeUnit.DAYS.toMillis(365))
    Seq("analyze", "poll").foreach { mode =>
      val reply = if (mode == "poll") ContentUnderstandingStubReply(200, Running, Map("Retry-After" -> date))
        else Accepted.copy(headers = Accepted.headers + ("Retry-After" -> date))
      withReplies(Seq(reply)) { server =>
        val transformer = stage(server).setOperationMode(mode).setMaxOperationDuration(60000)
        if (mode == "poll") transformer.setOperationLocation(server.endpoint + LocationPath)
        val response = responses(transformer, spark.range(1).coalesce(1)).head
        assert(response.status == "Running")
        assert(response.httpStatus == reply.status)
        assert(response.rawResponse == Running)
        assert(response.operationLocation.contains(server.endpoint + LocationPath))
        assert(response.error.exists(_.contains("OperationTimedOut")))
        assert(server.requests.map(_.method) == Seq(if (mode == "poll") "GET" else "POST"))
      }
    }
  }
}
