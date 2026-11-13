// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.contentunderstanding

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.http.HTTPRequestData
import org.apache.http.HttpStatus
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.spark.sql.Row

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.collection.mutable.ArrayBuffer

class ContentUnderstandingThrottlingSuite extends TestBase {
  import ContentUnderstandingFixtures.{
    Accepted => accepted, LocationPath => locationPath, Running => running, Succeeded => succeeded, AnalyzersPath}
  import ContentUnderstandingStub.withReplies

  private def stage(server: ContentUnderstandingStub): ContentUnderstanding =
    new ContentUnderstanding().setEndpoint(server.endpoint).setOutputCol("result").setErrorCol("error")
      .setDocumentBytes(Array[Byte](1)).setPollingDelay(0).setMaxPollAttempts(2)

  private def result(transformer: ContentUnderstanding): ContentUnderstandingResponse =
    ContentUnderstandingResponse.makeFromRowConverter(
      transformer.transform(spark.range(0L, 1L, 1L, 1)).head().getAs[Row]("result"))

  test("opt-in submission throttling honors Retry-After beyond the operation budget") {
    val throttled = ContentUnderstandingStubReply(429, """{"error":{"code":"TooManyRequests"}}""",
      Map("Retry-After" -> "2"))
    Seq("submit", "analyze").foreach { mode =>
      withReplies(Seq(throttled, accepted, ContentUnderstandingStubReply(HttpStatus.SC_OK, succeeded))) { server =>
        val started = System.nanoTime()
        val response = result(stage(server).setOperationMode(mode).setRetryThrottledRequests(true)
          .setMaxOperationDuration(1000).setMaxPollAttempts(1))
        assert(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) >= 2000)
        assert(response.status == (if (mode == "submit") "Running" else "Succeeded"))
        assert(response.operationLocation.contains(server.endpoint + locationPath))
        assert(server.requests.map(_.method) ==
          (if (mode == "submit") Seq("POST", "POST") else Seq("POST", "POST", "GET")))
        assert(server.requests.take(2).map(_.body).distinct.size == 1)
      }
    }
  }

  test("opt-in polling throttling consumes neither attempts nor duration") {
    val throttled = ContentUnderstandingStubReply(429, """{"error":{"code":"TooManyRequests"}}""",
      Map("Retry-After" -> "2"))
    withReplies(Seq(throttled, throttled, ContentUnderstandingStubReply(HttpStatus.SC_OK, succeeded))) { server =>
      val started = System.nanoTime()
      val response = result(stage(server).setOperationMode("poll")
        .setOperationLocation(server.endpoint + locationPath).setRetryThrottledRequests(true)
        .setMaxPollAttempts(1).setMaxOperationDuration(1000))
      assert(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) >= 4000)
      assert(response.status == "Succeeded")
      assert(response.error.isEmpty)
      assert(server.requests.map(_.method) == Seq("GET", "GET", "GET"))
    }
  }

  test("opt-in analysis forwards exact header waits and resets fallback for each exchange") {
    val pastDate = "Wed, 21 Oct 2015 07:28:00 GMT"
    val cases = Seq(None -> 1000L, Some("invalid") -> 1000L, Some(pastDate) -> 0L, Some("2") -> 2000L,
      Some("0" * 17 + "120") -> 120000L, Some("9" * 20) -> Long.MaxValue, Some("0" * 20) -> 0L,
      Some("Sunday, 06-Nov-94 08:49:37 GMT") -> 0L, Some("Sun Nov  6 08:49:37 1994") -> 0L,
      Some("01 Jan 999999999 00:00:00 GMT") -> 1000L)
    for (mode <- Seq("submit", "poll"); (header, expected) <- cases) {
      val throttled = ContentUnderstandingStubReply(429, "",
        header.map("Retry-After" -> _).toMap)
      val finalReply = if (mode == "submit") accepted else ContentUnderstandingStubReply(200, succeeded)
      withReplies(Seq(throttled, finalReply)) { server =>
        val waits = ArrayBuffer.empty[Long]
        val budget = new ContentUnderstandingBudget(10000) {
          override def pause(delay: Long, onTick: () => Unit): Unit = {
            onTick()
            waits += delay
          }
        }
        val settings = ContentUnderstandingProtocol.Settings(server.endpoint + AnalyzersPath,
          1000, 1, 0, 1024, 10000, sharedAnalyzer = false, sharedModels = false, retryThrottledRequests = true)
        val request = new HTTPRequestData(if (mode == "poll") new HttpGet(server.endpoint + locationPath)
          else new HttpPost(server.endpoint + AnalyzersPath + "/prebuilt-read:analyze"))
        assert(Set("Running", "Succeeded")(ContentUnderstandingProtocol.execute(
          settings, request, mode, budgetOverride = Some(budget)).status))
        assert(waits.toSeq == (if (mode == "poll") Seq(0L, expected) else Seq(expected)))
        assert(server.requests.size == 2)
      }
    }
  }

  test("throttling logs are sparse and exclude request credentials, URLs and response bodies") {
    val secret = "synthetic-throttling-secret"
    val throttled = ContentUnderstandingStubReply(429, secret, Map("Retry-After" -> "0"))
    withReplies(Seq.fill(4)(throttled) :+ accepted) { server =>
      ContentUnderstandingFixtures.withLogs { events =>
        assert(result(stage(server).setOperationMode("submit")
          .setConcurrency(2).setSubscriptionKey(secret).setRetryThrottledRequests(true)).status == "Running")
        val messages = events().map(_.getMessage.getFormattedMessage).filter(_.contains("throttled:"))
        assert(messages.size == 1)
        messages.foreach { message =>
          assert(message.contains("retryCount=1 "))
          assert(message.contains("stageId="))
          assert(message.contains("partitionId=0"))
          assert(message.contains("taskAttemptId="))
          assert(message.contains("method=POST"))
          assert(message.contains("waitMs=0"))
          assert(!message.contains(secret))
          assert(!message.contains(server.endpoint))
        }
      }
    }
  }

  test("opt-in throttling does not replay uncertain submissions") {
    val throttled = ContentUnderstandingStubReply(429, """{"error":{"code":"TooManyRequests"}}""",
      accepted.headers)
    val replies = Seq(throttled, throttled.copy(headers = Map("Operation-Location" -> "invalid")),
      ContentUnderstandingStubReply(HttpStatus.SC_INTERNAL_SERVER_ERROR, ""),
      ContentUnderstandingStubReply(HttpStatus.SC_REQUEST_TIMEOUT, ""),
      ContentUnderstandingStubReply(HttpStatus.SC_OK, "", disconnect = true))
    replies.foreach { reply =>
      withReplies(Seq(reply, accepted)) { server =>
        val response = result(stage(server).setOperationMode("submit").setRetryThrottledRequests(true))
        assert(response.status == "Unknown")
        assert(server.requests.map(_.method) == Seq("POST"))
      }
    }
  }

  test("definitive 429 headers permit recovery even when the rejected body cannot be read") {
    Seq(false, true).foreach { truncate =>
      val throttled = ContentUnderstandingStubReply(429, if (truncate) "{}" else "x" * 2048,
        Map("Retry-After" -> "0"), truncate = truncate)
      withReplies(Seq(throttled, accepted)) { server =>
        val response = result(stage(server).setOperationMode("submit").setRetryThrottledRequests(true)
          .setMaxResponseBytes(1024))
        assert(response.status == "Running")
        assert(server.requests.map(_.method) == Seq("POST", "POST"))
      }
    }
  }

  test("opt-in throttling preserves ordinary polling attempt and duration limits") {
    withReplies(Seq(accepted, ContentUnderstandingStubReply(HttpStatus.SC_OK, running))) { server =>
      val response = result(stage(server).setRetryThrottledRequests(true).setMaxPollAttempts(1))
      assert(response.status == "Running")
      assert(response.error.exists(_.contains("PollAttemptsExhausted")))
      assert(server.requests.map(_.method) == Seq("POST", "GET"))
    }
    withReplies(Seq(accepted.copy(headers = accepted.headers + ("Retry-After" -> "120")))) { server =>
      val response = result(stage(server).setRetryThrottledRequests(true).setMaxOperationDuration(1000))
      assert(response.status == "Running")
      assert(response.error.exists(_.contains("OperationTimedOut")))
      assert(server.requests.map(_.method) == Seq("POST"))
    }
  }

  test("opt-in throttling waits can be interrupted for submission and polling") {
    Seq("submit", "poll").foreach { mode =>
      val throttled = ContentUnderstandingStubReply(429, "", Map("Retry-After" -> "60"))
      withReplies(Seq(throttled, accepted)) { server =>
        val waiting = new CountDownLatch(1)
        val failure = new AtomicReference[Exception]()
        val budget = new ContentUnderstandingBudget(1000) {
          override def pause(delay: Long, onTick: () => Unit): Unit = {
            if (delay > 0) waiting.countDown()
            super.pause(delay, onTick)
          }
        }
        val settings = ContentUnderstandingProtocol.Settings(server.endpoint + AnalyzersPath,
          1000, 1, 0, 1024, 1000, sharedAnalyzer = false, sharedModels = false, retryThrottledRequests = true)
        val request = new HTTPRequestData(if (mode == "poll") new HttpGet(server.endpoint + locationPath)
          else new HttpPost(server.endpoint + AnalyzersPath + "/prebuilt-read:analyze"))
        val worker = new Thread(() => {
          try {
            ContentUnderstandingProtocol.execute(settings, request, mode, budgetOverride = Some(budget))
          } catch {
            case error: Exception => failure.set(error)
          } finally {
            waiting.countDown()
          }
        })
        worker.setDaemon(true)
        worker.start()
        try {
          assert(waiting.await(10, TimeUnit.SECONDS), s"$mode throttled wait was not reached.")
          worker.interrupt()
          worker.join(10000)
          assert(!worker.isAlive, s"Interrupted $mode throttling wait did not terminate.")
          assert(failure.get().isInstanceOf[InterruptedException])
          assert(server.requests.size == 1)
        } finally {
          worker.interrupt()
          worker.join(5000)
        }
      }
    }
  }

  test("non-429 admission and poll failures retain ordinary behavior with retry enabled") {
    for (enabled <- Seq(false, true); status <- Seq(401, 403)) {
      withReplies(Seq(ContentUnderstandingStubReply(status, """{"error":{"code":"Denied"}}"""), accepted)) { server =>
        val response = result(stage(server).setOperationMode("submit").setRetryThrottledRequests(enabled))
        assert(response.status == "Rejected")
        assert(response.httpStatus == status)
        assert(server.requests.map(_.method) == Seq("POST"))
      }
    }
    Seq(false, true).foreach { enabled =>
      withReplies(Seq(ContentUnderstandingStubReply(503, """{"error":{"code":"Unavailable"}}"""),
        ContentUnderstandingStubReply(200, succeeded))) { server =>
        val response = result(stage(server).setOperationMode("poll")
          .setOperationLocation(server.endpoint + locationPath).setMaxPollAttempts(1)
          .setRetryThrottledRequests(enabled))
        assert(response.httpStatus == 503)
        assert(response.error.exists(_.contains("Unavailable")))
        assert(server.requests.map(_.method) == Seq("GET"))
      }
    }
  }

  test("fallback growth resets between a rejected submission and its later poll") {
    val throttled = ContentUnderstandingStubReply(429, "")
    withReplies(Seq(throttled, throttled, accepted, throttled,
      ContentUnderstandingStubReply(200, succeeded))) { server =>
      val waits = ArrayBuffer.empty[Long]
      val budget = new ContentUnderstandingBudget(10000) {
        override def pause(delay: Long, onTick: () => Unit): Unit = {
          onTick()
          waits += delay
        }
      }
      val settings = ContentUnderstandingProtocol.Settings(server.endpoint + AnalyzersPath,
        1000, 1, 0, 1024, 10000, sharedAnalyzer = false, sharedModels = false, retryThrottledRequests = true)
      val request = new HTTPRequestData(new HttpPost(server.endpoint + AnalyzersPath + "/prebuilt-read:analyze"))
      assert(ContentUnderstandingProtocol.execute(settings, request, "analyze",
        budgetOverride = Some(budget)).status == "Succeeded")
      assert(waits.size == 4)
      assert(waits.head == 1000L && waits(1) >= 1800L && waits(1) <= 2000L)
      assert(waits.drop(2).toSeq == Seq(0L, 1000L))
      assert(server.requests.map(_.method) == Seq("POST", "POST", "POST", "GET", "GET"))
    }
  }

  test("shared failure and a sibling rejected submission retain shared diagnostics") {
    val order = new AtomicInteger()
    val server = new ContentUnderstandingStub(_ => order.getAndIncrement() match {
      case 0 => ContentUnderstandingStubReply(429, "", Map("Retry-After" -> "60"))
      case 1 => ContentUnderstandingStubReply(404, """{"error":{"code":"AnalyzerNotFound"}}""")
      case _ => accepted
    })
    try {
      val responses = stage(server).setConcurrency(2).setOperationMode("submit").setRetryThrottledRequests(true)
        .transform(spark.range(0L, 2L, 1L, 1)).collect()
        .map(row => ContentUnderstandingResponse.makeFromRowConverter(row.getAs[Row]("result")))
      assert(responses.map(response => response.httpStatus -> response.status).toMap ==
        Map(429 -> "Rejected", 404 -> "Failed"))
      assert(responses.forall(_.error.exists(_.contains("SharedConfigurationFailure"))))
      assert(server.requests.size == 2)
    } finally {
      server.close()
    }
  }

  test("a shared stop unwinds the real submission wait after sleeping without another request") {
    withReplies(Seq(ContentUnderstandingStubReply(429, "", Map("Retry-After" -> "2")), accepted)) { server =>
      val context = new ContentUnderstandingThrottling.Context
      var ticks = 0
      var pauseReturned = false
      val budget = new ContentUnderstandingBudget(10000) {
        override def pause(delay: Long, onTick: () => Unit): Unit = {
          super.pause(delay, () => {
            ticks += 1
            // The first two ticks precede sleep; the third runs after the first real sleep.
            if (ticks == 3) context.stop()
            onTick()
          })
          pauseReturned = true
        }
      }
      val settings = ContentUnderstandingProtocol.Settings(server.endpoint + AnalyzersPath,
        1000, 1, 0, 1024, 10000, sharedAnalyzer = false, sharedModels = false, retryThrottledRequests = true)
      val request = new HTTPRequestData(new HttpPost(server.endpoint + AnalyzersPath + "/prebuilt-read:analyze"))
      ContentUnderstandingFixtures.withLogs { events =>
        val response = ContentUnderstandingProtocol.execute(settings, request, "submit",
          budgetOverride = Some(budget), context = context)
        assert(response.status == "Rejected" && response.httpStatus == 429)
        assert(response.error.exists(_.contains("SharedConfigurationFailure")))
        assert(ticks == 3 && !pauseReturned)
        assert(server.requests.map(_.method) == Seq("POST"))
        val warnings = events().map(_.getMessage.getFormattedMessage).filter(_.contains("throttled:"))
        assert(warnings.size == 1 && warnings.head.contains("retryCount=1 "))
        assert(warnings.head.contains("taskContext=unavailable"))
      }
    }
  }

  test("a partition stop does not stop throttled GETs for already accepted handles") {
    withReplies(Seq(ContentUnderstandingStubReply(429, "", Map("Retry-After" -> "0")),
      ContentUnderstandingStubReply(200, succeeded))) { server =>
      val context = new ContentUnderstandingThrottling.Context
      context.stop()
      val settings = ContentUnderstandingProtocol.Settings(server.endpoint + AnalyzersPath,
        1000, 1, 0, 1024, 10000, sharedAnalyzer = false, sharedModels = false, retryThrottledRequests = true)
      val request = new HTTPRequestData(new HttpGet(server.endpoint + locationPath))
      assert(ContentUnderstandingProtocol.execute(settings, request, "poll", context = context).status == "Succeeded")
      assert(server.requests.map(_.method) == Seq("GET", "GET"))
    }
  }

  test("partition diagnostics stay shared across rows and submission-poll exchanges") {
    val throttled = ContentUnderstandingStubReply(429, "", Map("Retry-After" -> "0"))
    Seq("submit", "analyze").foreach { mode =>
      val replies = if (mode == "submit") Seq(throttled, accepted, throttled, accepted)
        else Seq(throttled, accepted, throttled, ContentUnderstandingStubReply(200, succeeded))
      ContentUnderstandingFixtures.withLogs { events =>
        withReplies(replies) { server =>
          val rows = if (mode == "submit") 2L else 1L
          val output = stage(server).setConcurrency(1).setOperationMode(mode).setRetryThrottledRequests(true)
            .transform(spark.range(0L, rows, 1L, 1)).collect()
          assert(output.length == rows)
          assert(server.requests.map(_.method) ==
            (if (mode == "submit") Seq.fill(4)("POST") else Seq("POST", "POST", "GET", "GET")))
          assert(events().size == 1)
          assert(events().head.getMessage.getFormattedMessage.contains("retryCount=1 "))
        }
      }
    }
  }

  test("a shared stop preserves in-flight acceptance with retry enabled or disabled") {
    Seq(false, true).foreach { enabled =>
      withReplies(Seq(accepted)) { server =>
        val context = new ContentUnderstandingThrottling.Context
        context.stop()
        val settings = ContentUnderstandingProtocol.Settings(server.endpoint + AnalyzersPath,
          1000, 1, 0, 1024, 10000, sharedAnalyzer = false, sharedModels = false,
          retryThrottledRequests = enabled)
        val request = new HTTPRequestData(new HttpPost(server.endpoint + AnalyzersPath + "/prebuilt-read:analyze"))
        val response = ContentUnderstandingProtocol.execute(settings, request, "submit", context = context)
        assert(response.status == "Running" && response.httpStatus == 202)
        assert(response.operationLocation.contains(server.endpoint + locationPath))
        assert(!response.error.exists(_.contains("SharedConfigurationFailure")))
        assert(server.requests.map(_.method) == Seq("POST"))
      }
    }
  }

  test("shared-stop relabel is opt-in and preserves rejected response-read diagnostics") {
    val body = """{"error":{"code":"TooManyRequests"}}"""
    val cases = Seq(
      ContentUnderstandingStubReply(429, body) -> "TooManyRequests",
      ContentUnderstandingStubReply(429, body, truncate = true) -> "ResponseBodyReadFailed",
      ContentUnderstandingStubReply(429, "x" * 2048) -> "ResponseTooLarge")
    for (enabled <- Seq(false, true); (reply, code) <- cases) {
      withReplies(Seq(reply)) { server =>
        val context = new ContentUnderstandingThrottling.Context
        context.stop()
        val settings = ContentUnderstandingProtocol.Settings(server.endpoint + AnalyzersPath,
          1000, 1, 0, 1024, 10000, sharedAnalyzer = false, sharedModels = false,
          retryThrottledRequests = enabled)
        val request = new HTTPRequestData(new HttpPost(server.endpoint + AnalyzersPath + "/prebuilt-read:analyze"))
        val response = ContentUnderstandingProtocol.execute(settings, request, "submit", context = context)
        assert(response.status == "Rejected" && response.httpStatus == 429)
        assert(response.error.exists(_.contains("SharedConfigurationFailure")) == enabled)
        if (!enabled || code != "TooManyRequests") assert(response.error.exists(_.contains(code)))
        if (enabled && code != "TooManyRequests") assert(response.error.exists(_.contains("innererror")))
        if (code == "TooManyRequests") assert(response.rawResponse == body)
        assert(server.requests.size == 1)
      }
    }
  }
}
