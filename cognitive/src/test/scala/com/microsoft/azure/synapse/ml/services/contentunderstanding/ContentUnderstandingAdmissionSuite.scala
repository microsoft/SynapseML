// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.contentunderstanding

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.http.HTTPRequestData
import org.apache.commons.io.FileUtils
import org.apache.http.{HttpHost, HttpRequest, HttpStatus, HttpVersion}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpPost, HttpPut}
import org.apache.http.conn.{ClientConnectionManager, ConnectTimeoutException, DnsResolver}
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.message.BasicHttpResponse
import org.apache.http.params.HttpParams
import org.apache.http.protocol.HttpContext
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{MapType, StringType, StructType}

import java.net.{InetAddress, UnknownHostException}
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.collection.JavaConverters._

class ContentUnderstandingAdmissionSuite extends TestBase {
  private class RetryBudget(expireAfterWait: Boolean) extends ContentUnderstandingBudget(60000) {
    @volatile var expired: Boolean = false
    var waits: Int = 0
    override def remainingMs: Long = if (expired) 0L else 60000L
    override def pause(delay: Long): Unit = if (delay > 0) {
      waits += 1
      expired = expireAfterWait
    }
  }

  private val creationLocation =
    "/contentunderstanding/analyzers/custom/operations/create-1?api-version=2025-11-01"
  private val initialMethods = Map("poll" -> Seq("GET"), "analyze" -> Seq("POST"),
    "get" -> Seq.empty[String], "create" -> Seq("PUT", "GET"))

  private def initialReplies(mode: String): Seq[ContentUnderstandingStubReply] = mode match {
    case "poll" => Seq(ContentUnderstandingStubReply(200, ContentUnderstandingFixtures.Running,
      Map("Retry-After" -> "0")))
    case "analyze" => Seq(ContentUnderstandingFixtures.Accepted)
    case "get" => Seq.empty
    case "create" => Seq(ContentUnderstandingStubReply(201, """{"analyzerId":"custom","status":"creating"}""",
      Map("Operation-Location" -> ("$ROOT" + creationLocation), "Retry-After" -> "0")),
      ContentUnderstandingStubReply(200, """{"status":"Succeeded"}"""))
  }

  private def executeWithBudget(mode: String, endpoint: String,
                                 budget: ContentUnderstandingBudget): ContentUnderstandingResponse = {
    val settings = ContentUnderstandingProtocol.Settings(endpoint + "/contentunderstanding/analyzers",
      5000, 5, 1, 65536, 60000,
      sharedAnalyzer = false, sharedModels = false)
    val analyzerUrl = endpoint + "/contentunderstanding/analyzers/custom?api-version=2025-11-01"
    val get = new HTTPRequestData(new HttpGet(analyzerUrl))
    mode match {
      case "get" =>
        intercept[ContentUnderstandingException] {
          ContentUnderstandingProtocol.getAnalyzer(settings, get, Some(budget))
        }.response
      case "create" =>
        val put = new HttpPut(analyzerUrl)
        put.setEntity(new StringEntity("{}", ContentType.APPLICATION_JSON))
        intercept[ContentUnderstandingException] {
          ContentUnderstandingProtocol.createAnalyzer(settings, new HTTPRequestData(put), get, "custom", Some(budget))
        }.response
      case "poll" =>
        val request = new HTTPRequestData(new HttpGet(endpoint + ContentUnderstandingFixtures.LocationPath))
        ContentUnderstandingProtocol.execute(settings, request, mode, budgetOverride = Some(budget))
      case "analyze" =>
        val post = new HttpPost(endpoint + "/contentunderstanding/analyzers/custom:analyze?api-version=2025-11-01")
        post.setEntity(new StringEntity("""{"inputs":[{"url":"https://example.test/document"}]}""",
          ContentType.APPLICATION_JSON))
        ContentUnderstandingProtocol.execute(settings, new HTTPRequestData(post), mode, budgetOverride = Some(budget))
    }
  }

  private def assertRetryDiagnostic(mode: String, failed: ContentUnderstandingStubReply,
                                    code: String, expire: Boolean): Unit = {
    val newer = ContentUnderstandingStubReply(403, """{"error":{"code":"Forbidden"}}""")
    val replies = initialReplies(mode) ++ Seq(failed, newer)
    ContentUnderstandingStub.withReplies(replies) { server =>
      val budget = new RetryBudget(expire)
      val result = executeWithBudget(mode, server.endpoint, budget)
      val expected = if (expire) failed else newer
      assert(result.error.exists(_.contains(if (expire) code else "Forbidden")))
      assert(!result.error.exists(_.contains("OperationTimedOut")))
      assert(result.httpStatus == (if (expected.disconnect) 0 else expected.status))
      assert(budget.waits == 1)
      val suffix = if (expire) Seq("GET") else Seq("GET", "GET")
      assert(server.requests.map(_.method) == initialMethods(mode) ++ suffix)
      if (mode == "poll" || mode == "analyze") {
        assert(result.status == "Running")
        assert(result.rawResponse == ContentUnderstandingFixtures.Running)
        assert(result.operationLocation.contains(server.endpoint + ContentUnderstandingFixtures.LocationPath))
      } else {
        assert(result.rawResponse == expected.body)
        assert(result.operationLocation == (if (mode == "create") Some(server.endpoint + creationLocation) else None))
      }
    }
  }

  for (cancelOlder <- Seq(false, true)) test(s"DNS capacity preserves admission order, cancelOlder=$cancelOlder") {
    val count = BudgetedDnsResolver.MaxConcurrentLookups
    val entered = new CountDownLatch(count)
    val finished = new CountDownLatch(count)
    val releaseOne = new CountDownLatch(1)
    val releaseAll = new CountDownLatch(1)
    val admitted = new CountDownLatch(1)
    val first = new AtomicReference[Option[String]](None)
    val failure = new AtomicReference[Option[Throwable]](None)
    val delegate = new DnsResolver {
      override def resolve(host: String): Array[InetAddress] = {
        val held = host.startsWith("held-")
        try {
          if (held) {
            entered.countDown()
            val release = if (host == "held-0") releaseOne else releaseAll
            assert(release.await(30, TimeUnit.SECONDS))
          } else {
            first.compareAndSet(None, Some(host))
            admitted.countDown()
            assert(releaseAll.await(30, TimeUnit.SECONDS))
          }
          Array(InetAddress.getByAddress(Array[Byte](127, 0, 0, 1)))
        } finally {
          if (held) finished.countDown()
        }
      }
    }
    def waiter(host: String): Thread = new Thread(() => {
      try {
        new BudgetedDnsResolver(() => 10000L, delegate).resolve(host)
      } catch {
        case _: java.io.InterruptedIOException if cancelOlder && host == "older" => ()
        case error: Throwable => failure.compareAndSet(None, Some(error))
      }
    })
    def awaitWaiting(thread: Thread): Unit = {
      assert((1 to 5000).exists { _ =>
        if (thread.getState == Thread.State.TIMED_WAITING) true
        else {
          Thread.sleep(1)
          false
        }
      }, "DNS caller did not enter its capacity wait.")
    }
    val older = waiter("older")
    val newer = waiter("newer")
    val holders = (0 until count).map(index => waiter(s"held-$index"))
    try {
      holders.foreach(_.start())
      assert(entered.await(5, TimeUnit.SECONDS))
      older.start()
      awaitWaiting(older)
      Thread.sleep(25)
      newer.start()
      awaitWaiting(newer)
      Thread.sleep(35)
      if (cancelOlder) {
        older.interrupt()
        older.join(5000)
        assert(!older.isAlive, "Cancelled DNS capacity waiter did not exit.")
      }
      releaseOne.countDown()
      assert(admitted.await(5, TimeUnit.SECONDS))
      val expected = if (cancelOlder) "newer" else "older"
      assert(first.get().contains(expected), s"Incorrect DNS admission order: ${first.get()}")
    } finally {
      releaseOne.countDown()
      releaseAll.countDown()
      older.join(11000)
      newer.join(11000)
      holders.foreach(_.join(11000))
      assert(finished.await(5, TimeUnit.SECONDS))
    }
    failure.get().foreach(error => throw error)
  }

  for (mode <- Seq("poll", "analyze", "get", "create")) {
    test(s"retry wait expiration preserves the last GET failure but newer HTTP errors win in $mode") {
      val failures = Seq(
        (ContentUnderstandingStubReply(503, """{"error":{"code":"Unavailable"}}"""), "Unavailable"),
        (ContentUnderstandingStubReply(429, """{"error":{"code":"Throttled"}}"""), "Throttled"),
        (ContentUnderstandingStubReply(200, "", disconnect = true), "TransportError"))
      for ((reply, code) <- failures; expire <- Seq(false, true)) {
        assertRetryDiagnostic(mode, reply, code, expire)
      }
    }
  }

  for (mode <- Seq("poll", "analyze")) {
    test(s"retry wait expiration preserves clean remote HTTP status in $mode") {
      val reply = if (mode == "poll") ContentUnderstandingStubReply(200, ContentUnderstandingFixtures.Running)
        else ContentUnderstandingFixtures.Accepted.copy(headers =
          ContentUnderstandingFixtures.Accepted.headers - "Retry-After")
      ContentUnderstandingStub.withReplies(Seq(reply)) { server =>
        val budget = new RetryBudget(expireAfterWait = true)
        val result = executeWithBudget(mode, server.endpoint, budget)
        assert(result.status == "Running")
        assert(result.httpStatus == reply.status)
        assert(result.error.exists(_.contains("OperationTimedOut")))
        assert(result.operationLocation.contains(server.endpoint + ContentUnderstandingFixtures.LocationPath))
        assert(result.rawResponse == ContentUnderstandingFixtures.Running)
        assert(server.requests.map(_.method) == initialMethods(mode))
        assert(budget.waits == 1)
      }
    }
  }

  test("successful nonstandard submissions diagnose an unaccepted handle without following it") {
    for {
      code <- Seq(HttpStatus.SC_NON_AUTHORITATIVE_INFORMATION, HttpStatus.SC_PARTIAL_CONTENT)
      status <- Seq("Running", "NotStarted")
      mode <- Seq("analyze", "submit")
    } {
      val body = s"""{"id":"op-1","status":"$status"}"""
      val reply = ContentUnderstandingStubReply(code, body,
        Map("Operation-Location" -> ("$ROOT" + ContentUnderstandingFixtures.LocationPath)))
      ContentUnderstandingStub.withReplies(Seq(reply)) { server =>
        val stage = new ContentUnderstanding().setEndpoint(server.endpoint)
          .setDocumentBytes(Array[Byte](1)).setPollingDelay(0).setMaxPollAttempts(2)
          .setOutputCol("result").setErrorCol("error").setOperationMode(mode)
        val row = stage.transform(spark.range(1).coalesce(1)).head().getAs[Row]("result")
        val response = ContentUnderstandingResponse.makeFromRowConverter(row)
        assert(response.status == "Unknown")
        assert(response.httpStatus == code)
        assert(response.rawResponse == body)
        assert(response.id.contains("op-1"))
        assert(response.operationLocation.isEmpty)
        assert(response.error.exists(_.contains("UnacceptedOperationLocation")))
        assert(response.error.exists(_.contains(code.toString)))
        assert(!response.error.exists(_.contains("MissingOperationLocation")))
        assert(server.requests.map(_.method) == Seq("POST"))
      }
    }
  }

  test("creation retains accepted PUT state when its first poll cannot be sent") {
    val bodies = Seq(
      ("""{"id":"create-1","status":"Running"}""", "Running", "OperationTimedOut", Some("create-1")),
      ("""{"id":"create-1","status":"Running","error":{"code":"ServiceDetail"}}""",
        "Running", "ServiceDetail", Some("create-1")),
      ("""{"analyzerId":"custom","status":"creating"}""", "Running", "OperationTimedOut", None),
      ("""{"analyzerId":"custom","status":"ready"}""", "Succeeded", "OperationTimedOut", None),
      ("""{"analyzerId":"custom","status":"deleting"}""", "Unknown", "OperationTimedOut", None),
      ("""{"analyzerId":"custom","status":"failed"}""", "Failed", "OperationFailed", None),
      ("""{"analyzerId":"custom","status":"creating","error":{"code":"ServiceDetail"}}""",
        "Running", "ServiceDetail", None),
      ("""{"analyzerId":"custom","status":"future-state"}""", "Failed", "InvalidResponse", None),
      ("""{"analyzerId":"custom"}""", "Failed", "InvalidResponse", None),
      ("not-json", "Failed", "InvalidResponse", None))
    for (httpStatus <- Seq(200, 201, 202); (body, status, error, id) <- bodies; duringWait <- Seq(false, true)) {
      val retryAfter = if (duringWait) "1" else "120"
      val accepted = ContentUnderstandingStubReply(httpStatus, body,
        Map("Operation-Location" -> ("$ROOT" + creationLocation), "Retry-After" -> retryAfter))
      ContentUnderstandingStub.withReplies(Seq(accepted)) { server =>
        val budget = new RetryBudget(expireAfterWait = true)
        val result = executeWithBudget("create", server.endpoint, budget)
        assert(result.status == status)
        assert(result.httpStatus == httpStatus)
        assert(result.rawResponse == body)
        assert(result.error.exists(_.contains(error)))
        assert(result.operationLocation.contains(server.endpoint + creationLocation))
        assert(result.id == id)
        assert(server.requests.map(_.method) == Seq("PUT"))
        assert(budget.waits == (if (duringWait) 1 else 0))
      }
    }
  }

  test("creation without an operation URL preserves its primary diagnostic") {
    val bodies = Seq(
      ("""{"analyzerId":"custom"}""", Some("InvalidResponse")),
      ("""{"analyzerId":"custom","status":"future-state"}""", Some("InvalidResponse")),
      ("""{"analyzerId":"custom","status":"failed"}""", Some("OperationFailed")),
      ("""{"analyzerId":"custom","status":"creating"}""", Some("MissingOperationLocation")),
      ("""{"analyzerId":"custom","status":"deleting"}""", Some("MissingOperationLocation")),
      ("""{"analyzerId":"custom","status":"failed","error":{"code":"ServiceDetail"}}""", Some("ServiceDetail")),
      ("not-json", Some("InvalidResponse")),
      ("""{"analyzerId":"custom","status":"ready"}""", None))
    for (httpStatus <- Seq(200, 201, 202); (body, code) <- bodies) {
      ContentUnderstandingStub.withReplies(Seq(ContentUnderstandingStubReply(httpStatus, body))) { server =>
        val stage = new ContentUnderstanding().setEndpoint(server.endpoint).setAnalyzerId("custom")
        code match {
          case Some(expected) =>
            val result = intercept[ContentUnderstandingException] {
              stage.createAnalyzer("{}", allowReplace = false)
            }.response
            assert(result.status == "Failed")
            assert(result.error.exists(_.contains(expected)))
            assert(result.httpStatus == httpStatus)
            assert(result.rawResponse == body)
            assert(result.operationLocation.isEmpty)
            assert(result.id.isEmpty)
          case None =>
            assert(stage.createAnalyzer("{}", allowReplace = false) == body)
        }
        assert(server.requests.map(_.method) == Seq("PUT"))
      }
    }
  }

  test("resource creation state survives transient-only polling until expiry or attempt exhaustion") {
    val body = """{"analyzerId":"custom","status":"creating","description":"resource-shape"}"""
    for (expire <- Seq(false, true); status <- Seq(429, 503)) {
      val replies = Seq(
        ContentUnderstandingStubReply(201, body,
          Map("Operation-Location" -> ("$ROOT" + creationLocation), "Retry-After" -> "0")),
        ContentUnderstandingStubReply(status, """{"error":{"code":"ServiceUnavailable"}}"""))
      ContentUnderstandingStub.withReplies(replies) { server =>
        val budget = new RetryBudget(expireAfterWait = expire)
        val result = executeWithBudget("create", server.endpoint, budget)
        assert(result.status == "Running")
        assert(result.httpStatus == status)
        assert(result.rawResponse == body)
        assert(result.error.exists(_.contains("ServiceUnavailable")))
        assert(result.operationLocation.contains(server.endpoint + creationLocation))
        assert(server.requests.map(_.method) == Seq("PUT") ++ Seq.fill(if (expire) 1 else 5)("GET"))
      }
    }
  }

  test("resource statuses are not accepted as analysis operation statuses") {
    val body = """{"analyzerId":"custom","status":"creating"}"""
    val reply = ContentUnderstandingFixtures.Accepted.copy(body = body)
    ContentUnderstandingStub.withReplies(Seq(reply)) { server =>
      val endpoint = server.endpoint + "/contentunderstanding/analyzers"
      val settings = ContentUnderstandingProtocol.Settings(endpoint, 5000, 5, 1, 65536, 60000, false, false)
      val request = new HttpPost(endpoint + "/custom:analyze?api-version=2025-11-01")
      request.setEntity(new StringEntity("{}", ContentType.APPLICATION_JSON))
      val result = ContentUnderstandingProtocol.execute(settings, new HTTPRequestData(request), "submit")
      assert(result.status == "Unknown")
      assert(result.error.exists(_.contains("InvalidResponse")))
      assert(result.rawResponse == body)
      assert(result.operationLocation.contains(server.endpoint + ContentUnderstandingFixtures.LocationPath))
      assert(server.requests.map(_.method) == Seq("POST"))
    }
  }

  test("expired analyzer retrieval does not recommend operation polling") {
    ContentUnderstandingStub.withReplies(Seq(ContentUnderstandingStubReply(200, """{"status":"ready"}"""))) { server =>
      val budget = new RetryBudget(expireAfterWait = false)
      budget.expired = true
      val result = executeWithBudget("get", server.endpoint, budget)
      assert(result.error.exists(_.contains("OperationTimedOut")))
      assert(!result.error.exists(_.contains("resume polling")))
      assert(result.operationLocation.isEmpty)
      assert(server.requests.isEmpty)
    }
  }

  test("creation retains completed poll state when the first final GET expires before dispatch") {
    val budget = new RetryBudget(expireAfterWait = false)
    val completed = """{"id":"create-1","status":"Succeeded","details":"creation-complete"}"""
    val server = new ContentUnderstandingStub(request => {
      if (request.method == "PUT") {
        initialReplies("create").head
      } else {
        budget.expired = true
        ContentUnderstandingStubReply(200, completed)
      }
    })
    try {
      val result = executeWithBudget("create", server.endpoint, budget)
      assert(result.status == "Succeeded")
      assert(result.httpStatus == 200)
      assert(result.rawResponse == completed)
      assert(result.id.contains("create-1"))
      assert(result.error.exists(_.contains("OperationTimedOut")))
      assert(!result.error.exists(_.contains("resume polling")))
      val message = new ContentUnderstandingException(result).getMessage
      assert(message.contains("OperationTimedOut"))
      assert(!message.contains("Check the configured") && !message.contains("creation-complete"))
      assert(result.operationLocation.contains(server.endpoint + creationLocation))
      assert(server.requests.map(_.method) == Seq("PUT", "GET"))
    } finally {
      server.close()
    }
  }

  test("dispatched final analyzer responses remain authoritative after successful creation") {
    val replies = Seq(
      (ContentUnderstandingStubReply(403, """{"error":{"code":"Forbidden"}}"""), "Forbidden", 403),
      (ContentUnderstandingStubReply(200, """{"status":"creating"}"""), "AnalyzerNotReady", 200),
      (ContentUnderstandingStubReply(200, "not-json"), "InvalidResponse", 200),
      (ContentUnderstandingStubReply(200, "", disconnect = true), "TransportError", 0))
    for ((reply, code, status) <- replies) {
      ContentUnderstandingStub.withReplies(initialReplies("create") :+ reply) { server =>
        val result = executeWithBudget("create", server.endpoint, new RetryBudget(expireAfterWait = false))
        assert(result.status == "Failed")
        assert(result.httpStatus == status)
        assert(result.rawResponse == reply.body)
        assert(result.error.exists(_.contains(code)))
        assert(result.operationLocation.contains(server.endpoint + creationLocation))
        assert(server.requests.count(_.method == "PUT") == 1)
      }
    }
  }

  private def withDnsCapacityHeld(test: (() => Unit) => Unit): Unit = {
    val count = BudgetedDnsResolver.MaxConcurrentLookups
    val entered = new CountDownLatch(count)
    val finished = new CountDownLatch(count)
    val release = new CountDownLatch(1)
    var occupied = false
    val delegate = new DnsResolver {
      override def resolve(host: String): Array[InetAddress] = {
        entered.countDown()
        try {
          assert(release.await(30, TimeUnit.SECONDS), "DNS capacity fixture was not released.")
          Array(InetAddress.getByAddress(Array[Byte](127, 0, 0, 1)))
        } finally {
          finished.countDown()
        }
      }
    }
    def occupy(): Unit = {
      assert(!occupied)
      occupied = true
      for (_ <- 0 until count) {
        intercept[OperationDeadlineExceeded] {
          new BudgetedDnsResolver(() => 5L, delegate).resolve("held-capacity.test")
        }
      }
      assert(entered.await(5, TimeUnit.SECONDS), "All DNS workers must be occupied.")
    }
    try {
      test(() => occupy())
    } finally {
      release.countDown()
      if (occupied) {
        assert(finished.await(5, TimeUnit.SECONDS), "DNS fixture workers did not finish.")
      }
    }
  }

  for (mode <- Seq("poll", "analyze", "get", "create")) {
    test(s"DNS capacity wait expiration retains the preceding GET response in $mode") {
      withDnsCapacityHeld { occupy =>
        val failed = ContentUnderstandingStubReply(503, """{"error":{"code":"Unavailable"}}""",
          Map("Connection" -> "close"))
        ContentUnderstandingStub.withReplies(initialReplies(mode) :+ failed) { server =>
          val budget = new RetryBudget(expireAfterWait = false) {
            override def remainingMs: Long = if (waits > 0) 20L else 60000L
            override def pause(delay: Long): Unit = {
              super.pause(delay)
              if (delay > 0 && waits == 1) occupy()
            }
          }
          val result = executeWithBudget(mode, server.endpoint, budget)
          assert(result.error.exists(_.contains("Unavailable")))
          assert(!result.error.exists(_.contains("DnsCapacityExceeded")))
          assert(result.httpStatus == 503)
          val raw = if (mode == "poll" || mode == "analyze") ContentUnderstandingFixtures.Running else failed.body
          assert(result.rawResponse == raw)
          assert(server.requests.map(_.method) == initialMethods(mode) :+ "GET")
          assert(budget.waits == 1)
          val location = if (mode == "create") Some(server.endpoint + creationLocation)
            else if (mode == "get") None else Some(server.endpoint + ContentUnderstandingFixtures.LocationPath)
          assert(result.operationLocation == location)
        }
      }
    }
  }

  test("DNS capacity expiration retains its diagnostic when there is no preceding response") {
    withDnsCapacityHeld { occupy =>
      occupy()
      ContentUnderstandingStub.withReplies(Seq(ContentUnderstandingStubReply(200, "{}"))) { server =>
        val budget = new RetryBudget(expireAfterWait = false) {
          override def remainingMs: Long = 20L
        }
        val result = executeWithBudget("get", server.endpoint, budget)
        assert(result.error.exists(_.contains("DnsCapacityExceeded")))
        assert(result.httpStatus == 0)
        assert(result.rawResponse.isEmpty)
        assert(server.requests.isEmpty)
      }
    }
  }

  for (finalGet <- Seq(false, true)) {
    test(s"creation retains known state on first GET DNS capacity expiry with finalGet=$finalGet") {
      withDnsCapacityHeld { occupy =>
        @volatile var beforeUnsentGet = false
        val body = if (finalGet) """{"id":"create-1","status":"Succeeded"}"""
          else """{"id":"create-1","status":"Running"}"""
        val budget = new RetryBudget(expireAfterWait = false) {
          override def remainingMs: Long = if (beforeUnsentGet) 20L else 60000L
          override def pause(delay: Long): Unit = {
            super.pause(delay)
            if (!finalGet && delay > 0) {
              occupy()
              beforeUnsentGet = true
            }
          }
        }
        val server = new ContentUnderstandingStub(request => {
          if (finalGet && request.method == "PUT") {
            initialReplies("create").head
          } else {
            if (finalGet) {
              occupy()
              beforeUnsentGet = true
            }
            val headers = if (finalGet) Map("Connection" -> "close")
              else Map("Connection" -> "close", "Retry-After" -> "1",
                "Operation-Location" -> ("$ROOT" + creationLocation))
            ContentUnderstandingStubReply(if (finalGet) 200 else 202, body, headers)
          }
        })
        try {
          val result = executeWithBudget("create", server.endpoint, budget)
          assert(result.httpStatus == (if (finalGet) 200 else 202))
          assert(result.status == (if (finalGet) "Succeeded" else "Running"))
          assert(result.rawResponse == body)
          assert(result.id.contains("create-1"))
          assert(result.error.exists(_.contains("OperationTimedOut")))
          assert(result.operationLocation.contains(server.endpoint + creationLocation))
          assert(server.requests.map(_.method) == (if (finalGet) Seq("PUT", "GET") else Seq("PUT")))
        } finally {
          server.close()
        }
      }
    }
  }

  test("pre-dispatch retry failures retain prior responses only for deadlines") {
    val failures = Seq(
      (200, new ConnectTimeoutException("Synthetic deadline"), "OperationTimedOut", 200),
      (503, new ConnectTimeoutException("Synthetic deadline"), "Unavailable", 503),
      (503, new DnsResolutionFailed(new UnknownHostException("Synthetic DNS failure")), "DnsResolutionFailed", 0),
      (503, new DnsCapacityExceeded(), "DnsCapacityExceeded", 0))
    for ((priorStatus, failure, code, httpStatus) <- failures) {
      val budget = new RetryBudget(expireAfterWait = false)
      val body = if (priorStatus == 200) ContentUnderstandingFixtures.Running
        else """{"error":{"code":"Unavailable"}}"""
      val calls = new AtomicInteger()
      val client = new CloseableHttpClient {
        override def close(): Unit = ()
        override def getConnectionManager: ClientConnectionManager = throw new UnsupportedOperationException
        override def getParams: HttpParams = throw new UnsupportedOperationException
        override protected def doExecute(target: HttpHost, request: HttpRequest,
                                          context: HttpContext): CloseableHttpResponse = {
          if (calls.incrementAndGet() == 1) {
            val response = new BasicHttpResponse(HttpVersion.HTTP_1_1, priorStatus, "Synthetic")
              with CloseableHttpResponse {
              override def close(): Unit = ()
            }
            response.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON))
            response
          } else {
            budget.expired = true
            throw failure
          }
        }
      }
      val endpoint = "https://example.invalid/contentunderstanding/analyzers"
      val settings = ContentUnderstandingProtocol.Settings(endpoint, 5000, 5, 1, 65536, 60000, false, false)
      val location = "https://example.invalid" + ContentUnderstandingFixtures.LocationPath
      try {
        val result = ContentUnderstandingProtocol.pollWithClient(client, settings,
          new HTTPRequestData(new HttpGet(location)), location, None, 0L, budget, None,
          context = new ContentUnderstandingThrottling.Context)
        assert(result.error.exists(_.contains(code)))
        assert(result.httpStatus == httpStatus)
        assert(result.rawResponse == body)
        assert(result.operationLocation.contains(location))
        assert(calls.get() == 2)
      } finally {
        client.close()
      }
    }
  }

  test("retry DNS deadline before dispatch preserves the last GET error") {
    val body = """{"error":{"code":"Unavailable"}}"""
    ContentUnderstandingStub.withReplies(Seq(
      ContentUnderstandingStubReply(503, body, Map("Connection" -> "close")))) { server =>
      val budget = new RetryBudget(expireAfterWait = false)
      val lookups = new AtomicInteger()
      val resolver = new DnsResolver {
        override def resolve(host: String): Array[InetAddress] = {
          if (lookups.incrementAndGet() == 2) {
            budget.expired = true
          }
          Array(InetAddress.getByAddress(Array[Byte](127, 0, 0, 1)))
        }
      }
      val settings = ContentUnderstandingProtocol.Settings(server.endpoint + "/contentunderstanding/analyzers",
        5000, 5, 1, 65536, 60000, false, false)
      val location = server.endpoint + ContentUnderstandingFixtures.LocationPath
      val result = ContentUnderstandingProtocol.execute(settings, new HTTPRequestData(new HttpGet(location)),
        "poll", resolver, Some(budget))
      assert(result.error.exists(_.contains("Unavailable")))
      assert(!result.error.exists(_.contains("OperationTimedOut")))
      assert(result.httpStatus == 503)
      assert(result.rawResponse == body)
      assert(result.operationLocation.contains(location))
      assert(server.requests.map(_.method) == Seq("GET"))
      assert(lookups.get() == 2)
    }
  }

  test("a newer HTTP failure still wins when the budget expires after dispatch") {
    for (mode <- Seq("poll", "analyze", "get", "create")) {
      val budget = new RetryBudget(expireAfterWait = false)
      val latest = ContentUnderstandingStubReply(403, """{"error":{"code":"Forbidden"}}""")
      val replies = initialReplies(mode) ++ Seq(
        ContentUnderstandingStubReply(503, """{"error":{"code":"Unavailable"}}"""), latest)
      val next = new AtomicInteger()
      val server = new ContentUnderstandingStub(_ => {
        val index = next.getAndIncrement()
        if (index == replies.size - 1) {
          budget.expired = true
        }
        replies(math.min(index, replies.size - 1))
      })
      try {
        val result = executeWithBudget(mode, server.endpoint, budget)
        assert(result.error.exists(_.contains("Forbidden")))
        assert(result.httpStatus == 403)
        assert(server.requests.size == replies.size)
        assert(budget.waits == 1)
      } finally {
        server.close()
      }
    }
  }

  test("DNS budget exhaustion before worker submission neither starts lookups nor leaks capacity") {
    val entered = new CountDownLatch(1)
    val loopback = InetAddress.getByAddress(Array[Byte](127, 0, 0, 1))
    val delegate = new DnsResolver {
      override def resolve(host: String): Array[InetAddress] = {
        entered.countDown()
        Array(loopback)
      }
    }
    intercept[OperationDeadlineExceeded] {
      new BudgetedDnsResolver(() => 0L, delegate).resolve("expired-at-entry.test")
    }
    for (_ <- 0 to BudgetedDnsResolver.MaxConcurrentLookups) {
      val reads = new AtomicInteger()
      val remaining = () => if (reads.getAndIncrement() == 0) 5000L else 0L
      intercept[OperationDeadlineExceeded] {
        new BudgetedDnsResolver(remaining, delegate).resolve("expired-after-admission.test")
      }
    }
    assert(!entered.await(1, TimeUnit.SECONDS), "An expired caller submitted DNS work.")
    val addresses = new BudgetedDnsResolver(() => 5000L, delegate).resolve("within-budget.test")
    assert(addresses.toSeq == Seq(loopback))
  }

  test("shared missing or nonstandard scalar authentication stops later submissions") {
    for (status <- Seq(401, 403); headerSource <- Seq("none", "customHeaders", "telemHeaders")) {
      val denied = s"""{"error":{"code":"Denied","message":"synthetic-service-detail-$status"}}"""
      ContentUnderstandingStub.withReplies(Seq(ContentUnderstandingStubReply(status, denied))) { server =>
        val stage = new ContentUnderstanding().setEndpoint(server.endpoint).setOutputCol("result")
          .setDocumentBytes(Array[Byte](1)).setOperationMode("submit").setConcurrency(1)
        if (headerSource == "customHeaders") {
          stage.setCustomHeaders(Map("X-Gateway-Key" -> "synthetic-gateway-value"))
        } else if (headerSource == "telemHeaders") {
          stage.setTelemHeaders(Map("X-Gateway-Key" -> "synthetic-gateway-value"))
        }
        val rows = stage.transform(spark.range(2).coalesce(1)).collect().map(_.getAs[Row]("result"))
        assert(rows.map(_.getAs[String]("status")).toSeq == Seq("Rejected", "Rejected"))
        assert(rows.forall(_.getAs[String]("error").contains("SharedConfigurationFailure")))
        assert(rows.map(_.getAs[Int]("httpStatus")).toSeq == Seq(status, 0))
        assert(rows.head.getAs[String]("rawResponse") == denied)
        assert(rows.forall(_.isNullAt(rows.head.fieldIndex("operationLocation"))))
        assert(server.requests.map(_.method) == Seq("POST"))
        assert(!server.requests.head.headers.contains("authorization"))
        assert(!server.requests.head.headers.contains("ocp-apim-subscription-key"))
        if (headerSource != "none") {
          assert(server.requests.head.headers("x-gateway-key") == "synthetic-gateway-value")
        }
      }
    }
  }

  test("potential row authentication without a scalar credential remains row-specific") {
    val authParameters = Seq("subscriptionKey", "AADToken", "CustomAuthHeader", "customHeaders", "telemHeaders")
    for (param <- authParameters; empty <- Seq(false, true)) {
      val denied = ContentUnderstandingStubReply(401, """{"error":{"code":"Denied"}}""")
      ContentUnderstandingStub.withReplies(Seq(denied, denied)) { server =>
        val schema = new StructType().add("credential", StringType)
          .add("headers", MapType(StringType, StringType))
        val headers = if (empty) Map.empty[String, String] else Map("X-Gateway-Key" -> "synthetic-value")
        val row = Row(if (empty) "" else "synthetic-credential", headers)
        val frame = spark.createDataFrame(Seq(row, row).asJava, schema).coalesce(1)
        val stage = new ContentUnderstanding().setEndpoint(server.endpoint).setOutputCol("result")
          .setDocumentBytes(Array[Byte](1)).setOperationMode("submit").setConcurrency(1)
          .setVectorParam(param, if (param.endsWith("Headers")) "headers" else "credential")
        val rows = stage.transform(frame).collect().map(_.getAs[Row]("result"))
        assert(rows.map(_.getAs[Int]("httpStatus")).toSeq == Seq(401, 401))
        assert(rows.forall(_.getAs[String]("status") == "Rejected"))
        assert(rows.forall(!_.getAs[String]("error").contains("SharedConfigurationFailure")))
        assert(server.requests.map(_.method) == Seq("POST", "POST"))
      }
    }
  }

  test("analyzer creation requires an explicit ID while retrieval retains the default") {
    val ready = """{"analyzerId":"prebuilt-read","status":"ready"}"""
    ContentUnderstandingStub.withReplies(Seq(ContentUnderstandingStubReply(200, ready))) { server =>
      val stage = new ContentUnderstanding().setEndpoint(server.endpoint)
      for (replace <- Seq(false, true); cleared <- Seq(false, true)) {
        if (cleared) {
          stage.setAnalyzerId("custom")
          stage.clear(stage.analyzerId)
        }
        val error = intercept[IllegalArgumentException] {
          stage.createAnalyzer("""{"baseAnalyzerId":"prebuilt-document"}""", allowReplace = replace)
        }
        assert(error.getMessage.contains("explicit analyzerId"))
        assert(server.requests.isEmpty)
      }
      assert(stage.getAnalyzer() == ready)
      assert(server.requests.map(_.method) == Seq("GET"))
      assert(server.requests.head.path == "/contentunderstanding/analyzers/prebuilt-read")
    }
  }

  test("unset and cleared analyzer IDs stay implicit through copy save load and resave") {
    val ready = """{"analyzerId":"prebuilt-read","status":"ready"}"""
    ContentUnderstandingStub.withReplies(Seq(ContentUnderstandingStubReply(200, ready))) { server =>
      for (cleared <- Seq(false, true)) {
        val stage = new ContentUnderstanding().setEndpoint(server.endpoint)
        if (cleared) {
          stage.setAnalyzerId("custom")
          stage.clear(stage.analyzerId)
        }
        val directory = Files.createTempDirectory("cu-explicit-id-").toFile
        try {
          val path = new java.io.File(directory, "stage").getAbsolutePath
          stage.copy(ParamMap.empty).write.session(spark).save(path)
          val loaded = ContentUnderstanding.load(path)
          val resavedPath = new java.io.File(directory, "resaved").getAbsolutePath
          loaded.copy(ParamMap.empty).write.session(spark).save(resavedPath)
          for (candidate <- Seq(stage, loaded, ContentUnderstanding.load(resavedPath))) {
            for (replace <- Seq(false, true)) {
              val error = intercept[IllegalArgumentException](candidate.createAnalyzer("{}", replace))
              assert(error.getMessage.contains("explicit analyzerId"))
            }
            assert(!candidate.isSet(candidate.analyzerId))
            assert(candidate.getAnalyzerId == "prebuilt-read")
            assert(candidate.getAnalyzer() == ready)
          }
          assert(server.requests.forall(_.method == "GET"))
        } finally {
          FileUtils.deleteDirectory(directory)
        }
      }
    }
  }

  test("explicit scalar and column analyzer settings preserve ordinary Spark parameter semantics") {
    val ready = """{"status":"ready"}"""
    ContentUnderstandingStub.withReplies(Seq(ContentUnderstandingStubReply(201, ready))) { server =>
      for (id <- Seq("prebuilt-read", "custom")) {
        val stage = new ContentUnderstanding().setEndpoint(server.endpoint)
        stage.set(stage.analyzerId, Left(id))
        val directory = Files.createTempDirectory("cu-selected-id-").toFile
        try {
          val path = new java.io.File(directory, "stage").getAbsolutePath
          stage.copy(ParamMap.empty).write.session(spark).save(path)
          val loaded = ContentUnderstanding.load(path)
          assert(loaded.isSet(loaded.analyzerId))
          for (replace <- Seq(false, true)) {
            assert(loaded.createAnalyzer("{}", replace) == ready)
            assert(server.requests.last.path == s"/contentunderstanding/analyzers/$id")
          }
          val column = loaded.copy(ParamMap(loaded.analyzerId -> Right("analyzerColumn")))
          val columnPath = new java.io.File(directory, "column").getAbsolutePath
          column.write.session(spark).save(columnPath)
          val restoredColumn = ContentUnderstanding.load(columnPath)
          assert(restoredColumn.getAnalyzerIdCol == "analyzerColumn")
          intercept[IllegalArgumentException](restoredColumn.createAnalyzer("{}", allowReplace = true))
          restoredColumn.clear(restoredColumn.analyzerId)
          assert(restoredColumn.getAnalyzerId == "prebuilt-read")
          intercept[IllegalArgumentException](restoredColumn.createAnalyzer("{}", allowReplace = true))
          assert(server.requests.forall(_.method == "PUT"))
        } finally {
          FileUtils.deleteDirectory(directory)
        }
      }
    }
  }
}
