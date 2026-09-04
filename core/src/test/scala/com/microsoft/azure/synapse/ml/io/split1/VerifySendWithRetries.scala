// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.split1

import com.microsoft.azure.synapse.ml.fabric.FabricClient
import com.microsoft.azure.synapse.ml.io.http.{HTTPRequestData, HandlingUtils, HeaderData, RequestLineData}

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients

import java.net.{InetSocketAddress, ServerSocket}
import java.util.concurrent.{ConcurrentLinkedQueue, Executors}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.collection.JavaConverters._
import scala.io.Source

class VerifySendWithRetries extends TestBase {

  private def getFreePort: Int = {
    val ss = new ServerSocket(0)
    val port = ss.getLocalPort
    ss.close()
    port
  }

  private def startServer(port: Int)(handler: HttpExchange => Unit): HttpServer = {
    val server = HttpServer.create(new InetSocketAddress("localhost", port), 10)
    server.setExecutor(Executors.newFixedThreadPool(2))
    server.createContext("/test", (exchange: HttpExchange) => handler(exchange))
    server.start()
    server
  }

  private def respond(exchange: HttpExchange, code: Int, body: String = "",
                      headers: Map[String, String] = Map.empty): Unit = {
    headers.foreach { case (k, v) => exchange.getResponseHeaders.add(k, v) }
    val bytes = body.getBytes("UTF-8")
    exchange.sendResponseHeaders(code, if (bytes.isEmpty) -1 else bytes.length)
    if (bytes.nonEmpty) {
      val os = exchange.getResponseBody
      os.write(bytes)
      os.close()
    }
    exchange.close()
  }

  private def readRequestBody(exchange: HttpExchange): String = {
    val source = Source.fromInputStream(exchange.getRequestBody, "UTF-8")
    try {
      source.mkString
    } finally {
      source.close()
    }
  }

  private def fabricPost(port: Int): HTTPRequestData = {
    val request = new HttpPost(s"http://localhost:$port/test")
    request.setHeader(HTTPRequestData.FabricAuthMarkerHeader, "true")
    request.setHeader("X-Custom", "preserved")
    request.setHeader("X-Taxonomy-TrafficType", "Background")
    request.setHeader("X-Llm-Service-Tier", "flex")
    request.setHeader("X-Taxonomy-ExtendedProperties", """{"feature":"synapseml"}""")
    request.setHeader("x-ms-llm-feature-name", "SparkCodeFirst")
    request.setEntity(new StringEntity("""{"prompt":"hello"}""", "UTF-8"))
    new HTTPRequestData(request)
  }

  test("429 without Retry-After uses exponential backoff") {
    val port = getFreePort
    val requestCount = new AtomicInteger(0)
    val server = startServer(port) { exchange =>
      val n = requestCount.incrementAndGet()
      if (n <= 3) {
        respond(exchange, 429)
      } else {
        respond(exchange, 200, """{"ok":true}""")
      }
    }
    try {
      val client = HttpClients.createDefault()
      val request = new HttpGet(s"http://localhost:$port/test")
      val start = System.currentTimeMillis()
      val response = HandlingUtils.sendWithRetries(
        client, request, Array(100, 100, 100, 100))
      val elapsed = System.currentTimeMillis() - start
      val code = response.getStatusLine.getStatusCode
      response.close()
      client.close()

      assert(code === 200, "Should eventually succeed")
      assert(requestCount.get() === 4, "Should have retried 3 times then succeeded")
      // Exponential: ~200ms + ~400ms + ~800ms = ~1400ms minimum (plus jitter)
      // Fixed (old behavior) would be ~300ms (3 * 100ms)
      assert(elapsed >= 1000, s"Exponential backoff should take >=1000ms, took ${elapsed}ms")
    } finally {
      server.stop(0)
    }
  }

  test("429 with numeric Retry-After header is respected") {
    val port = getFreePort
    val requestCount = new AtomicInteger(0)
    val server = startServer(port) { exchange =>
      val n = requestCount.incrementAndGet()
      if (n == 1) {
        respond(exchange, 429, headers = Map("Retry-After" -> "1"))
      } else {
        respond(exchange, 200, """{"ok":true}""")
      }
    }
    try {
      val client = HttpClients.createDefault()
      val request = new HttpGet(s"http://localhost:$port/test")
      val start = System.currentTimeMillis()
      val response = HandlingUtils.sendWithRetries(
        client, request, Array(100, 100))
      val elapsed = System.currentTimeMillis() - start
      val code = response.getStatusLine.getStatusCode
      response.close()
      client.close()

      assert(code === 200)
      assert(requestCount.get() === 2)
      // Retry-After: 1 = 1000ms (plus up to 10% jitter = 1100ms max)
      assert(elapsed >= 900, s"Should sleep ~1000ms from Retry-After, took ${elapsed}ms")
      assert(elapsed < 3000, s"Should not overshoot, took ${elapsed}ms")
    } finally {
      server.stop(0)
    }
  }

  test("429 with non-numeric Retry-After falls back to exponential backoff") {
    val port = getFreePort
    val requestCount = new AtomicInteger(0)
    val server = startServer(port) { exchange =>
      val n = requestCount.incrementAndGet()
      if (n == 1) {
        respond(exchange, 429, headers = Map("Retry-After" -> "Thu, 01 Dec 2025 16:00:00 GMT"))
      } else {
        respond(exchange, 200, """{"ok":true}""")
      }
    }
    try {
      val client = HttpClients.createDefault()
      val request = new HttpGet(s"http://localhost:$port/test")
      val response = HandlingUtils.sendWithRetries(
        client, request, Array(100, 100))
      val code = response.getStatusLine.getStatusCode
      response.close()
      client.close()

      assert(code === 200, "Non-numeric Retry-After should not crash")
      assert(requestCount.get() === 2)
    } finally {
      server.stop(0)
    }
  }

  test("429 with negative Retry-After falls back to exponential backoff") {
    val port = getFreePort
    val requestCount = new AtomicInteger(0)
    val server = startServer(port) { exchange =>
      val n = requestCount.incrementAndGet()
      if (n == 1) {
        respond(exchange, 429, headers = Map("Retry-After" -> "-1"))
      } else {
        respond(exchange, 200, """{"ok":true}""")
      }
    }
    try {
      val client = HttpClients.createDefault()
      val request = new HttpGet(s"http://localhost:$port/test")
      val response = HandlingUtils.sendWithRetries(
        client, request, Array(100, 100))
      val code = response.getStatusLine.getStatusCode
      response.close()
      client.close()

      assert(code === 200, "Negative Retry-After should not crash")
      assert(requestCount.get() === 2)
    } finally {
      server.stop(0)
    }
  }

  test("429 does not consume retriesLeft entries") {
    val port = getFreePort
    val requestCount = new AtomicInteger(0)
    val server = startServer(port) { exchange =>
      val n = requestCount.incrementAndGet()
      if (n <= 3) {
        respond(exchange, 429)
      } else {
        respond(exchange, 200, """{"ok":true}""")
      }
    }
    try {
      val client = HttpClients.createDefault()
      val request = new HttpGet(s"http://localhost:$port/test")
      // Only 2 retry slots, but 429 should not consume them
      val response = HandlingUtils.sendWithRetries(
        client, request, Array(50, 50))
      val code = response.getStatusLine.getStatusCode
      response.close()
      client.close()

      assert(code === 200)
      assert(requestCount.get() === 4, "429 retries should be unlimited (not bounded by retriesLeft)")
    } finally {
      server.stop(0)
    }
  }

  test("non-429 error consumes retriesLeft and eventually returns error") {
    val port = getFreePort
    val requestCount = new AtomicInteger(0)
    val server = startServer(port) { exchange =>
      requestCount.incrementAndGet()
      respond(exchange, 503, "Service Unavailable")
    }
    try {
      val client = HttpClients.createDefault()
      val request = new HttpGet(s"http://localhost:$port/test")
      val response = HandlingUtils.sendWithRetries(
        client, request, Array(50, 50, 50))
      val code = response.getStatusLine.getStatusCode
      response.close()
      client.close()

      assert(code === 503, "Should return the error after retries exhausted")
      // Initial request + 3 retries = 4 total
      assert(requestCount.get() === 4)
    } finally {
      server.stop(0)
    }
  }

  test("extraCodesToRetry causes retry on specified codes") {
    val port = getFreePort
    val requestCount = new AtomicInteger(0)
    val server = startServer(port) { exchange =>
      val n = requestCount.incrementAndGet()
      if (n <= 2) {
        respond(exchange, 404, "Not Found")
      } else {
        respond(exchange, 200, """{"ok":true}""")
      }
    }
    try {
      val client = HttpClients.createDefault()
      val request = new HttpGet(s"http://localhost:$port/test")
      val response = HandlingUtils.sendWithRetries(
        client, request, Array(50, 50, 50), extraCodesToRetry = Set(404))
      val code = response.getStatusLine.getStatusCode
      response.close()
      client.close()

      assert(code === 200)
      assert(requestCount.get() === 3)
    } finally {
      server.stop(0)
    }
  }

  test("Retry-After capped to MaxBackoffMs") {
    val port = getFreePort
    val requestCount = new AtomicInteger(0)
    val server = startServer(port) { exchange =>
      val n = requestCount.incrementAndGet()
      if (n == 1) {
        // Server asks for 120s, but we cap to 60s
        respond(exchange, 429, headers = Map("Retry-After" -> "120"))
      } else {
        respond(exchange, 200, """{"ok":true}""")
      }
    }
    try {
      val client = HttpClients.createDefault()
      val request = new HttpGet(s"http://localhost:$port/test")
      val start = System.currentTimeMillis()
      val response = HandlingUtils.sendWithRetries(
        client, request, Array(100, 100))
      val elapsed = System.currentTimeMillis() - start
      val code = response.getStatusLine.getStatusCode
      response.close()
      client.close()

      assert(code === 200)
      // Should be capped to ~60s (MaxBackoffMs), not 120s
      // We just verify it didn't wait the full 120s
      assert(elapsed < 90000, s"Retry-After should be capped to 60s, took ${elapsed}ms")
    } finally {
      server.stop(0)
    }
  }

  test("429 with CapacityLimitExceeded body is not retried") {
    val port = getFreePort
    val requestCount = new AtomicInteger(0)
    val capacityBody =
      """{"error":{"code":"CapacityLimitExceeded","message":"Serverless capacity limit exceeded"}}"""
    val server = startServer(port) { exchange =>
      val n = requestCount.incrementAndGet()
      if (n == 1) {
        respond(exchange, 429, capacityBody)
      } else {
        respond(exchange, 200, """{"ok":true}""")
      }
    }
    try {
      val client = HttpClients.createDefault()
      val request = new HttpGet(s"http://localhost:$port/test")
      val response = HandlingUtils.sendWithRetries(
        client, request, Array(100, 100, 100))
      val code = response.getStatusLine.getStatusCode
      response.close()
      client.close()

      assert(code === 429, "Capacity-exceeded 429 should be returned immediately, not retried")
      assert(requestCount.get() === 1, "Should not retry on CapacityLimitExceeded")
    } finally {
      server.stop(0)
    }
  }

  test("429 with CapacityLimitExceeded ignores Retry-After header") {
    val port = getFreePort
    val requestCount = new AtomicInteger(0)
    val capacityBody =
      """{"error":{"code":"CapacityLimitExceeded","message":"Serverless capacity limit exceeded"}}"""
    val server = startServer(port) { exchange =>
      val n = requestCount.incrementAndGet()
      if (n == 1) {
        respond(exchange, 429, capacityBody, headers = Map("Retry-After" -> "5"))
      } else {
        respond(exchange, 200, """{"ok":true}""")
      }
    }
    try {
      val client = HttpClients.createDefault()
      val request = new HttpGet(s"http://localhost:$port/test")
      val start = System.currentTimeMillis()
      val response = HandlingUtils.sendWithRetries(
        client, request, Array(100, 100, 100))
      val elapsed = System.currentTimeMillis() - start
      val code = response.getStatusLine.getStatusCode
      response.close()
      client.close()

      assert(code === 429, "Capacity-exceeded should not retry even with Retry-After")
      assert(requestCount.get() === 1, "Should not retry on CapacityLimitExceeded")
      // Verify we didn't sleep for the 5s Retry-After
      assert(elapsed < 4000, s"Should ignore Retry-After and return quickly, took ${elapsed}ms")
    } finally {
      server.stop(0)
    }
  }

  test("429 with non-capacity error body still retries normally") {
    val port = getFreePort
    val requestCount = new AtomicInteger(0)
    val rateLimitBody = """{"error":{"code":"RateLimitExceeded","message":"Too many requests"}}"""
    val server = startServer(port) { exchange =>
      val n = requestCount.incrementAndGet()
      if (n <= 2) {
        respond(exchange, 429, rateLimitBody)
      } else {
        respond(exchange, 200, """{"ok":true}""")
      }
    }
    try {
      val client = HttpClients.createDefault()
      val request = new HttpGet(s"http://localhost:$port/test")
      val response = HandlingUtils.sendWithRetries(
        client, request, Array(100, 100, 100))
      val code = response.getStatusLine.getStatusCode
      response.close()
      client.close()

      assert(code === 200, "Non-capacity 429 should still retry and eventually succeed")
      assert(requestCount.get() === 3, "Should have retried past the rate-limit 429s")
    } finally {
      server.stop(0)
    }
  }

  test("429 with CapacityLimitExceeded and chunked encoding (no Content-Length) is not retried") {
    val port = getFreePort
    val requestCount = new AtomicInteger(0)
    val capacityBody =
      """{"error":{"code":"CapacityLimitExceeded","message":"Serverless capacity limit exceeded"}}"""
    val server = startServer(port) { exchange =>
      val n = requestCount.incrementAndGet()
      if (n == 1) {
        // Send with Content-Length = 0 (chunked) to simulate no Content-Length header
        exchange.sendResponseHeaders(429, 0)
        val os = exchange.getResponseBody
        os.write(capacityBody.getBytes("UTF-8"))
        os.close()
        exchange.close()
      } else {
        respond(exchange, 200, """{"ok":true}""")
      }
    }
    try {
      val client = HttpClients.createDefault()
      val request = new HttpGet(s"http://localhost:$port/test")
      val response = HandlingUtils.sendWithRetries(
        client, request, Array(100, 100, 100))
      val code = response.getStatusLine.getStatusCode
      response.close()
      client.close()

      assert(code === 429, "Chunked capacity-exceeded 429 should be returned immediately")
      assert(requestCount.get() === 1, "Should not retry on chunked CapacityLimitExceeded")
    } finally {
      server.stop(0)
    }
  }

  test("429 with Retry-After 0 means retry immediately") {
    val port = getFreePort
    val requestCount = new AtomicInteger(0)
    val server = startServer(port) { exchange =>
      val n = requestCount.incrementAndGet()
      if (n == 1) {
        respond(exchange, 429, headers = Map("Retry-After" -> "0"))
      } else {
        respond(exchange, 200, """{"ok":true}""")
      }
    }
    try {
      val client = HttpClients.createDefault()
      val request = new HttpGet(s"http://localhost:$port/test")
      val start = System.currentTimeMillis()
      val response = HandlingUtils.sendWithRetries(
        client, request, Array(100, 100))
      val elapsed = System.currentTimeMillis() - start
      val code = response.getStatusLine.getStatusCode
      response.close()
      client.close()

      assert(code === 200)
      assert(requestCount.get() === 2)
      // Retry-After: 0 means retry immediately — should complete very fast
      assert(elapsed < 2000, s"Retry-After: 0 should retry immediately, took ${elapsed}ms")
    } finally {
      server.stop(0)
    }
  }

  test("implicit Fabric auth refreshes and replays a request once after 401") {
    val port = getFreePort
    val requestCount = new AtomicInteger(0)
    val refreshCount = new AtomicInteger(0)
    val currentAuth = new AtomicReference("MwcToken stale")
    val authHeaders = new ConcurrentLinkedQueue[String]()
    val requestBodies = new ConcurrentLinkedQueue[String]()
    val customHeaders = new ConcurrentLinkedQueue[String]()
    val taxonomyHeaders = new ConcurrentLinkedQueue[String]()
    val server = startServer(port) { exchange =>
      authHeaders.add(exchange.getRequestHeaders.getFirst("Authorization"))
      customHeaders.add(exchange.getRequestHeaders.getFirst("X-Custom"))
      taxonomyHeaders.add(exchange.getRequestHeaders.getFirst("X-Taxonomy-TrafficType"))
      requestBodies.add(readRequestBody(exchange))
      if (requestCount.incrementAndGet() == 1) {
        respond(exchange, 401, "expired")
      } else {
        respond(exchange, 200, """{"ok":true}""")
      }
    }
    try {
      val client = HttpClients.createDefault()
      val requestData = fabricPost(port)
      val (response, request) = HandlingUtils.sendWithFabricAuthRetries(
        client,
        requestData,
        Array(10),
        getAuthHeader = () => currentAuth.get(),
        refreshAuthHeader = rejectedAuthHeader => {
          assert(rejectedAuthHeader == "MwcToken stale")
          refreshCount.incrementAndGet()
          currentAuth.set("MwcToken fresh")
          currentAuth.get()
        })
      val code = response.getStatusLine.getStatusCode
      response.close()
      request.releaseConnection()
      client.close()

      assert(code === 200)
      assert(requestCount.get() === 2)
      assert(refreshCount.get() === 1)
      assert(authHeaders.asScala.toSeq === Seq("MwcToken stale", "MwcToken fresh"))
      assert(requestBodies.asScala.toSeq === Seq("""{"prompt":"hello"}""", """{"prompt":"hello"}"""))
      assert(customHeaders.asScala.toSeq === Seq("preserved", "preserved"))
      assert(taxonomyHeaders.asScala.toSeq === Seq("Background", "Background"))
      Seq(
        "X-Taxonomy-TrafficType",
        "X-Llm-Service-Tier",
        "X-Taxonomy-ExtendedProperties",
        "x-ms-llm-feature-name"
      ).foreach { headerName =>
        assert(requestData.headers.exists(_.name.equalsIgnoreCase(headerName)))
      }
    } finally {
      server.stop(0)
    }
  }

  test("implicit Fabric auth returns the second 401 without retrying again") {
    val port = getFreePort
    val requestCount = new AtomicInteger(0)
    val refreshCount = new AtomicInteger(0)
    val server = startServer(port) { exchange =>
      requestCount.incrementAndGet()
      readRequestBody(exchange)
      respond(exchange, 401, "unauthorized")
    }
    try {
      val client = HttpClients.createDefault()
      val (response, request) = HandlingUtils.sendWithFabricAuthRetries(
        client,
        fabricPost(port),
        Array(10, 10),
        extraCodesToRetry = Set(401),
        getAuthHeader = () => "MwcToken stale",
        refreshAuthHeader = _ => {
          refreshCount.incrementAndGet()
          "MwcToken refreshed"
        })
      val code = response.getStatusLine.getStatusCode
      response.close()
      request.releaseConnection()
      client.close()

      assert(code === 401)
      assert(requestCount.get() === 2)
      assert(refreshCount.get() === 1)
    } finally {
      server.stop(0)
    }
  }

  test("implicit Fabric auth is reacquired for a 429 retry") {
    val port = getFreePort
    val requestCount = new AtomicInteger(0)
    val authCallCount = new AtomicInteger(0)
    val authHeaders = new ConcurrentLinkedQueue[String]()
    val server = startServer(port) { exchange =>
      authHeaders.add(exchange.getRequestHeaders.getFirst("Authorization"))
      readRequestBody(exchange)
      if (requestCount.incrementAndGet() == 1) {
        respond(exchange, 429, headers = Map("Retry-After" -> "0"))
      } else {
        respond(exchange, 200, """{"ok":true}""")
      }
    }
    try {
      val client = HttpClients.createDefault()
      val (response, request) = HandlingUtils.sendWithFabricAuthRetries(
        client,
        fabricPost(port),
        Array(10),
        getAuthHeader = () => s"MwcToken token-${authCallCount.incrementAndGet()}",
        refreshAuthHeader = _ => fail("401 refresh should not run for a 429"))
      val code = response.getStatusLine.getStatusCode
      response.close()
      request.releaseConnection()
      client.close()

      assert(code === 200)
      assert(requestCount.get() === 2)
      assert(authCallCount.get() === 2)
      assert(authHeaders.asScala.toSeq === Seq("MwcToken token-1", "MwcToken token-2"))
    } finally {
      server.stop(0)
    }
  }

  test("implicit Fabric auth bounds 429 retries") {
    val port = getFreePort
    val requestCount = new AtomicInteger(0)
    val server = startServer(port) { exchange =>
      requestCount.incrementAndGet()
      readRequestBody(exchange)
      respond(exchange, 429, """{"error":{"code":"RateLimitExceeded"}}""",
        headers = Map("Retry-After" -> "0"))
    }
    try {
      val client = HttpClients.createDefault()
      val (response, request) = HandlingUtils.sendWithFabricAuthRetries(
        client,
        fabricPost(port),
        Array(0),
        getAuthHeader = () => "MwcToken current",
        refreshAuthHeader = _ => fail("401 refresh should not run for a 429"))
      val code = response.getStatusLine.getStatusCode
      response.close()
      request.releaseConnection()
      client.close()

      assert(code === 429)
      assert(requestCount.get() === 2, "Initial request plus one configured retry should be sent")
    } finally {
      server.stop(0)
    }
  }

  test("unknown-length 429 response bodies remain readable") {
    val port = getFreePort
    val responseBody = """{"error":{"code":"RateLimitExceeded"}}"""
    val server = startServer(port) { exchange =>
      exchange.sendResponseHeaders(429, 0)
      val output = exchange.getResponseBody
      output.write(responseBody.getBytes("UTF-8"))
      output.close()
      exchange.close()
    }
    try {
      val client = HttpClients.createDefault()
      val request = new HttpGet(s"http://localhost:$port/test")
      val response = HandlingUtils.sendWithRetries(client, request, Array.empty)
      val body = Source.fromInputStream(response.getEntity.getContent, "UTF-8")
      val actualBody = try {
        body.mkString
      } finally {
        body.close()
      }
      response.close()
      client.close()

      assert(actualBody === responseBody)
    } finally {
      server.stop(0)
    }
  }

  test("large unknown-length 429 response bodies remain readable") {
    val port = getFreePort
    val responseBody = "x" * (1024 * 1024 + 128)
    val server = startServer(port) { exchange =>
      exchange.sendResponseHeaders(429, 0)
      val output = exchange.getResponseBody
      output.write(responseBody.getBytes("UTF-8"))
      output.close()
      exchange.close()
    }
    try {
      val client = HttpClients.createDefault()
      val request = new HttpGet(s"http://localhost:$port/test")
      val response = HandlingUtils.sendWithRetries(client, request, Array.empty)
      val body = Source.fromInputStream(response.getEntity.getContent, "UTF-8")
      val actualBody = try {
        body.mkString
      } finally {
        body.close()
      }
      response.close()
      client.close()

      assert(actualBody === responseBody)
    } finally {
      server.stop(0)
    }
  }

  test("Fabric auth marker requires an MWC authorization header") {
    def requestData(markerValue: String, authHeader: String): HTTPRequestData = {
      val request = new HttpGet("https://workspace.fabric.microsoft.com/cognitive/openai/chat")
      request.setHeader(HTTPRequestData.FabricAuthMarkerHeader, markerValue)
      request.setHeader("Authorization", authHeader)
      new HTTPRequestData(request)
    }

    assert(requestData("true", "MwcToken token").usesFabricAuth)
    assert(!requestData("false", "MwcToken token").usesFabricAuth)
    assert(!requestData("true", "Bearer explicit").usesFabricAuth)
    assert(!requestData("true", "MwcToken ").usesFabricAuth)
  }

  test("null authorization values are treated as absent") {
    val requestData = HTTPRequestData(
      RequestLineData("GET", "https://workspace.fabric.microsoft.com/cognitive/openai/chat", None),
      Array(
        HeaderData(HTTPRequestData.FabricAuthMarkerHeader, "true"),
        HeaderData("Authorization", null)), //scalastyle:ignore null
      None)

    assert(requestData.authorizationHeader.isEmpty)
    assert(!requestData.usesFabricAuth)
  }

  test("Fabric auth retries replace duplicate authorization headers") {
    val port = getFreePort
    val authorizationHeaders = new AtomicReference[Seq[String]]()
    val server = startServer(port) { exchange =>
      authorizationHeaders.set(exchange.getRequestHeaders.get("Authorization").asScala.toSeq)
      readRequestBody(exchange)
      respond(exchange, 200, """{"ok":true}""")
    }
    try {
      val request = new HttpPost(s"http://localhost:$port/test")
      request.setHeader(HTTPRequestData.FabricAuthMarkerHeader, "true")
      request.addHeader("Authorization", "MwcToken stale")
      request.addHeader("authorization", "Bearer duplicate")
      request.setEntity(new StringEntity("""{"prompt":"hello"}""", "UTF-8"))

      val client = HttpClients.createDefault()
      val (response, replayedRequest) = HandlingUtils.sendWithFabricAuthRetries(
        client,
        new HTTPRequestData(request),
        Array.empty,
        getAuthHeader = () => "MwcToken current")
      val code = response.getStatusLine.getStatusCode
      response.close()
      replayedRequest.releaseConnection()
      client.close()

      assert(code === 200)
      assert(authorizationHeaders.get() === Seq("MwcToken current"))
    } finally {
      server.stop(0)
    }
  }

  test("untrusted marker does not replace explicit auth on a non-Fabric endpoint") {
    val port = getFreePort
    val authorization = new AtomicReference[String]()
    val server = startServer(port) { exchange =>
      authorization.set(exchange.getRequestHeaders.getFirst("Authorization"))
      respond(exchange, 200, """{"ok":true}""")
    }
    try {
      val client = HttpClients.createDefault()
      val request = new HttpGet(s"http://localhost:$port/test")
      request.setHeader(HTTPRequestData.FabricAuthMarkerHeader, "true")
      request.setHeader("Authorization", "Bearer explicit")

      val response = HandlingUtils.advanced(10)(client, new HTTPRequestData(request))

      client.close()
      assert(response.statusLine.statusCode === 200)
      assert(authorization.get() === "Bearer explicit")
    } finally {
      server.stop(0)
    }
  }

  test("Fabric endpoint validation requires HTTPS host and path containment") {
    val endpointRoot = "https://workspace.fabric.microsoft.com/cognitive/openai/"

    assert(FabricClient.isEndpointUnder(
      "https://workspace.fabric.microsoft.com//cognitive/openai/chat",
      endpointRoot))
    assert(FabricClient.isEndpointUnder(
      "https://workspace.fabric.microsoft.com:443/cognitive/openai/chat",
      endpointRoot))
    assert(!FabricClient.isEndpointUnder(
      "http://workspace.fabric.microsoft.com/cognitive/openai/chat",
      endpointRoot))
    assert(!FabricClient.isEndpointUnder(
      "https://attacker.example/cognitive/openai/chat",
      endpointRoot))
    assert(!FabricClient.isEndpointUnder(
      "https://workspace.fabric.microsoft.com/other",
      endpointRoot))
    assert(!FabricClient.isEndpointUnder(
      "https://workspace.fabric.microsoft.com/cognitive/openai/../other",
      endpointRoot))
    assert(!FabricClient.isEndpointUnder(
      "https://workspace.fabric.microsoft.com/cognitive/openai/%2e%2e/other",
      endpointRoot))
    assert(!FabricClient.isEndpointUnder(
      "https://workspace.fabric.microsoft.com/cognitive/openai/%252e%252e/other",
      endpointRoot))
    assert(!FabricClient.isEndpointUnder(
      "https://workspace.fabric.microsoft.com",
      endpointRoot))
  }
}
