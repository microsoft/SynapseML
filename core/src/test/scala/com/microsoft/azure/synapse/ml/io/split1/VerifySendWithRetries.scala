// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.split1

import com.microsoft.azure.synapse.ml.io.http.HandlingUtils

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients

import java.net.{InetSocketAddress, ServerSocket}
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

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
}
