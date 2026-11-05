// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.split1

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.http.{HTTPRequestData, HTTPResponseData, HTTPTransformer, HandlingUtils}
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.client.utils.DateUtils
import org.apache.http.entity.StringEntity
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.{Seconds, Span}

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.{ConcurrentLinkedQueue, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.collection.JavaConverters._
import scala.util.Using

class VerifyFabricRetryAfter extends TestBase {
  import VerifyBoundedRequests.{client, response}

  test("bounded responses reject oversized Retry-After hints before expensive numeric parsing") {
    val sends = new AtomicInteger()
    val closes = new AtomicInteger()
    val hint = "9" * (1024 * 1024)
    Using.resource(client(() => {
      val first = sends.incrementAndGet() == 1
      val result = response(if (first) 503 else 200, new StringEntity("result"),
        () => { closes.incrementAndGet(); () })
      if (first) result.setHeader("Retry-After", hint)
      result
    })) { http =>
      val request = new HTTPRequestData(new HttpPost("http://example.invalid/test"))
      val start = System.nanoTime()
      val result = HandlingUtils.bounded(25)(http, request)
      val elapsed = System.nanoTime() - start
      assert(result.statusLine.statusCode == 200 && sends.get() == 2 && closes.get() == 2)
      assert(elapsed >= TimeUnit.MILLISECONDS.toNanos(25) && elapsed < TimeUnit.SECONDS.toNanos(5))
      assert(HandlingUtils.retryAfterMillis("0." + ("0" * 61) + "1", 0).contains(1L))
      assert(HandlingUtils.retryAfterMillis("0." + ("0" * 62) + "1", 0).isEmpty)
    }
  }

  test("authentication callbacks require the bounded retry policy before any request is sent") {
    val sends = new AtomicInteger()
    val refreshes = new AtomicInteger()
    Using.resource(client(() => {
      sends.incrementAndGet()
      response(401, new StringEntity("rejected"), () => ())
    })) { http =>
      val request = new HttpPost("http://example.invalid/test")
      try {
        val error = intercept[IllegalArgumentException] {
          HandlingUtils.sendWithRetryPolicy(http, request, Array(0), Set.empty, 0,
            boundedPolicy = false, quiet = true, refreshAuth = Some { _ =>
              refreshes.incrementAndGet()
              throw new IOException("Synthetic refresh failure")
            })
        }
        assert(error.getMessage.contains("Authentication replay requires the bounded retry policy"))
        assert(sends.get() == 0 && refreshes.get() == 0)
      } finally request.releaseConnection()
    }
  }

  test("bounded Fabric refresh honors fractional Retry-After within the shared retry budget") {
    val times = new ConcurrentLinkedQueue[Long]()
    val refreshes = new AtomicInteger()
    val closes = new AtomicInteger()
    Using.resource(client(() => {
      times.add(System.nanoTime())
      val first = times.size() == 1
      val result = response(if (first) 401 else 503, new StringEntity("rejected"),
        () => { closes.incrementAndGet(); () })
      if (first) result.setHeader("Retry-After", "1.0005")
      result
    })) { http =>
      val request = new HttpPost("http://example.invalid/test")
      request.setHeader("Authorization", "MwcToken stale")
      try {
        val result = HandlingUtils.convertAndClose(HandlingUtils.sendWithRetryPolicy(
          http, request, Array(0), Set.empty, 0, boundedPolicy = true, quiet = true,
          refreshAuth = Some { rejected =>
            assert(rejected == "MwcToken stale")
            refreshes.incrementAndGet()
            "MwcToken fresh"
          }))
        val attempts = times.asScala.toVector
        assert(attempts.size == 2 && refreshes.get() == 1 && closes.get() == 2)
        assert(attempts(1) - attempts.head >= TimeUnit.MILLISECONDS.toNanos(1001))
        assert(result.statusLine.statusCode == 503, "Authentication replay consumes the only retry")
      } finally request.releaseConnection()
    }
  }

  test("bounded Fabric refresh keeps long Retry-After rejections without changing legacy replay") {
    Seq("60.0001", "120", DateUtils.formatDate(new java.util.Date(System.currentTimeMillis() + 120000)))
      .foreach { hint =>
        Seq(false, true).foreach { bounded =>
          val sends = new AtomicInteger()
          val refreshes = new AtomicInteger()
          val closes = new AtomicInteger()
          Using.resource(client(() => {
            val first = sends.incrementAndGet() == 1
            val result = response(if (first) 401 else 200, new StringEntity(if (first) "rejected" else "retried"),
              () => { closes.incrementAndGet(); () })
            if (first) result.setHeader("Retry-After", hint)
            result
          })) { http =>
            val request = new HttpPost("http://example.invalid/test")
            try {
              val refresh: String => String = _ => {
                refreshes.incrementAndGet()
                "MwcToken fresh"
              }
              val (received, sent) = if (bounded) {
                HandlingUtils.sendWithRetryPolicy(http, request, Array(0), Set.empty, 0,
                  boundedPolicy = true, quiet = true, refreshAuth = Some(refresh)) -> request
              } else {
                HandlingUtils.sendWithFabricAuthRetries(http, new HTTPRequestData(request), Array(0),
                  getAuthHeader = () => "MwcToken stale", refreshAuthHeader = refresh)
              }
              try {
                val result = HandlingUtils.convertAndClose(received)
                if (bounded) assert(Option(received.getFirstHeader("Retry-After")).exists(_.getValue == hint))
                assert(result.statusLine.statusCode == (if (bounded) 401 else 200))
                assert(new String(result.entity.get.content, StandardCharsets.UTF_8) ==
                  (if (bounded) "rejected" else "retried"))
                assert(sends.get() == (if (bounded) 1 else 2) && closes.get() == sends.get())
                assert(refreshes.get() == (if (bounded) 0 else 1))
              } finally sent.releaseConnection()
            } finally request.releaseConnection()
          }
        }
      }
  }

  test("cancellable HTTP dispatch gates Fabric refresh and honors server hints on the real transport") {
    val cases = Seq(
      ("120", true, true, "MwcToken stale", false),
      ("0.05", true, true, "MwcToken stale", true),
      ("0.05", false, true, "MwcToken stale", false),
      ("0.05", true, false, "MwcToken stale", false),
      ("0.05", true, true, "Bearer explicit", false))
    val budgets = cases.map { case (hint, marked, trusted, authorization, replay) =>
      (hint, marked, trusted, authorization, Seq(0, 0), replay)
    } :+ ("0.05", true, true, "MwcToken stale", Seq.empty[Int], false)
    budgets.foreach { case (hint, marked, trusted, authorization, budget, replay) =>
      val times = new ConcurrentLinkedQueue[Long]()
      val authHeaders = new ConcurrentLinkedQueue[String]()
      val refreshHeaders = new ConcurrentLinkedQueue[String]()
      val cookies = new ConcurrentLinkedQueue[String]()
      val markers = new ConcurrentLinkedQueue[String]()
      val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
      val pool = Executors.newFixedThreadPool(2)
      server.setExecutor(pool)
      server.createContext("/", (exchange: HttpExchange) => {
        IOUtils.toByteArray(exchange.getRequestBody)
        exchange.getRequestBody.close()
        val (code, body) = if (exchange.getRequestURI.getPath == "/refresh") {
          refreshHeaders.add(exchange.getRequestHeaders.getFirst("Authorization"))
          200 -> "MwcToken fresh"
        } else {
          times.add(System.nanoTime())
          authHeaders.add(exchange.getRequestHeaders.getFirst("Authorization"))
          Option(exchange.getRequestHeaders.getFirst("Cookie")).foreach(cookies.add)
          Option(exchange.getRequestHeaders.getFirst(HTTPRequestData.FabricAuthMarkerHeader)).foreach(markers.add)
          if (times.size() == 1) {
            exchange.getResponseHeaders.add("Retry-After", hint)
            exchange.getResponseHeaders.add("Set-Cookie", "synthetic=1")
            401 -> "rejected"
          } else 200 -> "accepted"
        }
        val bytes = body.getBytes(StandardCharsets.UTF_8)
        try {
          exchange.sendResponseHeaders(code, bytes.length)
          exchange.getResponseBody.write(bytes)
        } finally exchange.close()
      })
      server.start()
      try {
        val address = s"http://127.0.0.1:${server.getAddress.getPort}/test"
        val refreshAddress = s"http://127.0.0.1:${server.getAddress.getPort}/refresh"
        val request = new HttpPost(address)
        request.setHeader("Authorization", authorization)
        if (marked) request.setHeader(HTTPRequestData.FabricAuthMarkerHeader, "true")
        val input = HTTPRequestData.makeToRowConverter(new HTTPRequestData(request))
        request.releaseConnection()
        val data = spark.createDataFrame(Seq(Row(input)).asJava,
          new StructType().add("request", HTTPRequestData.schema))
        val stage = new HTTPTransformer().setInputCol("request").setOutputCol("response")
          .setConcurrency(1).setConcurrentTimeout(10)
        stage.setHandler { (http, requestData) =>
          HandlingUtils.boundedWithFabricAuth(budget,
            uri => trusted && uri == address,
            rejected => {
              val refreshRequest = new HttpGet(refreshAddress)
              refreshRequest.setHeader("Authorization", rejected)
              try {
                val result = HandlingUtils.convertAndClose(http.execute(refreshRequest))
                new String(result.entity.get.content, StandardCharsets.UTF_8)
              } finally refreshRequest.releaseConnection()
            })(http, requestData)
        }
        val result = HTTPResponseData.makeFromRowConverter(
          stage.transformCancellable(data).head().getAs[Row]("response"))
        val attempts = times.asScala.toVector
        assert(result.statusLine.statusCode == (if (replay) 200 else 401))
        assert(new String(result.entity.get.content, StandardCharsets.UTF_8) ==
          (if (replay) "accepted" else "rejected"))
        assert(attempts.size == (if (replay) 2 else 1))
        assert(refreshHeaders.asScala.toVector == (if (replay) Vector("MwcToken stale") else Vector.empty))
        assert(authHeaders.asScala.toVector ==
          (if (replay) Vector("MwcToken stale", "MwcToken fresh") else Vector(authorization)))
        assert(cookies.isEmpty && markers.isEmpty)
        if (replay) assert(attempts(1) - attempts.head >= TimeUnit.MILLISECONDS.toNanos(50))
        else assert(result.headers.exists(h => h.name.equalsIgnoreCase("Retry-After") && h.value == hint))
      } finally {
        server.stop(0)
        pool.shutdownNow()
      }
    }
  }

  test("bounded Fabric refresh can be cancelled during Retry-After without refreshing or replaying") {
    val sends = new AtomicInteger()
    val refreshes = new AtomicInteger()
    val closes = new AtomicInteger()
    val failure = new AtomicReference[Throwable]()
    Using.resource(client(() => {
      val first = sends.incrementAndGet() == 1
      val result = response(if (first) 401 else 200, new StringEntity("rejected"),
        () => { closes.incrementAndGet(); () })
      if (first) result.setHeader("Retry-After", "30")
      result
    })) { http =>
      val request = new HttpPost("http://example.invalid/test")
      val caller = new Thread(() => {
        try {
          val result = HandlingUtils.sendWithRetryPolicy(http, request, Array(0), Set.empty, 0,
            boundedPolicy = true, quiet = true, refreshAuth = Some { _ =>
              refreshes.incrementAndGet()
              "MwcToken fresh"
            })
          result.close()
        } catch {
          case error: Throwable => failure.set(error)
        }
      })
      caller.start()
      try {
        eventually(timeout(Span(5, Seconds))) {
          assert(caller.getState == Thread.State.TIMED_WAITING)
        }
        caller.interrupt()
        caller.join(5000)
        assert(!caller.isAlive && failure.get().isInstanceOf[InterruptedException])
        assert(sends.get() == 1 && refreshes.get() == 0 && closes.get() == 1)
      } finally {
        caller.interrupt()
        caller.join(5000)
        request.releaseConnection()
        assert(!caller.isAlive, "The cancelled authentication wait must not retain its worker")
      }
    }
  }
}
