// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.split1

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.http.{HTTPRequestData, HTTPSchema, HTTPTransformer, HandlingUtils,
  SingleThreadedHTTPClient}
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.apache.http.{Header, HttpEntity, HttpHost, HttpRequest, ProtocolVersion, StatusLine}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.conn.ClientConnectionManager
import org.apache.http.entity.{BasicHttpEntity, ByteArrayEntity, StringEntity}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.message.BasicHttpResponse
import org.apache.http.params.{BasicHttpParams, HttpParams}
import org.apache.http.protocol.HttpContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.{Seconds, Span}

import java.io.{IOException, InputStream, OutputStream}
import java.net.{InetSocketAddress, SocketException}
import java.nio.charset.StandardCharsets
import java.util.concurrent.{CancellationException, ConcurrentLinkedQueue, CountDownLatch, Executors,
  Semaphore, TimeUnit, TimeoutException}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Using

object VerifyBoundedRequests {
  private[split1] def client(next: () => CloseableHttpResponse): CloseableHttpClient = new CloseableHttpClient {
    override protected def doExecute(host: HttpHost, request: HttpRequest,
                                    context: HttpContext): CloseableHttpResponse = next()
    override def getParams: HttpParams = new BasicHttpParams()
    override def getConnectionManager: ClientConnectionManager = throw new UnsupportedOperationException()
    override def close(): Unit = ()
  }

  private[split1] def response(code: Int, entity: HttpEntity, onClose: () => Unit): CloseableHttpResponse = {
    val result = new BasicHttpResponse(new ProtocolVersion("HTTP", 1, 1), code, "Synthetic")
      with CloseableHttpResponse {
      override def close(): Unit = onClose()
    }
    result.setEntity(entity)
    result.setLocale(java.util.Locale.ROOT)
    result
  }
}

class VerifyBoundedRequests extends TestBase {
  import VerifyBoundedRequests.{client, response}

  private def truncatedEntity(): HttpEntity = {
    val entity = new BasicHttpEntity()
    entity.setContent(new InputStream {
      private var first = true
      override def read(): Int = {
        if (first) {
          first = false
          '{'.toInt
        } else throw new SocketException("Synthetic body reset")
      }
    })
    entity.setContentLength(100)
    entity
  }

  private def withServer(reply: HttpExchange => Unit)(body: String => Unit): Unit = {
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    val pool = Executors.newFixedThreadPool(2)
    server.setExecutor(pool)
    server.createContext("/test", (exchange: HttpExchange) => reply(exchange))
    server.start()
    try body(s"http://127.0.0.1:${server.getAddress.getPort}/test")
    finally {
      server.stop(0)
      pool.shutdownNow()
    }
  }

  private def cancelRemaining[A](results: Iterator[A]): Unit = {
    val interrupted = Thread.interrupted()
    Thread.currentThread().interrupt()
    try {
      val stopped = try !results.hasNext catch {
        case error: InterruptedException =>
          assert(error.getSuppressed.isEmpty, "HTTP worker cleanup failed")
          true
      }
      assert(stopped, "HTTP iterator did not stop after cancellation")
    } finally {
      Thread.interrupted()
      if (interrupted) Thread.currentThread().interrupt()
    }
  }

  test("already buffered HTTP responses survive the minimum concurrent timeout without waiting") {
    val worker = new AtomicReference[Thread]()
    val sent = new CountDownLatch(1)
    val reply = HTTPSchema.stringToResponse("buffered", 200, "OK")
    val handler: HandlingUtils.HandlerFunc = (_, _) => {
      worker.set(Thread.currentThread())
      sent.countDown()
      reply
    }
    Using.resource(new SingleThreadedHTTPClient(handler, 1000)) { production =>
      val request = new HTTPRequestData(new HttpPost("http://example.invalid/test"))
      val requests = Iterator(production.RequestWithContext(Some(request), None))
      val results = production.sendCancellableRequestsWithContext(requests, 1, 1.nanosecond)
      try {
        assert(results.hasNext && sent.await(5, TimeUnit.SECONDS))
        // This handler never blocks; an idle pool worker has completed the queued Future, not just its response.
        eventually(timeout(Span(5, Seconds))) {
          assert(worker.get().getState == Thread.State.WAITING)
        }
        val result = results.next().response.get
        assert(result.statusLine.statusCode == 200)
        assert(new String(result.entity.get.content, StandardCharsets.UTF_8) == "buffered")
        assert(!results.hasNext)
        worker.get().join(5000)
        assert(!worker.get().isAlive)
      } finally cancelRemaining(results)
    }
  }

  test("the minimum concurrent timeout still cancels pending HTTP work and drains its worker") {
    val worker = new AtomicReference[Thread]()
    val started = new CountDownLatch(1)
    val blocked = new CountDownLatch(1)
    val stopped = new CountDownLatch(1)
    val handler: HandlingUtils.HandlerFunc = (_, _) => {
      worker.set(Thread.currentThread())
      started.countDown()
      try {
        blocked.await()
        HTTPSchema.stringToResponse("unexpected", 200, "OK")
      } finally stopped.countDown()
    }
    Using.resource(new SingleThreadedHTTPClient(handler, 1000)) { production =>
      val request = new HTTPRequestData(new HttpPost("http://example.invalid/test"))
      val requests = Iterator(production.RequestWithContext(Some(request), None))
      val results = production.sendCancellableRequestsWithContext(requests, 1, 1.nanosecond)
      try {
        assert(results.hasNext && started.await(5, TimeUnit.SECONDS))
        val error = intercept[TimeoutException](results.next())
        assert(error.getSuppressed.isEmpty && stopped.await(5, TimeUnit.SECONDS))
        worker.get().join(5000)
        assert(!worker.get().isAlive)
      } finally {
        cancelRemaining(results)
        blocked.countDown()
      }
    }
  }

  Seq(false, true).foreach { interruptWorker =>
    val interruptedSource = if (interruptWorker) "retry workers" else "waiting callers"
    test(s"cancellable HTTP preserves interruption from $interruptedSource") {
      val worker = new AtomicReference[Thread]()
      val failure = new AtomicReference[Throwable]()
      val callerInterrupted = new AtomicBoolean()
      val workerInterrupted = new AtomicBoolean()
      val sends = new AtomicInteger()
      Using.resource(client(() => {
        sends.incrementAndGet()
        throw new IOException("Synthetic connection failure")
      })) { failingClient =>
        val handler: HandlingUtils.HandlerFunc = (_, request) => {
          worker.set(Thread.currentThread())
          try HandlingUtils.bounded(60000)(failingClient, request)
          finally workerInterrupted.set(Thread.currentThread().isInterrupted)
        }
        Using.resource(new SingleThreadedHTTPClient(handler, 1000)) { production =>
          val request = new HTTPRequestData(new HttpPost("http://example.invalid/test"))
          val requests = Iterator(production.RequestWithContext(Some(request), None))
          val results = production.sendCancellableRequestsWithContext(requests, 1, 30.seconds)
          val consumer = new Thread(() => {
            try results.next() catch {
              case error: Throwable => failure.set(error)
            } finally callerInterrupted.set(Thread.currentThread().isInterrupted)
          })
          try {
            assert(results.hasNext)
            eventually(timeout(Span(5, Seconds))) {
              assert(sends.get() == 1 && worker.get() != null)
              assert(worker.get().getStackTrace.exists(_.getMethodName == "sleep"))
            }
            consumer.start()
            eventually(timeout(Span(5, Seconds))) {
              assert(consumer.getStackTrace.exists(frame =>
                frame.getClassName == "java.util.concurrent.FutureTask" && frame.getMethodName == "get"))
              assert(consumer.getState == Thread.State.TIMED_WAITING)
            }
            if (interruptWorker) worker.get().interrupt() else consumer.interrupt()
            consumer.join(5000)
            assert(!consumer.isAlive)
            assert(failure.get().isInstanceOf[InterruptedException])
            assert(failure.get().getSuppressed.isEmpty, "HTTP worker cleanup failed")
            assert(callerInterrupted.get(), "The consumer lost its cancellation flag")
            assert(workerInterrupted.get(), "The bounded retry worker lost its cancellation flag")
            worker.get().join(5000)
            assert(!worker.get().isAlive && sends.get() == 1)
          } finally {
            consumer.interrupt()
            consumer.join(5000)
            cancelRemaining(results)
          }
        }
      }
    }
  }

  test("cancellable HTTP preserves interruption while draining a worker after timeout") {
    val release = new Semaphore(0)
    val worker = new AtomicReference[Thread]()
    val failure = new AtomicReference[Throwable]()
    val interrupted = new AtomicBoolean()
    val handler: HandlingUtils.HandlerFunc = (_, _) => {
      worker.set(Thread.currentThread())
      // Model an uncooperative request so cancellation must wait for explicit quiescence.
      release.acquireUninterruptibly()
      HTTPSchema.stringToResponse("released", 200, "OK")
    }
    Using.resource(new SingleThreadedHTTPClient(handler, 1000)) { production =>
      val request = new HTTPRequestData(new HttpPost("http://example.invalid/test"))
      val requests = Iterator(production.RequestWithContext(Some(request), None))
      val results = production.sendCancellableRequestsWithContext(requests, 1, 100.millis)
      val consumer = new Thread(() => {
        try results.next() catch {
          case error: Throwable => failure.set(error)
        } finally interrupted.set(Thread.currentThread().isInterrupted)
      })
      try {
        assert(results.hasNext)
        eventually(timeout(Span(5, Seconds))) {
          assert(worker.get() != null && worker.get().getState == Thread.State.WAITING)
        }
        consumer.start()
        eventually(timeout(Span(5, Seconds))) {
          assert(consumer.getStackTrace.exists(frame =>
            frame.getClassName == "java.util.concurrent.ThreadPoolExecutor" &&
              frame.getMethodName == "awaitTermination"))
        }
        consumer.interrupt()
        consumer.join(5000)
        assert(!consumer.isAlive && failure.get().isInstanceOf[TimeoutException])
        assert(failure.get().getSuppressed.exists(_.isInstanceOf[InterruptedException]))
        assert(interrupted.get(), "Worker draining lost the caller's cancellation flag")
      } finally {
        release.release()
        consumer.interrupt()
        consumer.join(5000)
        cancelRemaining(results)
      }
      worker.get().join(5000)
      assert(!worker.get().isAlive)
    }
  }

  test("large finite concurrent timeouts work in legacy and cancellable HTTP transforms") {
    val sends = new AtomicInteger()
    withServer { exchange =>
      sends.incrementAndGet()
      exchange.sendResponseHeaders(200, -1)
      exchange.close()
    } { address =>
      val request = HTTPRequestData.makeToRowConverter(new HTTPRequestData(new HttpPost(address)))
      val data = spark.createDataFrame(Seq(Row(request)).asJava,
        new StructType().add("request", HTTPRequestData.schema))
      Seq(1, 2).foreach { concurrency =>
        val stage = new HTTPTransformer().setInputCol("request").setOutputCol("response")
          .setConcurrency(concurrency).setConcurrentTimeout(1e20)
        Seq(stage.transform _, stage.transformCancellable _).foreach { transform =>
          val response = transform(data).head().getAs[Row]("response")
          assert(response.getAs[Row]("statusLine").getAs[Int]("statusCode") == 200)
        }
      }
      assert(sends.get() == 4)
    }
  }

  test("bounded policy respects Retry-After seconds and HTTP dates without retrying long hints early") {
    val now = 1700000000000L
    val date = org.apache.http.client.utils.DateUtils.formatDate(new java.util.Date(now + 2000))
    assert(HandlingUtils.retryAfterMillis("2", now).contains(2000L))
    assert(HandlingUtils.retryAfterMillis("1.0005", now).contains(1001L))
    assert(HandlingUtils.retryAfterMillis("0.0005", now).contains(1L))
    assert(HandlingUtils.retryAfterMillis("1e-1000000", now).contains(1L))
    assert(HandlingUtils.retryAfterMillis("0", now).contains(0L))
    assert(HandlingUtils.retryAfterMillis(date, now).contains(2000L))
    assert(HandlingUtils.retryAfterMillis("-1", now).isEmpty)
    assert(HandlingUtils.retryAfterMillis("not a date", now).isEmpty)
    Seq("120", "120.0005", "60.0005", "9223372036854776",
      "9999999999999999999999999999999999999999").foreach { hint =>
      Seq(429, 503).foreach { code =>
        val sends = new AtomicInteger()
        withServer { exchange =>
          sends.incrementAndGet()
          exchange.getResponseHeaders.add("Retry-After", hint)
          exchange.sendResponseHeaders(code, -1)
          exchange.close()
        } { address =>
          Using.resource(HttpClients.custom().disableAutomaticRetries().build()) { client =>
            val result = HandlingUtils.bounded(0, 0)(client, new HTTPRequestData(new HttpPost(address)))
            assert(result.statusLine.statusCode == code)
            assert(sends.get() == 1, s"Long Retry-After $hint must not trigger another send for HTTP $code")
          }
        }
      }
    }
  }

  test("bounded fractional Retry-After waits until at least the next whole millisecond") {
    Seq(429, 503).foreach { code =>
      val sends = new ConcurrentLinkedQueue[Long]()
      withServer { exchange =>
        sends.add(System.nanoTime())
        val first = sends.size() == 1
        exchange.getResponseHeaders.add("Connection", "close")
        if (first) exchange.getResponseHeaders.add("Retry-After", "1.0005")
        exchange.sendResponseHeaders(if (first) code else 200, -1)
        exchange.close()
      } { address =>
        Using.resource(HttpClients.custom().disableAutomaticRetries().build()) { client =>
          Using.resource(HandlingUtils.sendWithRetryPolicy(client, new HttpPost(address), Array(0),
            Set.empty, 0, boundedPolicy = true, quiet = false)) { result =>
            assert(result.getStatusLine.getStatusCode == 200)
          }
          val times = sends.asScala.toVector
          assert(times.size == 2)
          assert(times(1) - times.head >= TimeUnit.MILLISECONDS.toNanos(1001))
        }
      }
    }
  }

  test("bounded connection failures consume exactly the configured retry budget") {
    val sends = new AtomicInteger()
    withServer { exchange =>
      sends.incrementAndGet()
      exchange.close()
    } { address =>
      Using.resource(HttpClients.custom().disableAutomaticRetries().build()) { client =>
        val result = HandlingUtils.bounded(0, 0, 0)(client, new HTTPRequestData(new HttpPost(address)))
        assert(result == null)
        assert(sends.get() == 4, "Recursive IOException propagation must not restart exhausted retry budgets")
      }
    }
  }

  test("bounded rejected response read failures preserve status and never resend terminal errors") {
    Seq(400, 401, 403, 404, 413, 429, 503).foreach { code =>
      Seq("body", "firstHeader", "headers").foreach { fault =>
        val sends = new AtomicInteger()
        val closes = new AtomicInteger()
        Using.resource(client(() => {
          sends.incrementAndGet()
          val result = new BasicHttpResponse(new ProtocolVersion("HTTP", 1, 1), code, "Synthetic")
            with CloseableHttpResponse {
            override def getFirstHeader(name: String): Header = {
              if (fault == "firstHeader") throw new IOException("Synthetic retry header failure")
              super.getFirstHeader(name)
            }
            override def getAllHeaders: Array[Header] = {
              if (fault == "headers") throw new IOException("Synthetic header failure")
              super.getAllHeaders
            }
            override def close(): Unit = { closes.incrementAndGet(); () }
          }
          result.setEntity(truncatedEntity())
          result.setLocale(java.util.Locale.ROOT)
          result
        })) { http =>
          val result = HandlingUtils.bounded(0, 0)(http,
            new HTTPRequestData(new HttpPost("http://example.invalid/test")))
          val expected = if (code == 429 || code == 503) 3 else 1
          assert(sends.get() == expected && closes.get() == expected, s"Unexpected retries for HTTP $code")
          assert(result != null && result.statusLine.statusCode == code)
          val diagnostic = new String(result.entity.get.content, StandardCharsets.UTF_8)
          assert(diagnostic.contains("Could not read") && !diagnostic.contains("Synthetic body reset"))
        }
      }
    }
  }

  test("bounded truncated responses retain short Retry-After hints and share the retry budget") {
    Seq(429, 503).foreach { code =>
      Seq(0, 1, 2).foreach { retries =>
        val sends = new ConcurrentLinkedQueue[Long]()
        val closes = new AtomicInteger()
        Using.resource(client(() => {
          sends.add(System.nanoTime())
          val failing = sends.size() <= 2
          val result = response(if (failing) code else 200,
            if (failing) truncatedEntity() else new StringEntity("complete"),
            () => { closes.incrementAndGet(); () })
          if (failing) result.setHeader("Retry-After", "0.05")
          result
        })) { http =>
          val result = HandlingUtils.bounded(Array.fill(retries)(0): _*)(http,
            new HTTPRequestData(new HttpPost("http://example.invalid/test")))
          assert(result != null && result.statusLine.statusCode == (if (retries == 2) 200 else code))
          val times = sends.asScala.toVector
          assert(times.size == retries + 1 && closes.get() == retries + 1)
          times.sliding(2).filter(_.size == 2).foreach { pair =>
            assert(pair(1) - pair.head >= TimeUnit.MILLISECONDS.toNanos(50))
          }
        }
      }
    }
  }

  test("bounded truncated responses never retry Retry-After hints above the cap") {
    val future = org.apache.http.client.utils.DateUtils.formatDate(
      new java.util.Date(System.currentTimeMillis() + 120000))
    Seq(429, 503).foreach { code =>
      Seq("120", "60.0005", future).foreach { hint =>
        val sends = new AtomicInteger()
        val closes = new AtomicInteger()
        Using.resource(client(() => {
          sends.incrementAndGet()
          val result = response(code, truncatedEntity(), () => { closes.incrementAndGet(); () })
          result.setHeader("Retry-After", hint)
          result
        })) { http =>
          val result = HandlingUtils.bounded(0, 0)(http,
            new HTTPRequestData(new HttpPost("http://example.invalid/test")))
          assert(sends.get() == 1 && closes.get() == 1)
          assert(result != null && result.statusLine.statusCode == code)
          val diagnostic = new String(result.entity.get.content, StandardCharsets.UTF_8)
          assert(diagnostic.contains("Could not read") && !diagnostic.contains("Synthetic body reset"))
        }
      }
    }
  }

  test("production cancellable clients own the complete write-failure retry budget") {
    Seq(0, 2).foreach { retries =>
      val writes = new AtomicInteger()
      withServer { exchange =>
        exchange.sendResponseHeaders(200, -1)
        exchange.close()
      } { address =>
        val handler: HandlingUtils.HandlerFunc = (http, data) => {
          val request = new HttpPost(data.requestLine.uri)
          request.setEntity(new ByteArrayEntity(Array[Byte](1)) {
            override def writeTo(output: OutputStream): Unit = {
              writes.incrementAndGet()
              throw new SocketException("Synthetic request write failure")
            }
          })
          try HandlingUtils.convertAndClose(HandlingUtils.sendWithRetryPolicy(
            http, request, Array.fill(retries)(0), Set.empty, 0, boundedPolicy = true, quiet = true))
          finally request.releaseConnection()
        }
        Using.resource(new SingleThreadedHTTPClient(handler, 1000)) { production =>
          val data = new HTTPRequestData(new HttpPost(address))
          def requests: Iterator[production.RequestWithContext] =
            Iterator(production.RequestWithContext(Some(data), None))
          intercept[IOException] {
            production.sendCancellableRequestsWithContext(requests, 1, 5.seconds).toVector
          }
          assert(writes.get() == retries + 1, "Apache retries must not multiply the bounded send budget")
          writes.set(0)
          intercept[IOException](production.sendRequestsWithContext(requests).toVector)
          assert(writes.get() == (retries + 1) * 4, "Legacy Apache retry behavior must remain unchanged")
        }
      }
    }
  }

  test("bounded metadata failures preserve buffered bodies and capacity retry classification") {
    Seq("encoding", "type", "chunked").foreach { fault =>
      Seq(429 -> "CapacityLimitExceeded", 429 -> "transient", 503 -> "unavailable").foreach { case (code, body) =>
        val sends = new AtomicInteger()
        val closes = new AtomicInteger()
        Using.resource(client(() => {
          sends.incrementAndGet()
          val entity = new StringEntity(body) {
            override def getContentEncoding: Header =
              if (fault == "encoding") throw new IOException("Synthetic metadata failure")
              else super.getContentEncoding
            override def getContentType: Header =
              if (fault == "type") throw new IOException("Synthetic metadata failure")
              else super.getContentType
            override def isChunked: Boolean =
              if (fault == "chunked") throw new IOException("Synthetic metadata failure")
              else super.isChunked
          }
          val result = response(code, entity, () => { closes.incrementAndGet(); () })
          result.setHeader("Retry-After", "0")
          result
        })) { http =>
          val result = HandlingUtils.bounded(0, 0)(http,
            new HTTPRequestData(new HttpPost("http://example.invalid/test")))
          assert(result != null && result.statusLine.statusCode == code)
          assert(new String(result.entity.get.content, StandardCharsets.UTF_8) == body)
          val expectedSends = if (body == "CapacityLimitExceeded") 1 else 3
          assert(sends.get() == expectedSends && closes.get() == expectedSends)
        }
      }
    }
  }

  test("production cancellable clients do not follow cross-origin redirects or forward credentials") {
    Seq(0, 2).foreach { retries =>
      val sends = new AtomicInteger()
      val targetHeaders = new ConcurrentLinkedQueue[Map[String, String]]()
      withServer { exchange =>
        targetHeaders.add(exchange.getRequestHeaders.asScala.map { case (name, values) =>
          name.toLowerCase(java.util.Locale.ROOT) -> values.asScala.mkString(",")
        }.toMap)
        exchange.sendResponseHeaders(200, -1)
        exchange.close()
      } { destination =>
        withServer { exchange =>
          sends.incrementAndGet()
          Using.resource(exchange.getRequestBody)(_.readAllBytes())
          exchange.getResponseHeaders.add("Location", destination)
          exchange.sendResponseHeaders(303, -1)
          exchange.close()
        } { address =>
          val handler: HandlingUtils.HandlerFunc = (http, request) =>
            HandlingUtils.bounded(Array.fill(retries)(0): _*)(http, request)
          Using.resource(new SingleThreadedHTTPClient(handler, 1000)) { production =>
            val post = new HttpPost(address)
            post.setHeader("api-key", "synthetic-redirect-key")
            post.setHeader("Authorization", "Bearer synthetic-redirect-token")
            post.setHeader("X-Custom-Auth", "synthetic-custom-auth")
            val data = new HTTPRequestData(post)
            def requests: Iterator[production.RequestWithContext] =
              Iterator(production.RequestWithContext(Some(data), None))
            Seq(1, 2).foreach { concurrency =>
              val before = sends.get()
              val result = production.sendCancellableRequestsWithContext(requests, concurrency, 5.seconds)
                .toVector.head.response.get
              assert(result != null, s"Expected redirect response: retries=$retries, " +
                s"concurrency=$concurrency, sends=${sends.get() - before}")
              assert(targetHeaders.isEmpty, "Redirect destinations must receive no requests or credentials")
              assert(result.statusLine.statusCode == 303)
              assert(sends.get() - before == 1, "Redirect responses must not repeat the original request")
            }
            val legacy = production.sendRequestsWithContext(requests).toVector.head.response.get
            assert(legacy.statusLine.statusCode == 200 && targetHeaders.size() == 1,
              "Legacy automatic redirect behavior must remain unchanged")
          }
        }
      }
    }
  }

  test("bounded capacity metadata failures remain terminal without a usable Retry-After") {
    Seq(None, Some("unusable")).foreach { hint =>
      val sends = new AtomicInteger()
      val closes = new AtomicInteger()
      Using.resource(client(() => {
        sends.incrementAndGet()
        val result = response(429, new StringEntity("CapacityLimitExceeded") {
          override def getContentType: Header = throw new IOException("Synthetic metadata failure")
        }, () => { closes.incrementAndGet(); () })
        hint.foreach(result.setHeader("Retry-After", _))
        result
      })) { http =>
        val result = HandlingUtils.bounded(0, 0)(http,
          new HTTPRequestData(new HttpPost("http://example.invalid/test")))
        assert(result != null && result.statusLine.statusCode == 429)
        assert(new String(result.entity.get.content, StandardCharsets.UTF_8) == "CapacityLimitExceeded")
        assert(sends.get() == 1 && closes.get() == 1)
      }
    }
  }

  test("bounded policy returns every redirect status without repeating accepted requests") {
    Seq(300, 301, 302, 303, 304, 305, 306, 307, 308, 399).foreach { code =>
      val sends = new AtomicInteger()
      val closes = new AtomicInteger()
      Using.resource(client(() => {
        sends.incrementAndGet()
        response(code, new StringEntity("see other"), () => { closes.incrementAndGet(); () })
      })) { http =>
        val result = HandlingUtils.bounded(0, 0)(http, new HTTPRequestData(new HttpPost("http://example.invalid/test")))
        assert(result.statusLine.statusCode == code)
        assert(sends.get() == 1 && closes.get() == 1, s"HTTP $code must not replay the original POST")
      }
    }
  }

  test("bounded redirect read failures preserve terminal status without replaying") {
    Seq(300, 303, 399).foreach { code =>
      Seq("body", "headers", "metadata").foreach { fault =>
        val sends = new AtomicInteger()
        val closes = new AtomicInteger()
        Using.resource(client(() => {
          sends.incrementAndGet()
          val result = new BasicHttpResponse(new ProtocolVersion("HTTP", 1, 1), code, "Synthetic")
            with CloseableHttpResponse {
            override def getAllHeaders: Array[Header] = {
              if (fault == "headers") throw new IOException("Synthetic header failure")
              super.getAllHeaders
            }
            override def close(): Unit = { closes.incrementAndGet(); () }
          }
          result.setEntity(if (fault == "body") truncatedEntity() else new StringEntity("see other") {
            override def getContentType: Header = throw new IOException("Synthetic metadata failure")
          })
          result.setLocale(java.util.Locale.ROOT)
          result
        })) { http =>
          val result = HandlingUtils.bounded(0, 0)(http,
            new HTTPRequestData(new HttpPost("http://example.invalid/test")))
          assert(sends.get() == 1 && closes.get() == 1, s"HTTP $code with $fault failure must not replay")
          assert(result != null && result.statusLine.statusCode == code)
          val diagnostic = new String(result.entity.get.content, StandardCharsets.UTF_8)
          if (fault == "metadata") assert(diagnostic == "see other")
          else assert(diagnostic.contains("Could not read") && !diagnostic.contains("Synthetic"))
        }
      }
    }
  }

  test("bounded policy accepts every successful HTTP status without repeating completed requests") {
    Seq(203, 204, 206, 299).foreach { code =>
      val sends = new AtomicInteger()
      val closes = new AtomicInteger()
      Using.resource(client(() => {
        sends.incrementAndGet()
        response(code, new StringEntity("completed"), () => { closes.incrementAndGet(); () })
      })) { http =>
        val result = HandlingUtils.bounded(0, 0)(http, new HTTPRequestData(new HttpPost("http://example.invalid/test")))
        assert(result.statusLine.statusCode == code)
        assert(new String(result.entity.get.content, StandardCharsets.UTF_8) == "completed")
        assert(sends.get() == 1 && closes.get() == 1)
      }
    }
  }

  test("bounded policy propagates cancellation before performing HTTP") {
    Using.resource(HttpClients.createDefault()) { client =>
      try {
        Thread.currentThread().interrupt()
        intercept[InterruptedException] {
          HandlingUtils.bounded(0)(client, new HTTPRequestData(new HttpPost("http://127.0.0.1:1/cancelled")))
        }
      } finally Thread.interrupted()
    }
  }

  test("bounded Fabric refresh consumes the same retry budget and never refreshes twice") {
    val sends = new AtomicInteger()
    val refreshes = new AtomicInteger()
    val auth = new ConcurrentLinkedQueue[String]()
    withServer { exchange =>
      auth.add(exchange.getRequestHeaders.getFirst("Authorization"))
      sends.incrementAndGet()
      exchange.sendResponseHeaders(401, -1)
      exchange.close()
    } { address =>
      Using.resource(HttpClients.custom().disableAutomaticRetries().build()) { client =>
        val request = new HttpPost(address)
        request.setHeader("Authorization", "MwcToken stale")
        try {
          val response = HandlingUtils.sendWithRetryPolicy(client, request, Array(0, 0, 0), Set.empty, 0,
            boundedPolicy = true, quiet = true, refreshAuth = Some { rejected =>
              assert(rejected == "MwcToken stale")
              refreshes.incrementAndGet()
              "MwcToken fresh"
            })
          try assert(response.getStatusLine.getStatusCode == 401) finally response.close()
          assert(sends.get() == 2 && refreshes.get() == 1)
          assert(auth.asScala.toSeq == Seq("MwcToken stale", "MwcToken fresh"))
        } finally request.releaseConnection()
      }
    }
  }

  test("bounded body failures share the status retry budget and preserve the eventual response") {
    val sends = new AtomicInteger()
    val closes = new AtomicInteger()
    Using.resource(client(() => {
      val attempt = sends.incrementAndGet()
      val entity = if (attempt == 2) truncatedEntity() else new StringEntity("complete")
      response(if (attempt == 1) 503 else 200, entity, () => { closes.incrementAndGet(); () })
    })) { http =>
      val result = HandlingUtils.bounded(0, 0)(http, new HTTPRequestData(new HttpPost("http://example.invalid/test")))
      assert(result.statusLine.statusCode == 200)
      assert(new String(result.entity.get.content, StandardCharsets.UTF_8) == "complete")
      assert(sends.get() == 3 && closes.get() == 3)
    }

  }

  test("bounded Fabric refresh failures retain the terminal rejection without replaying") {
    Seq(new NoSuchMethodException("Synthetic missing token method"),
      new IllegalStateException("Synthetic token failure"), new IOException("Synthetic refresh failure"))
      .foreach { failure =>
        val sends = new AtomicInteger()
        val closes = new AtomicInteger()
        Using.resource(client(() => {
          sends.incrementAndGet()
          response(401, new StringEntity("rejected"), () => { closes.incrementAndGet(); () })
        })) { http =>
          val request = new HttpPost("http://example.invalid/test")
          try {
            val result = HandlingUtils.convertAndClose(HandlingUtils.sendWithRetryPolicy(
              http, request, Array(0, 0), Set.empty, 0, boundedPolicy = true, quiet = true,
              refreshAuth = Some(_ => throw failure)))
            assert(result.statusLine.statusCode == 401)
            assert(new String(result.entity.get.content, StandardCharsets.UTF_8) == "rejected")
            assert(sends.get() == 1 && closes.get() == 1)
          } finally request.releaseConnection()
        }
      }
  }

  test("bounded Fabric refresh preserves interruption and cancellation") {
    Seq(new InterruptedException("Synthetic interruption"), new CancellationException("Synthetic cancellation"))
      .foreach { failure =>
        val sends = new AtomicInteger()
        val closes = new AtomicInteger()
        Using.resource(client(() => {
          sends.incrementAndGet()
          response(401, new StringEntity("rejected"), () => { closes.incrementAndGet(); () })
        })) { http =>
          val request = new HttpPost("http://example.invalid/test")
          try {
            val observed = intercept[Exception] {
              HandlingUtils.sendWithRetryPolicy(http, request, Array(0, 0), Set.empty, 0,
                boundedPolicy = true, quiet = true, refreshAuth = Some(_ => throw failure))
            }
            assert(observed eq failure)
            assert(sends.get() == 1 && closes.get() == 1)
          } finally request.releaseConnection()
        }
      }
  }

  test("bounded exhausted body resets return transport failure rather than an empty successful response") {
    val sends = new AtomicInteger()
    val closes = new AtomicInteger()
    Using.resource(client(() => {
      sends.incrementAndGet()
      response(200, truncatedEntity(), () => { closes.incrementAndGet(); () })
    })) { http =>
      val result = HandlingUtils.bounded(0, 0)(http, new HTTPRequestData(new HttpPost("http://example.invalid/test")))
      assert(result == null && sends.get() == 3 && closes.get() == 3)
    }
  }

  test("bounded response metadata IO failures close the attempt and consume one retry") {
    Seq("status", "headers").foreach { fault =>
      val sends = new AtomicInteger()
      val closes = new AtomicInteger()
      Using.resource(client(() => {
        val attempt = sends.incrementAndGet()
        val result = new BasicHttpResponse(new ProtocolVersion("HTTP", 1, 1), 200, "Synthetic")
          with CloseableHttpResponse {
          override def getStatusLine: StatusLine = {
            if (attempt == 1 && fault == "status") throw new IOException("Synthetic status failure")
            super.getStatusLine
          }
          override def getAllHeaders: Array[Header] = {
            if (attempt == 1 && fault == "headers") throw new IOException("Synthetic header failure")
            super.getAllHeaders
          }
          override def close(): Unit = { closes.incrementAndGet(); () }
        }
        result.setEntity(new StringEntity("complete"))
        result.setLocale(java.util.Locale.ROOT)
        result
      })) { http =>
        val result = HandlingUtils.bounded(0)(http, new HTTPRequestData(new HttpPost("http://example.invalid/test")))
        assert(result != null && new String(result.entity.get.content, StandardCharsets.UTF_8) == "complete")
        assert(sends.get() == 2 && closes.get() == 2)
      }
    }
  }

  test("bounded close IO failure does not discard a fully materialized answer or repeat the request") {
    val sends = new AtomicInteger()
    val closes = new AtomicInteger()
    Using.resource(client(() => {
      sends.incrementAndGet()
      response(200, new StringEntity("complete"), () => {
        closes.incrementAndGet()
        throw new IOException("Synthetic close failure")
      })
    })) { http =>
      val result = HandlingUtils.bounded(0)(http, new HTTPRequestData(new HttpPost("http://example.invalid/test")))
      assert(result != null && new String(result.entity.get.content, StandardCharsets.UTF_8) == "complete")
      assert(sends.get() == 1 && closes.get() == 1)
    }
  }
}
