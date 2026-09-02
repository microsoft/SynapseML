// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.http

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.sun.net.httpserver.HttpServer
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.AbstractHttpEntity

import java.io.{IOException, InputStream, OutputStream}
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets

// scalastyle:off magic.number
class VerifyRESTHelpers extends TestBase {

  test("retry succeeds on first try with empty backoffs") {
    val result = RESTHelpers.retry(List.empty[Int], () => 42)
    assert(result === 42)
  }

  test("retry succeeds on first try with non-empty backoffs") {
    val result = RESTHelpers.retry(List(0, 0), () => "ok")
    assert(result === "ok")
  }

  test("retry retries on failure and eventually succeeds") {
    var attempts = 0
    val result = Console.withOut(new java.io.ByteArrayOutputStream()) {
      // Zero backoffs exercise the same retry path without a real Thread.sleep.
      RESTHelpers.retry(List(0, 0, 0), () => {
        attempts += 1
        if (attempts < 3) throw new RuntimeException("fail")
        "success"
      })
    }
    assert(result === "success")
    assert(attempts === 3)
  }

  test("retry throws when all retries exhausted") {
    Console.withOut(new java.io.ByteArrayOutputStream()) {
      intercept[RuntimeException] {
        RESTHelpers.retry(List(0), () => throw new RuntimeException("always fails"))
      }
    }
  }

  test("retry with empty backoff list throws immediately") {
    intercept[RuntimeException] {
      RESTHelpers.retry(List.empty[Int], () => throw new RuntimeException("immediate"))
    }
  }

  test("HTTP failure remains visible when the request body cannot be read") {
    val responseBody = "upstream failure".getBytes(StandardCharsets.UTF_8)
    val server = HttpServer.create(new InetSocketAddress("localhost", 0), 0)
    server.createContext("/failure", exchange => {
      exchange.sendResponseHeaders(500, responseBody.length)
      val output = exchange.getResponseBody
      try {
        output.write(responseBody)
      } finally {
        output.close()
      }
    })
    server.start()

    try {
      val request = new HttpPost(s"http://localhost:${server.getAddress.getPort}/failure")
      val payload = "request payload".getBytes(StandardCharsets.UTF_8)
      request.setEntity(new AbstractHttpEntity {
        override def isRepeatable: Boolean = false
        override def getContentLength: Long = payload.length.toLong
        override def getContent: InputStream = throw new IOException("request body unavailable")
        override def writeTo(output: OutputStream): Unit = output.write(payload)
        override def isStreaming: Boolean = true
      })

      val error = intercept[RuntimeException] {
        RESTHelpers.safeSend(request, backoffs = Nil)
      }

      assert(error.getMessage.contains("responseBody: upstream failure"))
      assert(!error.getMessage.contains("request body unavailable"))
    } finally {
      server.stop(0)
    }
  }
}
// scalastyle:on magic.number
