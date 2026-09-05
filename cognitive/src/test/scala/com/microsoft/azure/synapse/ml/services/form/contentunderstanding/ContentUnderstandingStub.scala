// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.form.contentunderstanding

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.commons.io.IOUtils

import java.io.ByteArrayOutputStream
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.{CopyOnWriteArrayList, Executors}
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.GZIPOutputStream
import scala.collection.JavaConverters._

private[contentunderstanding] object ContentUnderstandingFixtures {
  val AnalyzersPath = "/contentunderstanding/analyzers"
  val ResultsPath = "/contentunderstanding/analyzerResults/"
  val DefaultApiVersion = "2025-11-01"
  val TooManyRequests = 429
}

private[contentunderstanding] case class ContentUnderstandingStubReply(status: Int,
                                                                      body: String,
                                                                      headers: Map[String, String] = Map.empty,
                                                                      chunked: Boolean = false,
                                                                      disconnect: Boolean = false,
                                                                      gzip: Boolean = false)

private[contentunderstanding] case class ContentUnderstandingStubRequest(method: String,
                                                                        path: String,
                                                                        query: String,
                                                                        headers: Map[String, String],
                                                                        body: String)

private[contentunderstanding] object ContentUnderstandingStub {
  def withReplies(replies: Seq[ContentUnderstandingStubReply])(test: ContentUnderstandingStub => Unit): Unit = {
    require(replies.nonEmpty)
    val next = new AtomicInteger()
    val server = new ContentUnderstandingStub(_ => replies(math.min(next.getAndIncrement(), replies.size - 1)))
    try {
      test(server)
    } finally {
      server.close()
    }
  }
}

private[contentunderstanding] class ContentUnderstandingStub(
    respond: ContentUnderstandingStubRequest => ContentUnderstandingStubReply) extends AutoCloseable {

  private val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
  private val executor = Executors.newCachedThreadPool()
  private val recorded = new CopyOnWriteArrayList[ContentUnderstandingStubRequest]()
  val endpoint: String = s"http://127.0.0.1:${server.getAddress.getPort}"

  def requests: Seq[ContentUnderstandingStubRequest] = recorded.asScala.toVector

  private def responseBytes(response: ContentUnderstandingStubReply): Array[Byte] = {
    val bytes = response.body.getBytes(StandardCharsets.UTF_8)
    if (response.gzip) {
      val output = new ByteArrayOutputStream()
      val gzip = new GZIPOutputStream(output)
      try {
        gzip.write(bytes)
      } finally {
        gzip.close()
      }
      output.toByteArray
    } else {
      bytes
    }
  }

  server.setExecutor(executor)
  server.createContext("/", new HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      try {
        val input = exchange.getRequestBody
        val body = try {
          IOUtils.toString(input, StandardCharsets.UTF_8)
        } finally {
          input.close()
        }
        val request = ContentUnderstandingStubRequest(exchange.getRequestMethod,
          exchange.getRequestURI.getRawPath, Option(exchange.getRequestURI.getRawQuery).getOrElse(""),
          exchange.getRequestHeaders.asScala.map { case (name, values) =>
            name.toLowerCase(java.util.Locale.ROOT) -> values.asScala.mkString(",")
          }.toMap, body)
        recorded.add(request)
        val response = respond(request)
        if (!response.disconnect) {
          response.headers.foreach { case (name, value) =>
            exchange.getResponseHeaders.set(name, value.replace("$ROOT", endpoint))
          }
          exchange.getResponseHeaders.set("Content-Type", "application/json; charset=utf-8")
          if (response.gzip) {
            exchange.getResponseHeaders.set("Content-Encoding", "gzip")
          }
          val bytes = responseBytes(response)
          exchange.sendResponseHeaders(response.status, if (response.chunked) 0L else bytes.length.toLong)
          exchange.getResponseBody.write(bytes)
        }
      } finally {
        exchange.close()
      }
    }
  })
  server.start()

  override def close(): Unit = {
    server.stop(0)
    executor.shutdownNow()
  }
}
