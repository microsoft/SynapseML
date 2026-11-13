// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.commons.io.IOUtils
import spray.json._

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentLinkedQueue

abstract class OpenAIPromptHttpTestBase extends TestBase {

  protected val dataImage =
    "data:image/png;base64," +
      "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Y9Zl1sAAAAASUVORK5CYII="
  protected val unpaddedDataImage = dataImage.stripSuffix("=")
  protected val dataJson = "data:application/json;base64,e30="
  protected val dataPdf = "data:application/pdf;base64,JVBERi0xLjQK"
  protected val imageBytes = java.util.Base64.getDecoder.decode(dataImage.split(",", 2)(1))

  private def responseFor(path: String): String = {
    if (path.endsWith("/responses")) {
      JsObject(
        "id" -> JsString("resp_test"),
        "object" -> JsString("response"),
        "created_at" -> JsString("1"),
        "model" -> JsString("gpt-6-luna"),
        "output" -> JsArray(JsObject(
          "content" -> JsArray(JsObject(
            "type" -> JsString("output_text"),
            "text" -> JsString("ok")
          )),
          "status" -> JsString("completed")
        )),
        "system_fingerprint" -> JsNull,
        "usage" -> JsNull
      ).compactPrint
    } else {
      JsObject(
        "id" -> JsString("chatcmpl_test"),
        "object" -> JsString("chat.completion"),
        "created" -> JsString("1"),
        "model" -> JsString("gpt-6-luna"),
        "choices" -> JsArray(JsObject(
          "message" -> JsObject(
            "role" -> JsString("assistant"),
            "content" -> JsString("ok"),
            "name" -> JsNull
          ),
          "index" -> JsNumber(0),
          "finish_reason" -> JsString("stop")
        )),
        "system_fingerprint" -> JsNull,
        "usage" -> JsNull
      ).compactPrint
    }
  }

  protected def withEchoServer(
      testCode: (String, ConcurrentLinkedQueue[String], ConcurrentLinkedQueue[String]) => Unit,
      statusCode: Int = 200,
      responseBody: String => String = responseFor _
  ): Unit = {
    val bodies = new ConcurrentLinkedQueue[String]()
    val attachmentRequests = new ConcurrentLinkedQueue[String]()
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/openai/v1", new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        val body = new String(IOUtils.toByteArray(exchange.getRequestBody), StandardCharsets.UTF_8)
        bodies.add(body)
        val response = responseBody(exchange.getRequestURI.getPath).getBytes(StandardCharsets.UTF_8)
        exchange.getResponseHeaders.add("Content-Type", "application/json")
        exchange.sendResponseHeaders(statusCode, response.length)
        exchange.getResponseBody.write(response)
        exchange.close()
      }
    })
    def addAttachmentContext(path: String, contentType: String, content: Array[Byte]): Unit = {
      server.createContext(path, new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          attachmentRequests.add(exchange.getRequestURI.getPath)
          exchange.getResponseHeaders.add("Content-Type", contentType)
          exchange.sendResponseHeaders(200, content.length)
          exchange.getResponseBody.write(content)
          exchange.close()
        }
      })
    }
    addAttachmentContext("/image.png", "image/png", imageBytes)
    addAttachmentContext("/image", "image/png", imageBytes)
    addAttachmentContext("/document-json", "application/octet-stream", "{}".getBytes(StandardCharsets.UTF_8))
    addAttachmentContext(
      "/document-xml",
      "application/octet-stream",
      "<?xml version=\"1.0\"?><root/>".getBytes(StandardCharsets.UTF_8)
    )
    server.start()
    try {
      testCode(
        s"http://127.0.0.1:${server.getAddress.getPort}/openai/v1",
        bodies,
        attachmentRequests
      )
    } finally {
      server.stop(0)
    }
  }

  protected def withEchoServer(testCode: (String, ConcurrentLinkedQueue[String]) => Unit): Unit = {
    withEchoServer { (baseUrl, bodies, _) =>
      testCode(baseUrl, bodies)
    }
  }

  protected def prompt(baseUrl: String, apiType: String): OpenAIPrompt = {
    new OpenAIPrompt()
      .setUrl(baseUrl)
      .setApiType(apiType)
      .setDeploymentName("gpt-6-luna")
      .setSubscriptionKey("unused")
      .setPromptTemplate("{prompt}")
      .setColumnTypes(Map("image" -> "path"))
      .setOutputCol("output")
      .setErrorCol("error")
  }
}
