// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.apache.spark.sql.Row
import spray.json._

import java.net.{InetSocketAddress, ServerSocket}
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._
import scala.io.Source

class OpenAIChatCompletionToolsIntegrationSuite extends TestBase {
  import spark.implicits._

  private def freePort(): Int = {
    val socket = new ServerSocket(0)
    try {
      socket.getLocalPort
    } finally {
      socket.close()
    }
  }

  private def readBody(exchange: HttpExchange): String = {
    val source = Source.fromInputStream(exchange.getRequestBody, "UTF-8")
    try {
      source.mkString
    } finally {
      source.close()
    }
  }

  private def respond(exchange: HttpExchange, body: String): Unit = {
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.add("Content-Type", "application/json")
    exchange.sendResponseHeaders(200, bytes.length)
    val output = exchange.getResponseBody
    try {
      output.write(bytes)
    } finally {
      output.close()
      exchange.close()
    }
  }

  private def withServer(test: (String, ConcurrentLinkedQueue[String]) => Unit): Unit = {
    val port = freePort()
    val requests = new ConcurrentLinkedQueue[String]()
    val server = HttpServer.create(new InetSocketAddress("localhost", port), 10)
    server.createContext("/v1/chat/completions", (exchange: HttpExchange) => {
      requests.add(readBody(exchange))
      respond(exchange, ToolTestFixtures.ChatToolCallResponseJson)
    })
    server.start()
    try {
      test(s"http://localhost:$port/v1", requests)
    } finally {
      server.stop(0)
    }
  }

  test("Chat executor sends valid row tools and isolates malformed rows") {
    withServer { (url, requests) =>
      val transformer = new OpenAIChatCompletion()
        .setUrl(url)
        .setDeploymentName("gpt-5.1")
        .setSubscriptionKey("local-stub-key")
        .setMessagesCol("messages")
        .setToolsCol("row_tools")
        .setToolChoice("auto")
        .setToolCallsCol("tool_calls")
        .setOutputCol("out")
        .setConcurrency(1)
      val df = Seq(
        (1, Seq(OpenAIMessage("user", "valid request")), ToolTestFixtures.WeatherToolJson),
        (2, Seq(OpenAIMessage("user", "invalid request")), "[{")
      ).toDF("id", "messages", "row_tools")

      val rows = transformer.transform(df).orderBy("id").collect()
      val call = rows.head.getAs[Seq[Row]]("tool_calls").head
      assert(call.getAs[String]("call_id") === "call_a")
      assert(call.getAs[String]("name") === "get_weather")
      assert(rows.head.getAs[Row](transformer.getErrorCol) == null) //scalastyle:ignore null
      assert(rows(1).getAs[Row]("out") == null) //scalastyle:ignore null
      assert(rows(1).getAs[Row](transformer.getErrorCol).getAs[String]("response")
        .contains("tools must be a JSON array"))

      val bodies = requests.iterator().asScala.toSeq
      assert(bodies.nonEmpty)
      assert(bodies.forall(_.contains("valid request")))
      assert(!bodies.exists(_.contains("invalid request")))
      val payload = bodies.head.parseJson.asJsObject
      val tool = payload.fields("tools").asInstanceOf[JsArray].elements.head.asJsObject
      assert(tool.fields("function").asJsObject.fields("name") === JsString("get_weather"))
    }
  }

  test("OpenAIPrompt defaults to Chat tool calling without flagging tool-only output") {
    withServer { (url, requests) =>
      val prompt = new OpenAIPrompt()
        .setUrl(url)
        .setDeploymentName("gpt-5.1")
        .setSubscriptionKey("local-stub-key")
        .setPromptTemplate("Weather in {city}?")
        .setTools(ToolTestFixtures.WeatherToolJson)
        .setToolChoiceFunction("get_weather")
        .setToolCallsCol("tool_calls")
        .setResponseStructCol("response_struct")
        .setOutputCol("answer")
        .setErrorCol("error")
        .setConcurrency(1)

      val row = prompt.transform(Seq("Seattle").toDF("city")).collect().head
      assert(row.getAs[String]("answer") == null) //scalastyle:ignore null
      assert(row.getAs[Seq[Row]]("tool_calls").head.getAs[String]("call_id") === "call_a")
      assert(row.getAs[Row]("response_struct").getAs[String]("id") === "chatcmpl_1")
      assert(row.getAs[Row]("error") == null) //scalastyle:ignore null

      val payload = requests.iterator().asScala.toSeq.head.parseJson.asJsObject
      assert(payload.fields("tools").asInstanceOf[JsArray].elements.head
        .asJsObject.fields.contains("function"))
      assert(payload.fields("tool_choice").asJsObject.fields("function")
        .asJsObject.fields("name") === JsString("get_weather"))
    }
  }
}
