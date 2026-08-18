// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import spray.json._

import java.net.{InetSocketAddress, ServerSocket}
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._
import scala.io.Source

class OpenAIResponsesToolsIntegrationSuite extends TestBase {
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
    server.createContext("/v1/responses", (exchange: HttpExchange) => {
      val body = readBody(exchange)
      requests.add(body)
      val response =
        if (body.contains("function_call_output")) ToolTestFixtures.MessageResponseJson
        else ToolTestFixtures.ToolCallResponseJson
      respond(exchange, response)
    })
    server.start()
    try {
      test(s"http://localhost:$port/v1", requests)
    } finally {
      server.stop(0)
    }
  }

  test("Responses executor path sends tools and projects structured calls") {
    withServer { (url, requests) =>
      val transformer = new OpenAIResponses()
        .setUrl(url)
        .setDeploymentName("gpt-5.1")
        .setSubscriptionKey("local-stub-key")
        .setMessagesCol("messages")
        .setOutputCol("out")
        .setTools(ToolTestFixtures.WeatherToolJson)
        .setToolChoice("auto")
        .setToolCallsCol("tool_calls")
        .setConcurrency(1)
        .setTimeout(30.0)

      val result = transformer.transform(
        Seq(Seq(OpenAIMessage("user", "Weather in Seattle?"))).toDF("messages")
      ).collect().head

      val request = requests.iterator().asScala.toSeq.head.parseJson.asJsObject
      assert(request.fields("tools").isInstanceOf[JsArray])
      assert(request.fields("tool_choice") === JsString("auto"))
      assert(request.fields("model") === JsString("gpt-5.1"))
      assert(request.fields("input").asInstanceOf[JsArray].elements.head
        .asJsObject.fields("role") === JsString("user"))
      val calls = result.getAs[Seq[Row]]("tool_calls")
      assert(calls.size === 1)
      assert(calls.head.getAs[String]("call_id") === "call_a")
      assert(calls.head.getAs[String]("name") === "get_weather")
      assert(result.getAs[Row]("out").getAs[String]("status") === "completed")
      assert(result.getAs[Row](transformer.getErrorCol) == null) //scalastyle:ignore null
    }
  }

  test("typed function outputs continue a stored response without messages") {
    withServer { (url, requests) =>
      val schema = StructType(Seq(
        StructField("tool_results", OpenAIToolColumns.FunctionCallOutputStructType),
        StructField("response_id", StringType)
      ))
      val row = Row(
        Seq(ToolTestFixtures.functionOutput(
          "call_a",
          """{"tempC":20}""",
          "completed")),
        "resp_1"
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(row)), schema)
      val transformer = new OpenAIResponses()
        .setUrl(url)
        .setDeploymentName("gpt-5.1")
        .setSubscriptionKey("local-stub-key")
        .setFunctionCallOutputsCol("tool_results")
        .setPreviousResponseIdCol("response_id")
        .setTools(ToolTestFixtures.WeatherToolJson)
        .setOutputCol("out")
        .setConcurrency(1)

      val result = transformer.transform(df)
        .select(transformer.getOutputMessageText("out").as("text"))
        .collect().head.getString(0)
      assert(result === "It is 20C in Seattle.")

      val request = requests.iterator().asScala.toSeq.head.parseJson.asJsObject
      val input = request.fields("input").asInstanceOf[JsArray].elements
      assert(input.size === 1)
      assert(input.head.asJsObject.fields("type") === JsString("function_call_output"))
      assert(input.head.asJsObject.fields("call_id") === JsString("call_a"))
      assert(request.fields("previous_response_id") === JsString("resp_1"))
    }
  }

  test("malformed per-row tools populate errorCol without aborting the partition") {
    withServer { (url, requests) =>
      val transformer = new OpenAIResponses()
        .setUrl(url)
        .setDeploymentName("gpt-5.1")
        .setSubscriptionKey("local-stub-key")
        .setMessagesCol("messages")
        .setToolsCol("row_tools")
        .setOutputCol("out")
        .setConcurrency(1)
      val df = Seq(
        (1, Seq(OpenAIMessage("user", "valid request")), ToolTestFixtures.WeatherToolJson),
        (2, Seq(OpenAIMessage("user", "invalid request")), "[{")
      ).toDF("id", "messages", "row_tools")
      val rows = transformer.transform(df).orderBy("id").collect()
      assert(rows.head.getAs[Row]("out") != null) //scalastyle:ignore null
      assert(rows.head.getAs[Row](transformer.getErrorCol) == null) //scalastyle:ignore null
      assert(rows(1).getAs[Row]("out") == null) //scalastyle:ignore null
      assert(rows(1).getAs[Row](transformer.getErrorCol).getAs[String]("response")
        .contains("tools must be a JSON array"))
      val bodies = requests.iterator().asScala.toSeq
      assert(bodies.nonEmpty)
      assert(bodies.forall(_.contains("valid request")))
      assert(!bodies.exists(_.contains("invalid request")))
    }
  }

  test("row-bound toolChoice is validated against scalar tools before HTTP execution") {
    withServer { (url, requests) =>
      val transformer = new OpenAIResponses()
        .setUrl(url)
        .setDeploymentName("gpt-5.1")
        .setSubscriptionKey("local-stub-key")
        .setMessagesCol("messages")
        .setTools(ToolTestFixtures.WeatherToolJson)
        .setToolChoiceCol("row_choice")
        .setOutputCol("out")
        .setConcurrency(1)

      val df = Seq(
        (1, Seq(OpenAIMessage("user", "valid request")),
          """{"type":"function","name":"get_weather"}"""),
        (2, Seq(OpenAIMessage("user", "invalid request")),
          """{"type":"function","name":"missing_tool"}""")
      ).toDF("id", "messages", "row_choice")

      val rows = transformer.transform(df).orderBy("id").collect()
      assert(rows.head.getAs[Row]("out") != null) //scalastyle:ignore null
      assert(rows.head.getAs[Row](transformer.getErrorCol) == null) //scalastyle:ignore null
      assert(rows(1).getAs[Row]("out") == null) //scalastyle:ignore null
      assert(rows(1).getAs[Row](transformer.getErrorCol).getAs[String]("response")
        .contains("unknown function 'missing_tool'"))

      val bodies = requests.iterator().asScala.toSeq
      assert(bodies.nonEmpty)
      assert(bodies.forall(_.contains("valid request")))
      assert(!bodies.exists(_.contains("invalid request")))
    }
  }

  test("scalar toolChoice requires non-blank per-row tools") {
    withServer { (url, requests) =>
      val transformer = new OpenAIResponses()
        .setUrl(url)
        .setDeploymentName("gpt-5.1")
        .setSubscriptionKey("local-stub-key")
        .setMessagesCol("messages")
        .setToolsCol("row_tools")
        .setToolChoice("required")
        .setOutputCol("out")
        .setConcurrency(1)

      val df = Seq(
        (1, Seq(OpenAIMessage("user", "valid request")), ToolTestFixtures.WeatherToolJson),
        (2, Seq(OpenAIMessage("user", "invalid request")), " ")
      ).toDF("id", "messages", "row_tools")

      val rows = transformer.transform(df).orderBy("id").collect()
      assert(rows.head.getAs[Row]("out") != null) //scalastyle:ignore null
      assert(rows(1).getAs[Row]("out") == null) //scalastyle:ignore null
      assert(rows(1).getAs[Row](transformer.getErrorCol).getAs[String]("response")
        .contains("toolChoice requires tools"))

      val bodies = requests.iterator().asScala.toSeq
      assert(bodies.nonEmpty)
      assert(bodies.forall(_.contains("valid request")))
      assert(!bodies.exists(_.contains("invalid request")))
    }
  }

  test("OpenAIPrompt retains parsed response and does not flag tool-only rows") {
    withServer { (url, requests) =>
      val prompt = new OpenAIPrompt()
        .setUrl(url)
        .setDeploymentName("gpt-5.1")
        .setSubscriptionKey("local-stub-key")
        .setApiType("responses")
        .setPromptTemplate("Weather in {city}?")
        .setTools(ToolTestFixtures.WeatherToolJson)
        .setToolCallsCol("tool_calls")
        .setResponseStructCol("response_struct")
        .setOutputCol("answer")
        .setErrorCol("error")
        .setConcurrency(1)

      val input = Seq("Seattle").toDF("city")
      val output = prompt.transform(input)
      val declaredSchema = prompt.transformSchema(input.schema)
      assert(output.schema.fieldNames === declaredSchema.fieldNames)
      assert(DataType.equalsStructurally(
        output.schema,
        declaredSchema,
        ignoreNullability = true))
      val row = output.collect().head
      assert(row.getAs[String]("answer") == null) //scalastyle:ignore null
      assert(row.getAs[Seq[Row]]("tool_calls").head.getAs[String]("call_id") === "call_a")
      assert(row.getAs[Row]("response_struct").getAs[String]("id") === "resp_1")
      assert(row.getAs[Row]("error") == null) //scalastyle:ignore null
      assert(requests.size() === 1)
    }
  }
}
