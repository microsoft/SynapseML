// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.env.StreamUtilities
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.param.AnyJsonFormat.anyFormat
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap

class OpenAIResponseSchemaSuite extends TestBase {

  import spark.implicits._

  private val propertyNames = Seq("sentiment", "score", "reason", "category", "optional_note")
  private val schema: Map[String, Any] = ListMap(
    "type" -> "object",
    "title" -> "Review",
    "description" -> "A classified review",
    "properties" -> ListMap(
      "sentiment" -> Map("type" -> "string"),
      "score" -> Map("type" -> "integer", "minimum" -> Long.MinValue, "maximum" -> Long.MaxValue),
      "reason" -> Map("type" -> "string"),
      "category" -> Map("type" -> "string"),
      "optional_note" -> Map("type" -> Seq("string", "null"),
        "enum" -> Seq(Option.empty[AnyRef].orNull, "note"))
    ),
    "required" -> propertyNames,
    "additionalProperties" -> false
  )
  private val sparkSchema = new StructType().add("sentiment", StringType).add("score", IntegerType)
  private val answer =
    """{"sentiment":"positive","score":9,"reason":"clear","category":"review","optional_note":null}"""

  private def chatFormat(chat: OpenAIChatCompletion): Map[String, Any] =
    chat.getResponseFormat("json_schema").asInstanceOf[Map[String, Any]]

  private def assertSchema(format: JsObject, name: String = "response_schema", strict: Boolean = true): Unit = {
    assert(format.fields("name") == JsString(name))
    assert(format.fields("strict") == JsBoolean(strict))
    assert(format.fields("schema") == schema.toJson)
  }

  private def assertSchemaOrder(request: String): Unit = {
    Seq("schema" -> schema.keys.toSeq, "properties" -> propertyNames).foreach { case (field, keys) =>
      val start = request.indexOf(s""""$field":{""")
      assert(start >= 0, s"Missing $field object in $request")
      val positions = keys.map(key => request.indexOf(s""""$key":""", start))
      assert(positions.forall(_ >= 0), s"Missing $field keys in $request")
      assert(positions == positions.sorted, s"Incorrect $field key order in $request")
    }
  }

  private def withServer(testCode: (String, ConcurrentLinkedQueue[String]) => Unit): Unit = {
    val requests = new ConcurrentLinkedQueue[String]()
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/openai/v1", new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        try {
          val body = StreamUtilities.using(exchange.getRequestBody) { stream =>
            IOUtils.toString(stream, StandardCharsets.UTF_8)
          }.get
          requests.add(body)
          val response = if (exchange.getRequestURI.getPath.endsWith("/responses")) {
            s"""{"id":"response","object":"response","created_at":"1","model":"schema-test",
               |"output":[{"content":[{"type":"output_text","text":${JsString(answer).compactPrint}}],
               |"status":"completed"}]}""".stripMargin
          } else {
            s"""{"id":"chat","object":"chat.completion","created":"1","model":"schema-test",
               |"choices":[{"message":{"role":"assistant","content":${JsString(answer).compactPrint}},
               |"index":0,"finish_reason":"stop"}]}""".stripMargin
          }
          val bytes = response.getBytes(StandardCharsets.UTF_8)
          exchange.getResponseHeaders.add("Content-Type", "application/json")
          exchange.sendResponseHeaders(200, bytes.length)
          StreamUtilities.using(exchange.getResponseBody)(_.write(bytes)).get
        } finally {
          exchange.close()
        }
      }
    })
    server.start()
    try {
      testCode(s"http://127.0.0.1:${server.getAddress.getPort}/openai/v1", requests)
    } finally {
      server.stop(0)
    }
  }

  test("schema-only setters add a default name and strict mode without modifying the schema") {
    val chat = new OpenAIChatCompletion().setResponseSchema(schema)
    assert(chat.getResponseFormatType == "json_schema")
    assertSchema(chatFormat(chat).toJson.asJsObject)
    assert(chatFormat(chat)("schema") == schema)

    val responses = new OpenAIResponses().setResponseSchema(schema)
    val format = responses.getResponseFormat.toJson.asJsObject.fields("format").asJsObject
    assert(format.fields("type") == JsString("json_schema"))
    assertSchema(format)

    val prompt = new OpenAIPrompt().setResponseSchema(schema)
    assertSchema(prompt.getResponseFormat.toJson.asJsObject.fields("json_schema").asJsObject)
  }

  test("schema-only setters allow a custom name and explicit non-strict mode") {
    val chat = new OpenAIChatCompletion().setResponseSchema(schema, "sentiment-v1")
    assertSchema(chatFormat(chat).toJson.asJsObject, name = "sentiment-v1")
    chat.setResponseSchema(schema, "sentiment_v2", strict = false)
    assertSchema(chatFormat(chat).toJson.asJsObject, name = "sentiment_v2", strict = false)
    val responses = new OpenAIResponses().setResponseSchema(schema, "sentiment_v2", strict = false)
    assertSchema(responses.getResponseFormat.toJson.asJsObject.fields("format").asJsObject,
      name = "sentiment_v2", strict = false)
    Seq("a", "a" * 64).foreach { name =>
      assert(chatFormat(chat.setResponseSchema(schema, name))("name") == name)
    }
  }

  test("schema-only setters reject invalid input without changing a configured response format") {
    val chat = new OpenAIChatCompletion().setResponseFormat("json_object")
    Seq("", "has space", "a" * 65).foreach { name =>
      intercept[IllegalArgumentException] {
        chat.setResponseSchema(schema, name)
      }
    }
    intercept[IllegalArgumentException] {
      chat.setResponseSchema(Map.empty[String, Any])
    }
    intercept[IllegalArgumentException] {
      chat.setResponseSchema(null) // scalastyle:ignore null
    }
    intercept[IllegalArgumentException] {
      chat.setResponseSchema(schema, null) // scalastyle:ignore null
    }
    Seq[Any](BigInt("9223372036854775808"), BigDecimal("0.12345678901234567890123456789")).foreach { value =>
      intercept[IllegalArgumentException] {
        chat.setResponseSchema(Map("maximum" -> value))
      }
    }
    assert(chat.getResponseFormat == Map("type" -> "json_object"))
  }

  test("all Scala stages can override strictness while keeping the default schema name") {
    val chat = new OpenAIChatCompletion().setResponseSchema(schema, strict = false)
    assertSchema(chatFormat(chat).toJson.asJsObject, strict = false)
    val responses = new OpenAIResponses().setResponseSchema(schema, strict = false)
    assertSchema(responses.getResponseFormat.toJson.asJsObject.fields("format").asJsObject, strict = false)
    val prompt = new OpenAIPrompt().setResponseSchema(schema, strict = false)
    assertSchema(prompt.getResponseFormat.toJson.asJsObject.fields("json_schema").asJsObject, strict = false)
  }

  test("legacy response formats retain their existing strictness and schema-only calls can be replaced") {
    val chat = new OpenAIChatCompletion().setResponseFormat(schema)
    assert(!chatFormat(chat).contains("strict"))
    chat.setResponseSchema(schema).setResponseFormat("text")
    assert(chat.getResponseFormat == Map("type" -> "text"))
    chat.setResponseFormat(Map("name" -> "legacy", "schema" -> schema, "strict" -> false))
    assert(chatFormat(chat).get("strict").contains(false))
  }

  test("schema-only settings retain the existing responseFormat parameter through copy and save/load") {
    val chat = new OpenAIChatCompletion().setResponseSchema(schema, "persisted", strict = false)
    assert(!chat.hasParam("responseSchema"))
    val copied = chat.copy(ParamMap.empty)
    assert(copied.getOrDefault(chat.responseFormat) == chat.getOrDefault(chat.responseFormat))
    val directory = Files.createTempDirectory("openai-response-schema").toFile
    try {
      val path = new java.io.File(directory, "chat").toString
      chat.write.session(spark).save(path)
      assert(OpenAIChatCompletion.read.session(spark).load(path).getResponseFormat == chat.getResponseFormat)
    } finally {
      FileUtils.deleteDirectory(directory)
    }
  }

  test("Chat Completions sends a schema-only format and supports native typed extraction") {
    withServer { (url, requests) =>
      val chat = new OpenAIChatCompletion()
        .setUrl(url)
        .setSubscriptionKey("unused")
        .setDeploymentName("schema-test")
        .setMessagesCol("messages")
        .setOutputCol("output")
        .setErrorCol("error")
      val input = Seq(Seq(OpenAIMessage("user", "I love this."))).toDF("messages")
      val originalSchema = chat.transformSchema(input.schema)
      val originalRuntimeSchema = chat.transform(input).schema
      chat.setResponseSchema(schema)
      val output = chat.transform(input)
      assert(chat.transformSchema(input.schema) == originalSchema)
      assert(output.schema == originalRuntimeSchema)
      val parsed = output.withColumn("parsed",
        from_json(col("output.choices").getItem(0).getField("message").getField("content"), sparkSchema))
      val result = parsed.head()
      assert(Option(result.getAs[Row]("error")).isEmpty)
      assert(result.getAs[Row]("parsed") == Row("positive", 9))
      assert(requests.size() == 1)
      assertSchemaOrder(requests.asScala.head)
      val payload = requests.asScala.head.parseJson.asJsObject.fields("response_format").asJsObject
      assert(payload.fields("type") == JsString("json_schema"))
      assertSchema(payload.fields("json_schema").asJsObject)
    }
  }

  test("Responses sends a schema-only format under text.format") {
    withServer { (url, requests) =>
      val responses = new OpenAIResponses()
        .setUrl(url)
        .setSubscriptionKey("unused")
        .setDeploymentName("schema-test")
        .setMessagesCol("messages")
        .setOutputCol("output")
        .setErrorCol("error")
      val input = Seq(Seq(OpenAIMessage("user", "I love this."))).toDF("messages")
      val originalSchema = responses.transformSchema(input.schema)
      val originalRuntimeSchema = responses.transform(input).schema
      responses.setResponseSchema(schema)
      val output = responses.transform(input)
      assert(responses.transformSchema(input.schema) == originalSchema)
      assert(output.schema == originalRuntimeSchema)
      val result = output.head()
      assert(Option(result.getAs[Row]("error")).isEmpty)
      assert(requests.size() == 1)
      assertSchemaOrder(requests.asScala.head)
      val payload = requests.asScala.head.parseJson.asJsObject
      assert(!payload.fields.contains("response_format"))
      val format = payload.fields("text").asJsObject.fields("format").asJsObject
      assert(format.fields("type") == JsString("json_schema"))
      assertSchema(format)
    }
  }

  Seq("chat_completions", "responses").foreach { apiType =>
    test(s"OpenAIPrompt forwards schema-only settings and parses typed output with $apiType") {
      withServer { (url, requests) =>
        val prompt = new OpenAIPrompt()
          .setUrl(url)
          .setSubscriptionKey("unused")
          .setDeploymentName("schema-test")
          .setApiType(apiType)
          .setPromptTemplate("{text}")
          .setResponseSchema(schema)
          .setPostProcessing("json")
          .setPostProcessingOptions(Map("jsonSchema" -> "sentiment STRING, score INT"))
          .setOutputCol("output")
          .setErrorCol("error")
        val input = Seq("I love this.").toDF("text")
        val output = prompt.transform(input)
        assert(output.schema == prompt.transformSchema(input.schema))
        val result = output.head()
        assert(Option(result.getAs[Row]("error")).isEmpty)
        assert(result.getAs[Row]("output") == Row("positive", 9))
        assert(requests.size() == 1)
        assertSchemaOrder(requests.asScala.head)
        val payload = requests.asScala.head.parseJson.asJsObject
        val format = if (apiType == "responses") {
          payload.fields("text").asJsObject.fields("format").asJsObject
        } else {
          payload.fields("response_format").asJsObject.fields("json_schema").asJsObject
        }
        assertSchema(format)
      }
    }
  }
}
