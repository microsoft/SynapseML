// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import spray.json._
import spray.json.DefaultJsonProtocol._

class OpenAICoreOfflineSuite extends AnyFunSuite {

  private val stringMessageSchema = StructType(Seq(
    StructField("role", StringType, nullable = false),
    StructField("content", StringType, nullable = true),
    StructField("name", StringType, nullable = true)
  ))

  private val compositeMessageSchema = StructType(Seq(
    StructField("role", StringType, nullable = false),
    StructField(
      "content",
      ArrayType(
        MapType(StringType, StringType, valueContainsNull = true),
        containsNull = false
      ),
      nullable = true
    ),
    StructField("name", StringType, nullable = true)
  ))

  private val unsupportedContentSchema = StructType(Seq(
    StructField("role", StringType, nullable = false),
    StructField("content", IntegerType, nullable = false)
  ))

  private def messageRow(role: String, content: String): Row =
    new GenericRowWithSchema(Array[Any](role, content, ""), stringMessageSchema)

  private def compositeMessageRow(role: String, parts: Seq[Map[String, String]]): Row =
    new GenericRowWithSchema(Array[Any](role, parts, ""), compositeMessageSchema)

  private def unsupportedMessageRow(role: String, content: Int): Row =
    new GenericRowWithSchema(Array[Any](role, content), unsupportedContentSchema)

  private def parseEntity(entity: StringEntity): JsObject =
    EntityUtils.toString(entity).parseJson.asJsObject

  test("encodeMessagesToMap supports text and composite message shapes") {
    val chat = new OpenAIChatCompletion()
    val compositeParts = Seq(
      Map("type" -> "text", "text" -> "first"),
      Map("type" -> "input_file", "filename" -> "example.txt")
    )

    val mapped = chat.encodeMessagesToMap(Seq(
      messageRow("user", "hello"),
      compositeMessageRow("assistant", compositeParts)
    ))

    assert(mapped.head("role") == "user")
    assert(mapped.head("content") == "hello")
    val secondContent = mapped(1)("content").asInstanceOf[Seq[Map[String, Any]]]
    assert(secondContent.head("type") == "text")
    assert(secondContent(1)("type") == "input_file")
  }

  test("encodeMessagesToMap rejects unsupported content types") {
    val chat = new OpenAIChatCompletion()
    val ex = intercept[IllegalArgumentException] {
      chat.encodeMessagesToMap(Seq(unsupportedMessageRow("user", 123)))
    }
    assert(ex.getMessage.contains("Unsupported content type"))
  }

  test("OpenAIChatCompletion getStringEntity collapses content parts into text") {
    val chat = new OpenAIChatCompletion()
    val messageParts = Seq(
      Map("type" -> "text", "text" -> "Line one"),
      Map("type" -> "input_file", "filename" -> "example.txt"),
      Map("type" -> "text", "text" -> "Line two")
    )

    val entity = chat.getStringEntity(
      Seq(compositeMessageRow("user", messageParts)),
      Map("temperature" -> 0.0)
    )

    val payload = parseEntity(entity)
    val JsArray(messages) = payload.fields("messages")
    val content = messages.head.asJsObject.fields("content").convertTo[String]

    assert(content == "Line one\nLine two")
  }

  test("OpenAIChatCompletion response_format wraps bare schemas and exposes type") {
    val chat = new OpenAIChatCompletion()
    chat.setResponseFormat(Map(
      "name" -> "answer_schema",
      "strict" -> true,
      "schema" -> Map(
        "type" -> "object",
        "properties" -> Map("answer" -> Map("type" -> "string"))
      )
    ))

    val responseFormat = chat.getResponseFormat
    assert(chat.getResponseFormatType == "json_schema")
    assert(responseFormat("type") == "json_schema")
    val jsonSchema = responseFormat("json_schema").asInstanceOf[Map[String, Any]]
    assert(jsonSchema("name") == "answer_schema")
    assert(jsonSchema.contains("schema"))
  }

  test("OpenAIResponses optional params merge text/reasoning and drop gpt-5 sampling") {
    val responses = new OpenAIResponses()
      .setDeploymentName("gpt-5-mini")
      .setTemperature(0.3)
      .setTopP(0.7)
      .setResponseFormat("json_object")
      .setVerbosity("high")
      .setReasoningEffort("medium")

    val params = responses.getOptionalParams(messageRow("user", "hello"))

    assert(params("model") == "gpt-5-mini")
    assert(!params.contains("temperature"))
    assert(!params.contains("top_p"))
    assert(!params.contains("reasoning_effort"))

    val text = params("text").asInstanceOf[Map[String, Any]]
    val format = text("format").asInstanceOf[Map[String, Any]]
    assert(format("type") == "json_object")
    assert(text("verbosity") == "high")

    val reasoning = params("reasoning").asInstanceOf[Map[String, Any]]
    assert(reasoning("effort") == "medium")
  }

  test("OpenAIResponses keeps sampling params for non-gpt5 deployments") {
    val responses = new OpenAIResponses()
      .setDeploymentName("gpt-4.1-mini")
      .setTemperature(0.2)
      .setTopP(0.6)

    val params = responses.getOptionalParams(messageRow("user", "hello"))

    assert(params("model") == "gpt-4.1-mini")
    assert(params("temperature") == 0.2)
    assert(params("top_p") == 0.6)
  }

  test("OpenAIResponses getStringEntity wraps plain text and preserves composite parts") {
    val responses = new OpenAIResponses()
    val compositeParts = Seq(
      Map("type" -> "input_file", "filename" -> "example.txt", "file_data" -> "AAA")
    )

    val entity = responses.getStringEntity(
      Seq(
        messageRow("user", "plain text"),
        compositeMessageRow("user", compositeParts)
      ),
      Map("model" -> "gpt-4.1-mini")
    )

    val payload = parseEntity(entity)
    val JsArray(inputs) = payload.fields("input")

    val JsArray(firstContent) = inputs.head.asJsObject.fields("content")
    assert(firstContent.head.asJsObject.fields("type").convertTo[String] == "input_text")
    assert(firstContent.head.asJsObject.fields("text").convertTo[String] == "plain text")

    val JsArray(secondContent) = inputs(1).asJsObject.fields("content")
    assert(secondContent.head.asJsObject.fields("type").convertTo[String] == "input_file")
  }

  test("OpenAI chat and responses stages expose expected response schemas") {
    assert(new OpenAIChatCompletion().responseDataType == ChatModelResponse.schema)
    assert(new OpenAIResponses().responseDataType == ResponsesModelResponse.schema)
  }
}
