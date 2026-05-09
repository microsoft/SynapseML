// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.services.HasCognitiveServiceInput
import org.apache.http.entity.AbstractHttpEntity
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import spray.json._

class OpenAIV1EndpointSuite extends TestBase {

  import spark.implicits._

  private val prepareUrl = classOf[HasCognitiveServiceInput].getDeclaredMethod("prepareUrl")
  prepareUrl.setAccessible(true)

  private val prepareEntity = classOf[HasCognitiveServiceInput].getDeclaredMethod("prepareEntity")
  prepareEntity.setAccessible(true)

  private def requestUrl(transformer: HasCognitiveServiceInput, row: Row): String =
    prepareUrl.invoke(transformer).asInstanceOf[Row => String].apply(row)

  private def requestPayload(transformer: HasCognitiveServiceInput, row: Row): JsObject = {
    val entityBuilder = prepareEntity.invoke(transformer).asInstanceOf[Row => Option[AbstractHttpEntity]]
    EntityUtils.toString(entityBuilder.apply(row).get).parseJson.asJsObject
  }

  private val messageSchema = StructType(Seq(
    StructField("role", StringType, nullable = false),
    StructField("content", StringType, nullable = true),
    StructField("name", StringType, nullable = true)
  ))

  private val messagesRequestSchema = StructType(Seq(
    StructField("messages", ArrayType(messageSchema, containsNull = false), nullable = true)
  ))

  private def messagesRow: Row = {
    val message = new GenericRowWithSchema(
      Array[Any]("user", "hello", null), // scalastyle:ignore null
      messageSchema
    )
    new GenericRowWithSchema(Array[Any](Seq(message)), messagesRequestSchema)
  }

  test("chat completions uses OpenAI v1 base URL without api-version and sends model") {
    val transformer = new OpenAIChatCompletion()
      .setUrl("https://example.services.ai.azure.com/openai/v1")
      .setDeploymentName("gpt-4o")
      .setMessagesCol("messages")
      .setApiVersion("2025-04-01-preview")

    val row = messagesRow
    assert(requestUrl(transformer, row) == "https://example.services.ai.azure.com/openai/v1/chat/completions")

    val payload = requestPayload(transformer, row)
    assert(payload.fields.get("model").contains(JsString("gpt-4o")))
    assert(payload.fields.contains("messages"))
  }

  test("chat completions keeps legacy Azure deployment URL and api-version") {
    val transformer = new OpenAIChatCompletion()
      .setUrl("https://example.openai.azure.com/")
      .setDeploymentName("gpt-4o")
      .setMessagesCol("messages")
      .setApiVersion("2025-04-01-preview")

    val row = messagesRow
    assert(requestUrl(transformer, row) ==
      "https://example.openai.azure.com/openai/deployments/gpt-4o/chat/completions" +
        "?api-version=2025-04-01-preview")
    assert(!requestPayload(transformer, row).fields.contains("model"))
  }

  test("embeddings uses OpenAI v1 base URL and sends deployment as model") {
    val transformer = new OpenAIEmbedding()
      .setUrl("https://example.services.ai.azure.com/openai/v1")
      .setDeploymentName("text-embedding-3-large")
      .setTextCol("text")
      .setApiVersion("2025-04-01-preview")

    val row = Seq("hello").toDF("text").collect().head
    assert(requestUrl(transformer, row) == "https://example.services.ai.azure.com/openai/v1/embeddings")

    val payload = requestPayload(transformer, row)
    assert(payload.fields.get("model").contains(JsString("text-embedding-3-large")))
    assert(payload.fields.get("input").contains(JsString("hello")))
  }

  test("embeddings keeps legacy Azure deployment URL and api-version") {
    val transformer = new OpenAIEmbedding()
      .setUrl("https://example.openai.azure.com/")
      .setDeploymentName("text-embedding-3-large")
      .setTextCol("text")
      .setApiVersion("2025-04-01-preview")

    val row = Seq("hello").toDF("text").collect().head
    assert(requestUrl(transformer, row) ==
      "https://example.openai.azure.com/openai/deployments/text-embedding-3-large/embeddings" +
        "?api-version=2025-04-01-preview")

    val payload = requestPayload(transformer, row)
    assert(!payload.fields.contains("model"))
    assert(payload.fields.get("input").contains(JsString("hello")))
  }

  test("responses uses OpenAI v1 base URL without api-version") {
    val transformer = new OpenAIResponses()
      .setUrl("https://example.services.ai.azure.com/openai/v1")
      .setDeploymentName("gpt-5-mini")
      .setMessagesCol("messages")
      .setApiVersion("2025-04-01-preview")

    val row = messagesRow
    assert(requestUrl(transformer, row) == "https://example.services.ai.azure.com/openai/v1/responses")

    val payload = requestPayload(transformer, row)
    assert(payload.fields.get("model").contains(JsString("gpt-5-mini")))
    assert(payload.fields.contains("input"))
  }

  test("responses v1 endpoint requires deployment name as model") {
    val transformer = new OpenAIResponses()
      .setUrl("https://example.services.ai.azure.com/openai/v1")
      .setMessagesCol("messages")

    val err = intercept[IllegalArgumentException] {
      requestPayload(transformer, messagesRow)
    }
    assert(err.getMessage.contains("No deployment/model name provided for OpenAI v1 endpoint"))
  }

  test("responses keeps legacy Azure URL shape when URL is not an OpenAI v1 base") {
    val transformer = new OpenAIResponses()
      .setUrl("https://example.openai.azure.com/")
      .setDeploymentName("gpt-5-mini")
      .setMessagesCol("messages")
      .setApiVersion("2025-04-01-preview")

    assert(requestUrl(transformer, messagesRow) ==
      "https://example.openai.azure.com/openai/responses?api-version=2025-04-01-preview")
  }

  test("OpenAIPrompt treats services.ai.azure.com/openai/v1 as OpenAI v1, not models chat endpoint") {
    val prompt = new OpenAIPrompt()
      .setUrl("https://example.services.ai.azure.com/openai/v1")
      .setModel("gpt-4o")
      .setMessagesCol("messages")

    val prepareEntity = classOf[OpenAIPrompt].getDeclaredMethod("prepareEntity")
    prepareEntity.setAccessible(true)
    val buildEntity = prepareEntity.invoke(prompt).asInstanceOf[Row => Option[AbstractHttpEntity]]

    val payload = EntityUtils.toString(buildEntity(messagesRow).get).parseJson.asJsObject
    assert(payload.fields.get("model").contains(JsString("gpt-4o")))
    assert(payload.fields.contains("messages"))
  }
}
