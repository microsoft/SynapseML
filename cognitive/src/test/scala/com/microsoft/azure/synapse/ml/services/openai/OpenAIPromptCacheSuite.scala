// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.param.AnyJsonFormat.anyFormat
import com.microsoft.azure.synapse.ml.services.HasCognitiveServiceInput
import com.microsoft.azure.synapse.ml.services.aifoundry.AIFoundryChatCompletion
import org.apache.http.entity.AbstractHttpEntity
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, BooleanType, MapType, StringType, StructType}
import org.scalatest.funsuite.AnyFunSuite
import spray.json.DefaultJsonProtocol._
import spray.json._

class OpenAIPromptCacheSuite extends AnyFunSuite {

  private type CacheStage = HasOpenAITextParams with HasCognitiveServiceInput

  private val messageSchema = new StructType().add("role", StringType).add("content", StringType)
  private val requestSchema = new StructType()
    .add("messages", ArrayType(messageSchema))
    .add("cache_options", MapType(StringType, StringType))

  private val prepareEntity = classOf[HasCognitiveServiceInput].getDeclaredMethod("prepareEntity")
  prepareEntity.setAccessible(true)

  private def stages: Seq[CacheStage] = Seq(
    new OpenAIChatCompletion().setMessagesCol("messages"),
    new OpenAIResponses().setMessagesCol("messages"),
    new AIFoundryChatCompletion().setModel("gpt-6-luna").setMessagesCol("messages"),
    new OpenAIPrompt().setModel("gpt-6-luna")
      .setUrl("https://example.services.ai.azure.com").setMessagesCol("messages"),
    new OpenAIPrompt().setMessagesCol("messages").setApiType("chat_completions"),
    new OpenAIPrompt().setMessagesCol("messages").setApiType("responses")
  ).map(_.setDeploymentName("gpt-6-luna"))

  private def requestRow(options: Option[Map[String, Any]] = None): Row = {
    val message = new GenericRowWithSchema(Array[Any]("user", "hello"), messageSchema)
    new GenericRowWithSchema(Array[Any](Seq(message), options.orNull), requestSchema)
  }

  private def payload(stage: CacheStage, row: Row): JsObject = {
    val build = prepareEntity.invoke(stage).asInstanceOf[Row => Option[AbstractHttpEntity]]
    EntityUtils.toString(build(row).get).parseJson.asJsObject
  }

  test("unset options preserve the request and scalar options are forwarded without policy validation") {
    val options = Seq(
      Map.empty[String, Any],
      Map[String, Any]("mode" -> "explicit", "ttl" -> "30m"),
      Map[String, Any]("ttl" -> "12h", "prewarm" -> true),
      Map[String, Any]("mode" -> "provider-decides", "ttl" -> "not-a-cache-lifetime")
    )
    stages.foreach { stage =>
      val original = payload(stage, requestRow()).fields
      assert(!original.contains("prompt_cache_options"))
      options.foreach { value =>
        stage.setPromptCacheOptions(value)
        assert(payload(stage, requestRow()).fields == original.updated("prompt_cache_options", value.toJson))
      }
    }
  }

  test("column options replace scalar binding and omit null rows without validating values") {
    stages.foreach { stage =>
      stage.setPromptCacheOptions(Map("mode" -> "implicit")).setPromptCacheOptionsCol("cache_options")
      assert(!payload(stage, requestRow()).fields.contains("prompt_cache_options"))
      Seq(
        Map[String, Any]("mode" -> "explicit", "ttl" -> "30m"),
        Map[String, Any]("ttl" -> "not-a-cache-lifetime")
      ).foreach { options =>
        assert(payload(stage, requestRow(Some(options))).fields("prompt_cache_options") == options.toJson)
      }
    }
  }

  test("Responses text image and file inputs preserve explicit breakpoint objects") {
    val partSchema = new StructType()
      .add("type", StringType).add("text", StringType).add("image_url", StringType)
      .add("file_id", StringType).add("prompt_cache_breakpoint", MapType(StringType, StringType))
    val messagesSchema = new StructType().add("role", StringType).add("content", ArrayType(partSchema))
    val breakpoint = Map("mode" -> "explicit")
    val parts = Seq(
      Array[Any]("input_text", "Reusable instructions", null, null, breakpoint), // scalastyle:ignore null
      Array[Any]("input_image", null, "https://example.org/image.png", null, breakpoint), // scalastyle:ignore null
      Array[Any]("input_file", null, null, "file-example", breakpoint) // scalastyle:ignore null
    ).map(values => new GenericRowWithSchema(values, partSchema))
    val message = new GenericRowWithSchema(Array[Any]("developer", parts), messagesSchema)
    val response = new OpenAIResponses()
    val body = EntityUtils.toString(response.getStringEntity(Seq(message), Map.empty)).parseJson.asJsObject
    val encodedParts = body.fields("input").asInstanceOf[JsArray].elements.head.asJsObject
      .fields("content").asInstanceOf[JsArray].elements
    assert(encodedParts.length == parts.length)
    encodedParts.foreach { part =>
      assert(part.asJsObject.fields("prompt_cache_breakpoint") == breakpoint.toJson)
    }
  }

  test("struct options retain nested JSON values without adding cache-policy validation") {
    val nestedSchema = new StructType().add("enabled", BooleanType)
    val optionsSchema = new StructType().add("ttl", StringType).add("prewarm", BooleanType)
      .add("future", ArrayType(MapType(StringType, nestedSchema)))
    val schema = new StructType().add("messages", ArrayType(messageSchema)).add("cache_options", optionsSchema)
    val nested = new GenericRowWithSchema(Array[Any](false), nestedSchema)
    val options = new GenericRowWithSchema(
      Array[Any]("provider-decides", true, Seq(Map("entry" -> nested))), optionsSchema)
    val message = new GenericRowWithSchema(Array[Any]("user", "hello"), messageSchema)
    val row = new GenericRowWithSchema(Array[Any](Seq(message), options), schema)
    val expected =
      """{"ttl":"provider-decides","prewarm":true,"future":[{"entry":{"enabled":false}}]}""".parseJson
    stages.foreach { stage =>
      assert(payload(stage.setPromptCacheOptionsCol("cache_options"), row).fields("prompt_cache_options") == expected)
    }
  }

  test("Chat text and image inputs preserve breakpoint values for service validation") {
    val imageSchema = new StructType().add("url", StringType)
    val partSchema = new StructType().add("type", StringType).add("text", StringType)
      .add("image_url", imageSchema).add("prompt_cache_breakpoint", MapType(StringType, StringType))
    val messagesSchema = new StructType().add("role", StringType).add("content", ArrayType(partSchema))
    val breakpoint = Map("mode" -> "provider-decides", "ttl" -> "not-a-cache-lifetime")
    val image = new GenericRowWithSchema(Array[Any]("https://example.org/image.png"), imageSchema)
    val parts = Seq(
      Array[Any]("text", "Reusable instructions", null, breakpoint), // scalastyle:ignore null
      Array[Any]("image_url", null, image, breakpoint) // scalastyle:ignore null
    ).map(values => new GenericRowWithSchema(values, partSchema))
    val message = new GenericRowWithSchema(Array[Any]("developer", parts), messagesSchema)
    val body = EntityUtils.toString(new OpenAIChatCompletion()
      .getStringEntity(Seq(message), Map.empty)).parseJson.asJsObject
    val encodedParts = body.fields("messages").asInstanceOf[JsArray].elements.head.asJsObject
      .fields("content").asInstanceOf[JsArray].elements
    assert(encodedParts.length == parts.length)
    encodedParts.foreach { part =>
      assert(part.asJsObject.fields("prompt_cache_breakpoint") == breakpoint.toJson)
    }
  }
}
