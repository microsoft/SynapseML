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
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}
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
    new AIFoundryChatCompletion().setModel("example-model").setMessagesCol("messages"),
    new OpenAIPrompt().setModel("example-model")
      .setUrl("https://example.services.ai.azure.com").setMessagesCol("messages"),
    new OpenAIPrompt().setMessagesCol("messages").setApiType("chat_completions"),
    new OpenAIPrompt().setMessagesCol("messages").setApiType("responses")
  ).map(_.setDeploymentName("example-deployment"))

  private def requestRow(options: Option[Map[String, Any]] = None): Row = {
    val message = new GenericRowWithSchema(Array[Any]("user", "hello"), messageSchema)
    new GenericRowWithSchema(Array[Any](Seq(message), options.orNull), requestSchema)
  }

  private def payload(stage: CacheStage, row: Row): JsObject = {
    val build = prepareEntity.invoke(stage).asInstanceOf[Row => Option[AbstractHttpEntity]]
    EntityUtils.toString(build(row).get).parseJson.asJsObject
  }

  private val validOptions = Seq(
    Map.empty[String, Any],
    Map[String, Any]("mode" -> "implicit"),
    Map[String, Any]("mode" -> "explicit"),
    Map[String, Any]("ttl" -> "30m"),
    Map[String, Any]("mode" -> "explicit", "ttl" -> "30m")
  )

  private val invalidOptions = Seq(
    Map[String, Any]("mode" -> "other"),
    Map[String, Any]("ttl" -> "24h"),
    Map[String, Any]("mode" -> true),
    Map[String, Any]("ttl" -> Option.empty[String].orNull),
    Map[String, Any]("unsupported" -> "value")
  )

  test("cache options are omitted by default and add only a top-level JSON object") {
    stages.foreach { stage =>
      stage.setReasoningEffort("low").setVerbosity("low").setMaxCompletionTokens(32)
      val row = requestRow()
      val original = payload(stage, row).fields
      assert(!original.contains("prompt_cache_options"))

      validOptions.foreach { options =>
        stage.setPromptCacheOptions(options)
        val result = payload(stage, row)
        assert(result.fields == original.updated("prompt_cache_options", options.toJson))
        assert(!result.compactPrint.contains("prompt_cache_breakpoint"))
      }
    }
  }

  test("cache option columns serialize row values and omit null rows") {
    stages.foreach { stage =>
      stage.setPromptCacheOptions(Map("mode" -> "implicit"))
        .setPromptCacheOptionsCol("cache_options")
      assert(!payload(stage, requestRow()).fields.contains("prompt_cache_options"))
      validOptions.foreach { options =>
        assert(payload(stage, requestRow(Some(options))).fields("prompt_cache_options") == options.toJson)
      }
    }
  }

  test("invalid scalar cache options are rejected without replacing valid values") {
    stages.foreach { stage =>
      val original = Map[String, Any]("mode" -> "implicit")
      stage.setPromptCacheOptions(original)
      (invalidOptions :+ Option.empty[Map[String, Any]].orNull).foreach { options =>
        intercept[IllegalArgumentException] {
          stage.setPromptCacheOptions(options)
        }
        assert(stage.getPromptCacheOptions == original)
        intercept[IllegalArgumentException] {
          stage.set(stage.promptCacheOptions, Left(options))
        }
      }
    }
  }

  test("invalid cache option column values are rejected before request serialization") {
    stages.foreach { stage =>
      stage.setPromptCacheOptionsCol("cache_options")
      invalidOptions.foreach { options =>
        val error = intercept[IllegalArgumentException] {
          payload(stage, requestRow(Some(options)))
        }
        assert(error.getMessage.contains("promptCacheOptions"))
      }
    }
  }
}
