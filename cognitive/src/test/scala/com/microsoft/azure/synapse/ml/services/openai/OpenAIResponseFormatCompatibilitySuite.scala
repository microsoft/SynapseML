// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.param.AnyJsonFormat.anyFormat
import spray.json._

import scala.collection.immutable.ListMap

class OpenAIResponseFormatCompatibilitySuite extends TestBase {

  private case class Stage(
      name: String,
      setToken: String => Unit,
      setFormat: Map[String, Any] => Unit,
      read: () => Map[String, Any])

  private def stages: Seq[Stage] = {
    val chat = new OpenAIChatCompletion()
    val responses = new OpenAIResponses()
    val promptChat = new OpenAIPrompt().setApiType("chat_completions")
    val promptResponses = new OpenAIPrompt().setApiType("responses")
    Seq(
      Stage("chat", value => chat.setResponseFormat(value), value => chat.setResponseFormat(value),
        () => chat.getResponseFormat),
      Stage("responses", value => responses.setResponseFormat(value), value => responses.setResponseFormat(value),
        () => responses.getResponseFormat),
      Stage("prompt-chat", value => promptChat.setResponseFormat(value), value => promptChat.setResponseFormat(value),
        () => promptChat.getResponseFormat),
      Stage("prompt-responses", value => promptResponses.setResponseFormat(value),
        value => promptResponses.setResponseFormat(value), () => promptResponses.getResponseFormat)
    )
  }

  private val inner: Map[String, Any] = ListMap(
    "type" -> "object",
    "properties" -> ListMap(
      "answer" -> Map("type" -> "string"),
      "note" -> Map("type" -> Seq("string", "null"), "default" -> Option.empty[String].orNull)
    ),
    "required" -> Seq("answer", "note"),
    "additionalProperties" -> false
  )

  private def normalized(stage: Stage): JsObject = {
    val value = anyFormat.write(stage.read()).asJsObject
    value.fields.get("json_schema").orElse(value.fields.get("format")).getOrElse(value).asJsObject
  }

  Seq("text", "json_object").foreach { token =>
    test(s"$token strings and minimal dictionaries remain plain format selectors") {
      stages.foreach { stage =>
        Seq(token, s" ${token.toUpperCase} ").foreach { input =>
          stage.setToken(input)
          assert(normalized(stage) == JsObject("type" -> JsString(token)), stage.name)
        }
        stage.setFormat(Map("type" -> token))
        assert(normalized(stage) == JsObject("type" -> JsString(token)), stage.name)
      }
    }
  }

  test("Named partial, nested Chat, and flat Responses schemas preserve their explicit metadata") {
    Seq(None, Some(false), Some(true)).foreach { strict =>
      val base = Map("name" -> "ai_function_schema", "schema" -> inner) ++ strict.map("strict" -> _)
      val inputs = Seq(
        base,
        base + ("type" -> "json_schema"),
        Map("type" -> "json_schema", "json_schema" -> base)
      )
      stages.foreach { stage =>
        inputs.foreach { input =>
          stage.setFormat(input)
          val result = normalized(stage)
          assert(result.fields("name") == JsString("ai_function_schema"), stage.name)
          assert(result.fields("schema") == anyFormat.write(inner), stage.name)
          assert(result.fields.get("strict") == strict.map(JsBoolean(_)), stage.name)
          assert(!result.fields.contains("json_schema"), stage.name)
        }
      }
    }
  }

  test("Legacy inner-schema configuration does not silently enable strict mode") {
    stages.foreach { stage =>
      stage.setFormat(inner)
      val result = normalized(stage)
      assert(result.fields("name") == JsString("response_schema"), stage.name)
      assert(result.fields("schema") == anyFormat.write(inner), stage.name)
      assert(!result.fields.contains("strict"), stage.name)
    }
  }

  test("Inner schema completion adds only format metadata and not missing JSON Schema constraints") {
    val incomplete = Map("type" -> "object", "properties" -> Map("answer" -> Map("type" -> "string")))
    val chat = new OpenAIChatCompletion().setResponseSchema(incomplete)
    val result = anyFormat.write(chat.getResponseFormat).asJsObject.fields("json_schema").asJsObject
    assert(result.fields("name") == JsString("response_schema"))
    assert(result.fields("strict") == JsTrue)
    assert(result.fields("schema") == anyFormat.write(incomplete))
    assert(!chat.hasParam("responseSchema"))
  }

  test("Unsupported tokens and incomplete envelopes fail without changing the configured format") {
    val invalid = Seq(
      Map.empty[String, Any],
      Map("type" -> "xml"),
      Map("type" -> "json_schema"),
      Map("schema" -> inner),
      Map("type" -> "json_schema", "json_schema" -> Map("schema" -> inner))
    )
    stages.foreach { stage =>
      stage.setToken("json_object")
      Seq("xml", "json_schema").foreach { token =>
        intercept[IllegalArgumentException](stage.setToken(token))
        assert(normalized(stage) == JsObject("type" -> JsString("json_object")), stage.name)
      }
      invalid.foreach { input =>
        intercept[IllegalArgumentException](stage.setFormat(input))
        assert(normalized(stage) == JsObject("type" -> JsString("json_object")), stage.name)
      }
      stage.setToken(" ")
      stage.setToken(Option.empty[String].orNull)
      assert(normalized(stage) == JsObject("type" -> JsString("json_object")), stage.name)
    }
  }
}
