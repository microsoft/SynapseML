// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.{Row, functions => F}
import spray.json._

class OpenAIChatCompletionToolsSuite extends TestBase {
  import spark.implicits._

  test("Chat Completions nests function tools and named tool choice on the wire") {
    val transformer = new OpenAIChatCompletion()
      .setMessagesCol("messages")
      .setTools(ToolTestFixtures.WeatherToolJson)
      .setToolChoiceFunction("get_weather")
      .setParallelToolCalls(false)
    val row = Seq(
      Seq(OpenAIMessage("user", "Weather in Seattle?"))
    ).toDF("messages").collect().head

    val payload = EntityUtils.toString(transformer.prepareEntity(row).get)
      .parseJson.asJsObject
    val tool = payload.fields("tools").asInstanceOf[JsArray].elements.head.asJsObject
    assert(tool.fields.keySet === Set("type", "function"))
    assert(tool.fields("type") === JsString("function"))
    assert(tool.fields("function").asJsObject.fields("name") === JsString("get_weather"))
    val choice = payload.fields("tool_choice").asJsObject
    assert(choice.fields("function").asJsObject.fields("name") === JsString("get_weather"))
    assert(payload.fields("parallel_tool_calls") === JsBoolean(false))
  }

  test("Chat Completions preserves assistant tool calls and tool result messages") {
    val messages = Seq(
      OpenAIChatMessage("user", content = Some("Weather in Seattle?")),
      OpenAIChatMessage(
        "assistant",
        tool_calls = Some(Seq(
          OpenAIChatToolCall(
            "call_a",
            OpenAIChatFunctionCall("get_weather", """{"city":"Seattle"}"""))
        ))),
      OpenAIChatMessage(
        "tool",
        content = Some("""{"tempC":20}"""),
        tool_call_id = Some("call_a"))
    )
    val row = Seq(messages).toDF("messages").collect().head
    val transformer = new OpenAIChatCompletion().setMessagesCol("messages")

    val payload = EntityUtils.toString(transformer.prepareEntity(row).get)
      .parseJson.asJsObject
    val encoded = payload.fields("messages").asInstanceOf[JsArray].elements
    val assistant = encoded(1).asJsObject
    val call = assistant.fields("tool_calls").asInstanceOf[JsArray].elements.head.asJsObject
    assert(!assistant.fields.contains("content"))
    assert(call.fields("id") === JsString("call_a"))
    assert(call.fields("function").asJsObject.fields("arguments") ===
      JsString("""{"city":"Seattle"}"""))
    val tool = encoded(2).asJsObject
    assert(tool.fields("role") === JsString("tool"))
    assert(tool.fields("tool_call_id") === JsString("call_a"))
    assert(tool.fields("content") === JsString("""{"tempC":20}"""))
  }

  test("Chat tool calls project to the shared DataFrame contract") {
    val parsed = Seq(1).toDF("id")
      .select(F.from_json(
        F.lit(ToolTestFixtures.ChatToolCallResponseJson),
        ChatModelResponseV2.schema
      ).as("value"))
      .withColumn("tool_calls", OpenAIToolColumns.chatToolCallsColumn("value"))
      .collect()
      .head
    val call = parsed.getAs[Seq[Row]]("tool_calls").head
    assert(call.getAs[String]("call_id") === "call_a")
    assert(call.getAs[String]("item_id") == null) //scalastyle:ignore null
    assert(call.getAs[String]("type") === "function")
    assert(call.getAs[String]("name") === "get_weather")
    assert(call.getAs[String]("arguments") === """{"city":"Seattle"}""")
    assert(call.getAs[Int]("index") === 0)
    assert(!new OpenAIChatCompletion().isContentFiltered(parsed.getAs[Row]("value")))
  }

  test("Chat tool validation and generated Python expose the supported surface") {
    val transformer = new OpenAIChatCompletion()
    assert(!transformer.hasParam("maxToolCalls"))
    val generated = transformer.generatedPythonClass
    assert(generated.contains("def setTools("))
    assert(generated.contains("def setToolChoice("))
    assert(generated.contains("def addFunctionTool("))
    assert(generated.contains("def toolCallsColumn("))
    assert(!generated.contains("def setMaxToolCalls("))
    assert(!generated.contains("def getMaxToolCallsCol("))
    assert(!generated.contains("def getMetadata("))
    assert(!generated.contains("def getInclude("))
    assert(!generated.contains("def replayItemsColumn("))
  }
}
