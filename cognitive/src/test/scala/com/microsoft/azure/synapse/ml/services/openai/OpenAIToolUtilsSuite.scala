// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.sql.types.StructType
import spray.json._

import java.util.{Arrays, LinkedHashMap}
import scala.collection.immutable.ListMap

class OpenAIToolUtilsSuite extends TestBase {
  test("function tools normalize to the flat Responses shape") {
    val nested =
      """[{"type":"function","allowed_callers":["direct"],"name":"outer","function":{
        |"name":"inner","description":"d","parameters":{"type":"object","properties":{},
        |"required":[],"additionalProperties":false},"strict":true}}]"""
        .stripMargin
    val tool = OpenAIToolUtils.parseTools(nested).elements.head.asJsObject
    assert(tool.fields("type") === JsString("function"))
    assert(tool.fields("name") === JsString("inner"))
    assert(tool.fields.contains("allowed_callers"))
    assert(!tool.fields.contains("function"))
  }

  test("function tools and choices adapt to each API wire shape") {
    val tools = OpenAIToolUtils.parseTools(ToolTestFixtures.WeatherToolJson)
    val chatTool = OpenAIToolUtils.toChatCompletionsTools(tools)
      .elements.head.asJsObject
    assert(chatTool.fields.keySet === Set("type", "function"))
    assert(chatTool.fields("function").asJsObject.fields("name") ===
      JsString("get_weather"))

    val nestedChoice = OpenAIToolUtils.parseToolChoice(
      """{"type":"function","function":{"name":"get_weather"}}""").get
    OpenAIToolUtils.validateToolChoiceAgainst(tools, nestedChoice)
    assert(OpenAIToolUtils.toResponsesToolChoice(nestedChoice).asJsObject
      .fields("name") === JsString("get_weather"))

    val flatChoice = OpenAIToolUtils.parseToolChoice(
      """{"type":"function","name":"get_weather"}""").get
    assert(OpenAIToolUtils.toChatCompletionsToolChoice(flatChoice).asJsObject
      .fields("function").asJsObject.fields("name") === JsString("get_weather"))

    val allowedChoice = OpenAIToolUtils.parseToolChoice(
      """{"type":"allowed_tools","mode":"required","tools":[
        |{"type":"function","name":"get_weather"}]}""".stripMargin).get
    val chatChoice = OpenAIToolUtils.toChatCompletionsToolChoice(allowedChoice).asJsObject
    val chatAllowed = chatChoice.fields("allowed_tools").asJsObject
    assert(chatAllowed.fields("mode") === JsString("required"))
    assert(chatAllowed.fields("tools").asInstanceOf[JsArray].elements.head
      .asJsObject.fields("function").asJsObject.fields("name") ===
      JsString("get_weather"))
    assert(OpenAIToolUtils.toResponsesToolChoice(chatChoice).asJsObject
      .fields("tools").asInstanceOf[JsArray].elements.nonEmpty)
  }

  test("unknown tool types and JSON scalar widths pass through") {
    val json =
      """[{"type":"web_search"},{"type":"future_tool","payload":{"n":9007199254740993,
        |"items":[1,2],"nullable":null}}]""".stripMargin
    val parsed = OpenAIToolUtils.parseTools(json)
    assert(parsed.elements.size === 2)
    val payload = parsed.elements(1).asJsObject.fields("payload").asJsObject
    assert(payload.fields("n") === JsNumber(BigDecimal("9007199254740993")))
    assert(payload.fields("nullable") === JsNull)
  }

  test("tool validation reports malformed definitions") {
    val missingType = intercept[IllegalArgumentException] {
      OpenAIToolUtils.parseTools("""[{"name":"f"}]""")
    }
    assert(missingType.getMessage.contains("index 0"))

    Seq("", "get weather", "tool!", "a" * 65).foreach { name =>
      val error = intercept[IllegalArgumentException] {
        OpenAIToolUtils.parseTools(
          s"""[{"type":"function","name":"$name","parameters":null}]""")
      }
      assert(error.getMessage.contains("Invalid function tool name"))
    }

    val duplicate = intercept[IllegalArgumentException] {
      OpenAIToolUtils.parseTools(
        """[{"type":"function","name":"f","parameters":null},
          |{"type":"function","name":"f","parameters":null}]""".stripMargin)
    }
    assert(duplicate.getMessage.contains("Duplicate function tool name 'f'"))
  }

  test("strict function schemas enforce Structured Outputs requirements recursively") {
    val missingAdditionalProperties = intercept[IllegalArgumentException] {
      OpenAIToolUtils.parseTools(
        """[{"type":"function","name":"f","strict":true,
          |"parameters":{"type":"object","properties":{},"required":[]}}]""".stripMargin)
    }
    assert(missingAdditionalProperties.getMessage.contains("additionalProperties"))

    val missingNestedRequired = intercept[IllegalArgumentException] {
      OpenAIToolUtils.parseTools(
        """[{"type":"function","name":"f","strict":true,"parameters":{
          |"type":"object","properties":{"address":{"type":"object",
          |"properties":{"city":{"type":"string"}},"required":[],
          |"additionalProperties":false}},"required":["address"],
          |"additionalProperties":false}}]""".stripMargin)
    }
    assert(missingNestedRequired.getMessage.contains("missing: city"))

    val undefinedRequired = intercept[IllegalArgumentException] {
      OpenAIToolUtils.parseTools(
        """[{"type":"function","name":"f","strict":true,"parameters":{
          |"type":"object","properties":{},"required":["missing"],
          |"additionalProperties":false}}]""".stripMargin)
    }
    assert(undefinedRequired.getMessage.contains("undefined properties"))

    val nullableOptional = OpenAIToolUtils.parseTools(
      """[{"type":"function","name":"f","strict":true,"parameters":{
        |"type":"object","properties":{"units":{"type":["string","null"]}},
        |"required":["units"],"additionalProperties":false}}]""".stripMargin)
    assert(OpenAIToolUtils.hasStrictFunctionTool(nullableOptional))

    val nonStrict = OpenAIToolUtils.parseTools(
      """[{"type":"function","name":"f","strict":false,
        |"parameters":{"type":"object","properties":{"city":{"type":"string"}}}}]""".stripMargin)
    assert(!OpenAIToolUtils.hasStrictFunctionTool(nonStrict))
  }

  test("tool choice remains forward compatible and blank rows are omitted") {
    assert(OpenAIToolUtils.parseToolChoice(" auto ") === Some(JsString("auto")))
    assert(OpenAIToolUtils.parseToolChoice("future_mode") === Some(JsString("future_mode")))
    assert(OpenAIToolUtils.parseToolChoice("   ").isEmpty)
    assert(OpenAIToolUtils.parseToolChoice(null).isEmpty) //scalastyle:ignore null

    val choice = OpenAIToolUtils.parseToolChoice(
      """{"type":"allowed_tools","mode":"auto","tools":[]}""").get.asJsObject
    assert(choice.fields("type") === JsString("allowed_tools"))

    assertThrows[IllegalArgumentException] {
      OpenAIToolUtils.parseToolChoice("""[{"type":"function"}]""")
    }
    assertThrows[IllegalArgumentException] {
      OpenAIToolUtils.parseToolChoice("""{"name":"f"}""")
    }
  }

  test("forced function choices are checked only when statically knowable") {
    val tools = OpenAIToolUtils.parseTools(
      """[{"type":"function","name":"a","parameters":null}]""")
    OpenAIToolUtils.validateToolChoiceAgainst(
      tools,
      OpenAIToolUtils.parseToolChoice("""{"type":"function","name":"a"}""").get)
    val error = intercept[IllegalArgumentException] {
      OpenAIToolUtils.validateToolChoiceAgainst(
        tools,
        OpenAIToolUtils.parseToolChoice("""{"type":"function","name":"b"}""").get)
    }
    assert(error.getMessage.contains("unknown function 'b'"))
  }

  test("input item parsing accepts arrays objects and blanks") {
    assert(OpenAIToolUtils.parseInputItems("").isEmpty)
    assert(OpenAIToolUtils.parseInputItems(null).isEmpty) //scalastyle:ignore null
    assert(OpenAIToolUtils.parseInputItems("""{"type":"reasoning","id":"r"}""").size === 1)
    assert(OpenAIToolUtils.parseInputItems(
      """[{"type":"reasoning"},{"type":"function_call"}]""").size === 2)
    assertThrows[IllegalArgumentException] {
      OpenAIToolUtils.parseInputItems("123")
    }
  }

  test("toJsValue handles Scala Java and prebuilt JSON values") {
    assert(OpenAIToolUtils.toJsValue(null) === JsNull) //scalastyle:ignore null
    assert(OpenAIToolUtils.toJsValue(7L) === JsNumber(7))
    assert(OpenAIToolUtils.toJsValue(0.1f).compactPrint === "0.1")
    assert(OpenAIToolUtils.toJsValue(JsObject.empty) === JsObject.empty)
    assert(OpenAIToolUtils.toJsValue(Arrays.asList("a", "b")) ===
      JsArray(JsString("a"), JsString("b")))

    val linked = new LinkedHashMap[String, Object]()
    linked.put("b", Integer.valueOf(1))
    linked.put("a", null) //scalastyle:ignore null
    assert(OpenAIToolUtils.toJsValue(linked).compactPrint === """{"b":1,"a":null}""")

    assert(OpenAIToolUtils.toJsValue(
      ListMap("k" -> Seq(Map("n" -> null)))).compactPrint === //scalastyle:ignore null
      """{"k":[{"n":null}]}""")
    assertThrows[IllegalArgumentException] {
      OpenAIToolUtils.toJsValue(new Object())
    }
  }

  test("toJsValue rejects non-finite floating point values clearly") {
    Seq[Any](
      Double.NaN,
      Double.PositiveInfinity,
      Double.NegativeInfinity,
      Float.NaN,
      Float.PositiveInfinity,
      Float.NegativeInfinity
    ).foreach { value =>
      val error = intercept[IllegalArgumentException] {
        OpenAIToolUtils.toJsValue(value)
      }
      assert(error.getMessage ===
        s"Cannot serialize non-finite ${value.getClass.getSimpleName} tool value $value")
    }
  }

  test("function call output rows preserve order and omit null status") {
    val outputs = Seq(
      ToolTestFixtures.functionOutput("call_a", """{"tempC":20}""", "completed"),
      ToolTestFixtures.functionOutput("call_b", "plain text")
    )
    val items = OpenAIToolColumns.toFunctionCallOutputs(outputs)
    assert(items.map(_("call_id")) === Seq("call_a", "call_b"))
    assert(items.head("type") === "function_call_output")
    assert(items.head("status") === "completed")
    assert(!items(1).contains("status"))

    assert(OpenAIToolColumns.toFunctionCallOutputs(null).isEmpty) //scalastyle:ignore null
    assertThrows[IllegalArgumentException] {
      OpenAIToolColumns.toFunctionCallOutputs(
        Seq(ToolTestFixtures.functionOutput("call_a", "a"), null)) //scalastyle:ignore null
    }
    assertThrows[IllegalArgumentException] {
      OpenAIToolColumns.toFunctionCallOutputs(Seq(
        ToolTestFixtures.functionOutput("call_a", "a"),
        ToolTestFixtures.functionOutput("call_a", "b")
      ))
    }
    assertThrows[IllegalArgumentException] {
      OpenAIToolColumns.toFunctionCallOutputs(
        Seq(ToolTestFixtures.functionOutput(" ", "a")))
    }
  }

  test("tool call and continuation schemas match the public contract") {
    val toolElement = OpenAIToolColumns.ToolCallStructType.elementType
      .asInstanceOf[StructType]
    assert(toolElement.fieldNames ===
      Array("call_id", "item_id", "type", "name", "arguments", "status", "index"))
    assert(!toolElement("index").nullable)
    assert(!OpenAIToolColumns.ToolCallStructType.containsNull)

    val outputElement = OpenAIToolColumns.FunctionCallOutputStructType.elementType
      .asInstanceOf[StructType]
    assert(outputElement.fieldNames === Array("call_id", "output", "status"))
    assert(!OpenAIToolColumns.FunctionCallOutputStructType.containsNull)
    assert(!outputElement("call_id").nullable)
    assert(!outputElement("output").nullable)
    assert(outputElement("status").nullable)
  }
}
