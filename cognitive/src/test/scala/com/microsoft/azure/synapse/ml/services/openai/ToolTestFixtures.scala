// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

object ToolTestFixtures {
  val EmptyObjectSchema: Map[String, Any] = Map(
    "type" -> "object",
    "properties" -> Map.empty[String, Any],
    "required" -> Seq.empty[String],
    "additionalProperties" -> false
  )

  val WeatherToolMap: Map[String, Any] = Map(
    "type" -> "function",
    "name" -> "get_weather",
    "description" -> "Get the current weather for a city.",
    "parameters" -> Map(
      "type" -> "object",
      "properties" -> Map("city" -> Map("type" -> "string")),
      "required" -> Seq("city"),
      "additionalProperties" -> false
    ),
    "strict" -> true
  )

  val WeatherToolJson: String = OpenAIToolUtils.toolsToJson(Seq(WeatherToolMap))

  val MessageSchema: StructType = StructType(Seq(
    StructField("role", StringType, nullable = false),
    StructField("content", StringType),
    StructField("name", StringType)
  ))

  val MessagesArrayType: ArrayType = ArrayType(MessageSchema, containsNull = false)

  val FunctionOutputSchema: StructType =
    OpenAIToolColumns.FunctionCallOutputStructType.elementType.asInstanceOf[StructType]

  def message(role: String, content: String, name: String = null): Row = //scalastyle:ignore null
    new GenericRowWithSchema(Array[Any](role, content, name), MessageSchema)

  def functionOutput(callId: String, output: String, status: String = null): Row = //scalastyle:ignore null
    new GenericRowWithSchema(Array[Any](callId, output, status), FunctionOutputSchema)

  def dynamicRow(fields: (String, DataType, Any)*): Row = {
    val schema = StructType(fields.map { case (name, dataType, _) =>
      StructField(name, dataType)
    })
    new GenericRowWithSchema(fields.map(_._3).toArray, schema)
  }

  val ToolCallResponseJson: String =
    """{
      |"id":"resp_1",
      |"object":"response",
      |"created_at":"1",
      |"model":"gpt-5.1",
      |"output":[{
      |  "type":"function_call",
      |  "id":"fc_1",
      |  "call_id":"call_a",
      |  "name":"get_weather",
      |  "arguments":"{\"city\":\"Seattle\"}",
      |  "status":"completed",
      |  "content":null
      |}],
      |"system_fingerprint":null,
      |"usage":null,
      |"status":"completed",
      |"incomplete_details":null,
      |"error":null
      |}""".stripMargin

  val MessageResponseJson: String =
    """{
      |"id":"resp_2",
      |"object":"response",
      |"created_at":"2",
      |"model":"gpt-5.1",
      |"output":[{
      |  "type":"message",
      |  "id":"msg_2",
      |  "role":"assistant",
      |  "phase":"final_answer",
      |  "status":"completed",
      |  "content":[{"type":"output_text","text":"It is 20C in Seattle."}]
      |}],
      |"system_fingerprint":null,
      |"usage":null,
      |"status":"completed"
      |}""".stripMargin

  val ChatToolCallResponseJson: String =
    """{
      |"id":"chatcmpl_1",
      |"object":"chat.completion",
      |"created":"1",
      |"model":"gpt-5.1",
      |"choices":[{
      |  "index":0,
      |  "message":{
      |    "role":"assistant",
      |    "content":null,
      |    "name":null,
      |    "refusal":null,
      |    "tool_calls":[{
      |      "id":"call_a",
      |      "type":"function",
      |      "function":{
      |        "name":"get_weather",
      |        "arguments":"{\"city\":\"Seattle\"}"
      |      }
      |    }]
      |  },
      |  "finish_reason":"tool_calls"
      |}],
      |"system_fingerprint":null,
      |"usage":null
      |}""".stripMargin

  val MixedResponseJson: String =
    """{
      |"id":"resp_mixed",
      |"object":"response",
      |"created_at":"3",
      |"model":"gpt-5.1",
      |"output":[
      |  {
      |    "type":"reasoning",
      |    "id":"rs_drop",
      |    "status":"completed",
      |    "summary":[{"type":"summary_text","text":"stored"}],
      |    "encrypted_content":null,
      |    "content":null
      |  },
      |  {
      |    "type":"reasoning",
      |    "id":"rs_keep",
      |    "status":"completed",
      |    "summary":[{"type":"summary_text","text":"stateless"}],
      |    "encrypted_content":"encrypted",
      |    "content":null
      |  },
      |  {
      |    "type":"message",
      |    "id":"msg_1",
      |    "role":"assistant",
      |    "phase":"commentary",
      |    "status":"completed",
      |    "content":[{"type":"output_text","text":"Working"}]
      |  },
      |  {
      |    "type":"function_call",
      |    "id":"fc_1",
      |    "call_id":"call_a",
      |    "name":"get_weather",
      |    "arguments":"{\"city\":\"Seattle\"}",
      |    "status":"completed",
      |    "content":null
      |  }
      |],
      |"system_fingerprint":null,
      |"usage":{
      |  "input_tokens":10,
      |  "output_tokens":5,
      |  "total_tokens":15,
      |  "input_tokens_details":{"cached_tokens":4,"cache_write_tokens":2},
      |  "output_tokens_details":{"reasoning_tokens":3}
      |},
      |"status":"completed"
      |}""".stripMargin
}
