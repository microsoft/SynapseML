// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import org.scalatest.funsuite.AnyFunSuite
import org.apache.http.entity.StringEntity
import org.apache.spark.sql.Row

class ResponseFormatOrderSuite extends AnyFunSuite {

  private def entityToString(e: StringEntity): String = {
    val is = e.getContent
    try {
      scala.io.Source.fromInputStream(is, "UTF-8").mkString
    } finally {
      is.close()
    }
  }

  private def makeJsonSchema(properties: Map[String, Any]): Map[String, Any] = {
    Map(
      "type" -> "json_schema",
      "json_schema" -> Map(
        "name" -> "ordered_schema",
        "strict" -> true,
        "schema" -> Map(
          "type" -> "object",
          "properties" -> properties,
          "required" -> properties.keys.toSeq,
          "additionalProperties" -> false
        )
      )
    )
  }

  private def reasonAnsProps(reasonFirst: Boolean): Map[String, Any] = {
    val r = Map("reason" -> Map("type" -> "string"))
    val a = Map("ans" -> Map("type" -> "string"))
    if (reasonFirst) r ++ a else a ++ r
  }

  test("ChatCompletions payload preserves json_schema.properties order: reason then ans") {
    val chat = new OpenAIChatCompletion()
    val rf = makeJsonSchema(reasonAnsProps(reasonFirst = true))
    chat.setResponseFormat(rf)

    val entity = chat.getStringEntity(Seq.empty[Row], Map("response_format" -> chat.getResponseFormat))
    val json = entityToString(entity)

    val propsStart = json.indexOf("\"properties\":{")
    assert(propsStart >= 0, s"properties block not found in JSON: $json")
    val reasonIdx = json.indexOf("\"reason\"", propsStart)
    val ansIdx = json.indexOf("\"ans\"", propsStart)
    assert(reasonIdx >= 0 && ansIdx >= 0, s"reason/ans keys not found in properties: $json")
    assert(reasonIdx < ansIdx, s"Order incorrect: expected reason before ans. JSON: $json")
  }

  test("ChatCompletions payload preserves json_schema.properties order: ans then reason") {
    val chat = new OpenAIChatCompletion()
    val rf = makeJsonSchema(reasonAnsProps(reasonFirst = false))
    chat.setResponseFormat(rf)

    val entity = chat.getStringEntity(Seq.empty[Row], Map("response_format" -> chat.getResponseFormat))
    val json = entityToString(entity)

    val propsStart = json.indexOf("\"properties\":{")
    assert(propsStart >= 0, s"properties block not found in JSON: $json")
    val reasonIdx = json.indexOf("\"reason\"", propsStart)
    val ansIdx = json.indexOf("\"ans\"", propsStart)
    assert(reasonIdx >= 0 && ansIdx >= 0, s"reason/ans keys not found in properties: $json")
    assert(ansIdx < reasonIdx, s"Order incorrect: expected ans before reason. JSON: $json")
  }

  test("Responses payload preserves json_schema.properties order: reason then ans") {
    val resp = new OpenAIResponses()
    val rf = makeJsonSchema(reasonAnsProps(reasonFirst = true))
    resp.setResponseFormat(rf)

    // For test purposes, place the format at top-level to focus on serialization order
    val entity = resp.getStringEntity(Seq.empty[Row], resp.getResponseFormat)
    val json = entityToString(entity)

    val propsStart = json.indexOf("\"properties\":{")
    assert(propsStart >= 0, s"properties block not found in JSON: $json")
    val reasonIdx = json.indexOf("\"reason\"", propsStart)
    val ansIdx = json.indexOf("\"ans\"", propsStart)
    assert(reasonIdx >= 0 && ansIdx >= 0, s"reason/ans keys not found in properties: $json")
    assert(reasonIdx < ansIdx, s"Order incorrect: expected reason before ans. JSON: $json")
  }

  test("Responses payload preserves json_schema.properties order: ans then reason") {
    val resp = new OpenAIResponses()
    val rf = makeJsonSchema(reasonAnsProps(reasonFirst = false))
    resp.setResponseFormat(rf)

    val entity = resp.getStringEntity(Seq.empty[Row], resp.getResponseFormat)
    val json = entityToString(entity)

    val propsStart = json.indexOf("\"properties\":{")
    assert(propsStart >= 0, s"properties block not found in JSON: $json")
    val reasonIdx = json.indexOf("\"reason\"", propsStart)
    val ansIdx = json.indexOf("\"ans\"", propsStart)
    assert(reasonIdx >= 0 && ansIdx >= 0, s"reason/ans keys not found in properties: $json")
    assert(ansIdx < reasonIdx, s"Order incorrect: expected ans before reason. JSON: $json")
  }
}

