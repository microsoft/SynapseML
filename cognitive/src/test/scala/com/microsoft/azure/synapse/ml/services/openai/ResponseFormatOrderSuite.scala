// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.env.StreamUtilities
import org.scalatest.funsuite.AnyFunSuite
import org.apache.http.entity.StringEntity
import org.apache.spark.sql.Row

class ResponseFormatOrderSuite extends AnyFunSuite {

  private def entityToString(e: StringEntity): String =
    StreamUtilities.using(e.getContent)(scala.io.Source.fromInputStream(_, "UTF-8").mkString).get

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

  private def makeBareSchema(properties: Map[String, Any]): Map[String, Any] = {
    Map(
      "name" -> "ordered_schema",
      "strict" -> true,
      "schema" -> Map(
        "type" -> "object",
        "properties" -> properties,
        "required" -> properties.keys.toSeq,
        "additionalProperties" -> false
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

  test("ChatCompletions accepts bare schema Map and wraps as json_schema") {
    val chat = new OpenAIChatCompletion()
    val bare = makeBareSchema(reasonAnsProps(reasonFirst = true))
    chat.setResponseFormat(bare)

    val entity = chat.getStringEntity(Seq.empty[Row], Map("response_format" -> chat.getResponseFormat))
    val json = entityToString(entity)

    assert(json.contains("\"response_format\""), s"missing response_format in JSON: $json")
    assert(json.contains("\"type\":\"json_schema\""), s"missing type json_schema in JSON: $json")
    assert(json.contains("\"json_schema\""), s"missing nested json_schema in JSON: $json")
  }

  test("Responses accepts bare schema Map and flattens under format") {
    val resp = new OpenAIResponses()
    val bare = makeBareSchema(reasonAnsProps(reasonFirst = true))
    resp.setResponseFormat(bare)

    val entity = resp.getStringEntity(Seq.empty[Row], resp.getResponseFormat)
    val json = entityToString(entity)

    assert(json.contains("\"format\""), s"missing format in JSON: $json")
    assert(json.contains("\"type\":\"json_schema\""), s"missing type json_schema in JSON: $json")
    assert(!json.contains("\"json_schema\":{"), s"Responses should flatten, unexpected nested json_schema: $json")
    assert(json.contains("\"name\":\"ordered_schema\""), s"missing name in flattened format: $json")
  }
}
