// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import org.apache.spark.sql.{Column, Row, functions => F}
import org.apache.spark.sql.types._

import scala.collection.immutable.ListMap

/** Spark-facing adapters for Responses API tool calls and continuations. */
object OpenAIToolColumns {
  val ToolCallStructType: ArrayType = ArrayType(
    StructType(Seq(
      StructField("call_id", StringType),
      StructField("item_id", StringType),
      StructField("type", StringType),
      StructField("name", StringType),
      StructField("arguments", StringType),
      StructField("status", StringType),
      StructField("index", IntegerType, nullable = false)
    )),
    containsNull = false
  )

  val FunctionCallOutputStructType: ArrayType = ArrayType(StructType(Seq(
    StructField("call_id", StringType),
    StructField("output", StringType),
    StructField("status", StringType)
  )))

  private[openai] val ReplayJsonOptions: Map[String, String] =
    Map("ignoreNullFields" -> "true")

  def toFunctionCallOutputs(outputs: Seq[Row]): Vector[Map[String, Any]] = {
    val items = Option(outputs).getOrElse(Seq.empty).zipWithIndex.map { case (row, index) =>
      val callId = Option(row.getAs[String]("call_id")).map(_.trim).getOrElse("")
      require(
        callId.nonEmpty,
        s"function_call_output $index: call_id must be non-blank and unique")
      val output = Option(row.getAs[String]("output")).getOrElse {
        throw new IllegalArgumentException(
          s"function_call_output $index: output must not be null")
      }
      val status = Option(row.getAs[String]("status")).filter(_.nonEmpty)
      ListMap[String, Any](
        "type" -> OpenAIToolUtils.FunctionCallOutputItemType,
        "call_id" -> callId,
        "output" -> output
      ) ++ status.map("status" -> _)
    }.toVector
    val callIds = items.map(_("call_id"))
    require(
      callIds.distinct.size == callIds.size,
      "function_call_output call_id values must be unique")
    items
  }

  def toolCallsColumn(structColName: String): Column = {
    val items = F.col(structColName).getField("output")
    val calls = F.filter(
      items,
      item => item.getField("type") === OpenAIToolUtils.FunctionCallItemType)
    F.transform(calls, (item, index) => F.struct(
      item.getField("call_id").as("call_id"),
      item.getField("id").as("item_id"),
      item.getField("type").as("type"),
      item.getField("name").as("name"),
      item.getField("arguments").as("arguments"),
      item.getField("status").as("status"),
      index.cast(IntegerType).as("index")
    ))
  }

  def replayItemsColumn(structColName: String): Column = {
    val items = F.col(structColName).getField("output")
    val replayable = F.filter(
      items,
      item =>
        item.getField("type") =!= OpenAIToolUtils.ReasoningItemType ||
          item.getField(OpenAIToolUtils.EncryptedContentField).isNotNull)
    F.to_json(replayable, ReplayJsonOptions)
  }
}
