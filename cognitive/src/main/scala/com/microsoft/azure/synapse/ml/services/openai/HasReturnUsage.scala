// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import org.apache.spark.ml.param.{BooleanParam, Params}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{array, lit, map_from_arrays, struct, typedLit}
import org.apache.spark.sql.types.{LongType, MapType, StringType, StructField, StructType}

trait HasReturnUsage extends Params {
  import HasReturnUsage._

  final val returnUsage: BooleanParam = new BooleanParam(
    this,
    "returnUsage",
    "Whether to include usage statistics alongside the response output."
  )

  setDefault(returnUsage -> false)

  final def getReturnUsage: Boolean = $(returnUsage)

  final def setReturnUsage(value: Boolean): this.type = set(returnUsage, value)

  protected def usageStructType: StructType = UsageStructType

  protected def normalizeUsageColumn(column: Column,
                                     mapping: UsageFieldMapping): Column =
    HasReturnUsage.normalize(column, mapping)

  protected def emptyUsageColumn: Column = HasReturnUsage.EmptyUsageColumn
}

object HasReturnUsage {

  final case class UsageFieldMapping(inputTokens: Option[String],
                                     outputTokens: Option[String],
                                     totalTokens: Option[String] = Some("total_tokens"),
                                     inputDetails: Option[(String, Seq[String])] = None,
                                     outputDetails: Option[(String, Seq[String])] = None) extends Serializable

  val UsageStructType: StructType = StructType(Seq(
    StructField("input_tokens", LongType, nullable = true),
    StructField("output_tokens", LongType, nullable = true),
    StructField("total_tokens", LongType, nullable = true),
    StructField("input_token_details", MapType(StringType, LongType, valueContainsNull = true), nullable = false),
    StructField("output_token_details", MapType(StringType, LongType, valueContainsNull = true), nullable = false)
  ))

  private val NullLongColumn: Column = lit(0L).cast(LongType)
  private val EmptyDetailsMapColumn: Column = typedLit(Map.empty[String, Long])

  private def tokenValue(usage: Column, nameOpt: Option[String]): Column =
    nameOpt.map(name => usage.getField(name).cast(LongType)).getOrElse(NullLongColumn)

  private def detailMap(usage: Column, detailsOpt: Option[(String, Seq[String])]): Column =
    detailsOpt match {
      case Some((fieldName, keys)) if keys.nonEmpty =>
        val detailCol = usage.getField(fieldName)
        val keyColumns = keys.map(lit)
        val valueColumns = keys.map(key => detailCol.getField(key).cast(LongType))
        map_from_arrays(array(keyColumns: _*), array(valueColumns: _*))
      case _ => EmptyDetailsMapColumn
    }

  private[openai] def normalize(usage: Column, mapping: UsageFieldMapping): Column = {
    val inputTokensCol = tokenValue(usage, mapping.inputTokens)
    val outputTokensCol = tokenValue(usage, mapping.outputTokens)
    val totalTokensCol = tokenValue(usage, mapping.totalTokens)
    val inputDetailsCol = detailMap(usage, mapping.inputDetails)
    val outputDetailsCol = detailMap(usage, mapping.outputDetails)

    struct(
      inputTokensCol.alias("input_tokens"),
      outputTokensCol.alias("output_tokens"),
      totalTokensCol.alias("total_tokens"),
      inputDetailsCol.alias("input_token_details"),
      outputDetailsCol.alias("output_token_details")
    ).cast(UsageStructType)
  }

  private[openai] val EmptyUsageColumn: Column = struct(
    NullLongColumn.alias("input_tokens"),
    NullLongColumn.alias("output_tokens"),
    NullLongColumn.alias("total_tokens"),
    EmptyDetailsMapColumn.alias("input_token_details"),
    EmptyDetailsMapColumn.alias("output_token_details")
  ).cast(UsageStructType)
}
