// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{array, lit, map_from_arrays, struct, typedLit}
import org.apache.spark.sql.types.{LongType, MapType, StringType, StructField, StructType}

/**
 * Utility object for normalizing usage statistics from different OpenAI API responses
 * into a consistent schema format.
 */
object UsageUtils {

  final case class UsageFieldMapping(inputTokens: Option[String],
                                     outputTokens: Option[String],
                                     totalTokens: Option[String] = Some("total_tokens"),
                                     inputDetails: Option[(String, Seq[String])] = None,
                                     outputDetails: Option[(String, Seq[String])] = None) extends Serializable

  object UsageMappings {
    val ChatCompletions: UsageFieldMapping = UsageFieldMapping(
      inputTokens = Some("prompt_tokens"),
      outputTokens = Some("completion_tokens"),
      totalTokens = Some("total_tokens"),
      inputDetails = Some(
        "prompt_tokens_details" -> Seq("audio_tokens", "cached_tokens")
      ),
      outputDetails = Some(
        "completion_tokens_details" ->
          Seq(
            "accepted_prediction_tokens",
            "audio_tokens",
            "reasoning_tokens",
            "rejected_prediction_tokens"
          )
      )
    )

    val Responses: UsageFieldMapping = UsageFieldMapping(
      inputTokens = Some("input_tokens"),
      outputTokens = Some("output_tokens"),
      totalTokens = Some("total_tokens"),
      inputDetails = Some("input_tokens_details" -> Seq("cached_tokens")),
      outputDetails = Some("output_tokens_details" -> Seq("reasoning_tokens"))
    )

    val Embeddings: UsageFieldMapping = UsageFieldMapping(
      inputTokens = Some("prompt_tokens"),
      outputTokens = None,
      totalTokens = Some("total_tokens"),
      inputDetails = None,
      outputDetails = None
    )
  }

  val UsageStructType: StructType = StructType(Seq(
    StructField("input_tokens", LongType, nullable = true),
    StructField("output_tokens", LongType, nullable = true),
    StructField("total_tokens", LongType, nullable = true),
    StructField("input_token_details", MapType(StringType, LongType, valueContainsNull = true), nullable = false),
    StructField("output_token_details", MapType(StringType, LongType, valueContainsNull = true), nullable = false)
  ))

  private val NullLongColumn: Column = lit(null).cast(LongType)  // scalastyle:ignore null
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

  /**
   * Normalizes usage information from an OpenAI API response into a consistent schema.
   *
   * @param usage The usage column from the API response
   * @param mapping The field mapping for the specific API type
   * @return A column with normalized usage struct
   */
  def normalize(usage: Column, mapping: UsageFieldMapping): Column = {
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
}
