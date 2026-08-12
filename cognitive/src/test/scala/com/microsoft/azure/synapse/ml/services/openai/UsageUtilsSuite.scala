// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, struct}

class UsageUtilsSuite extends TestBase {

  import spark.implicits._

  private def normalizeUsage(df: DataFrame, mapping: UsageUtils.UsageFieldMapping): Row = {
    df.select(UsageUtils.normalize(col("usage"), mapping).alias("usage")).head().getAs[Row]("usage")
  }

  test("normalize maps chat completion usage fields and nested details") {
    val usageDf = Seq((1L, 2L, 3L, 10L, 11L, 21L, 22L, 23L, 24L))
      .toDF(
        "prompt_tokens",
        "completion_tokens",
        "total_tokens",
        "audio_tokens",
        "cached_tokens",
        "accepted_prediction_tokens",
        "completion_audio_tokens",
        "reasoning_tokens",
        "rejected_prediction_tokens"
      )
      .withColumn("usage", struct(
        col("prompt_tokens"),
        col("completion_tokens"),
        col("total_tokens"),
        struct(col("audio_tokens"), col("cached_tokens")).alias("prompt_tokens_details"),
        struct(
          col("accepted_prediction_tokens"),
          col("completion_audio_tokens").alias("audio_tokens"),
          col("reasoning_tokens"),
          col("rejected_prediction_tokens")
        ).alias("completion_tokens_details")
      ))
      .select("usage")

    val normalized = normalizeUsage(usageDf, UsageUtils.UsageMappings.ChatCompletions)
    assert(normalized.getAs[Long]("input_tokens") == 1L)
    assert(normalized.getAs[Long]("output_tokens") == 2L)
    assert(normalized.getAs[Long]("total_tokens") == 3L)
    assert(
      normalized.getMap[String, Long](normalized.fieldIndex("input_token_details")) ==
        Map("audio_tokens" -> 10L, "cached_tokens" -> 11L)
    )
    assert(
      normalized.getMap[String, Long](normalized.fieldIndex("output_token_details")) ==
        Map(
          "accepted_prediction_tokens" -> 21L,
          "audio_tokens" -> 22L,
          "reasoning_tokens" -> 23L,
          "rejected_prediction_tokens" -> 24L
        )
    )
  }

  test("normalize maps responses usage fields and details") {
    val usageDf = Seq((4L, 5L, 9L, 2L, 3L))
      .toDF("input_tokens", "output_tokens", "total_tokens", "cached_tokens", "reasoning_tokens")
      .withColumn("usage", struct(
        col("input_tokens"),
        col("output_tokens"),
        col("total_tokens"),
        struct(col("cached_tokens")).alias("input_tokens_details"),
        struct(col("reasoning_tokens")).alias("output_tokens_details")
      ))
      .select("usage")

    val normalized = normalizeUsage(usageDf, UsageUtils.UsageMappings.Responses)
    assert(normalized.getAs[Long]("input_tokens") == 4L)
    assert(normalized.getAs[Long]("output_tokens") == 5L)
    assert(normalized.getAs[Long]("total_tokens") == 9L)
    assert(normalized.getMap[String, Long](normalized.fieldIndex("input_token_details")) == Map("cached_tokens" -> 2L))
    assert(normalized.getMap[String, Long](normalized.fieldIndex("output_token_details")) ==
      Map("reasoning_tokens" -> 3L))
  }

  test("normalize handles missing or empty detail mappings") {
    val embeddingDf = Seq((7L, 7L))
      .toDF("prompt_tokens", "total_tokens")
      .withColumn("usage", struct(col("prompt_tokens"), col("total_tokens")))
      .select("usage")
    val embeddingUsage = normalizeUsage(embeddingDf, UsageUtils.UsageMappings.Embeddings)
    assert(embeddingUsage.getAs[Long]("input_tokens") == 7L)
    assert(embeddingUsage.isNullAt(embeddingUsage.fieldIndex("output_tokens")))
    assert(embeddingUsage.getMap[String, Long](embeddingUsage.fieldIndex("input_token_details")).isEmpty)
    assert(embeddingUsage.getMap[String, Long](embeddingUsage.fieldIndex("output_token_details")).isEmpty)

    val customMapping = UsageUtils.UsageFieldMapping(
      inputTokens = Some("input_tokens"),
      outputTokens = None,
      totalTokens = Some("total_tokens"),
      inputDetails = Some("input_tokens_details" -> Seq.empty),
      outputDetails = None
    )
    val customDf = Seq((8L, 8L, 4L))
      .toDF("input_tokens", "total_tokens", "cached_tokens")
      .withColumn("usage", struct(
        col("input_tokens"),
        col("total_tokens"),
        struct(col("cached_tokens")).alias("input_tokens_details")
      ))
      .select("usage")

    val customUsage = normalizeUsage(customDf, customMapping)
    assert(customUsage.getAs[Long]("input_tokens") == 8L)
    assert(customUsage.getMap[String, Long](customUsage.fieldIndex("input_token_details")).isEmpty)
  }
}
