// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.schema.SparkBindings
import org.apache.spark.sql.types._
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

object EmbeddingUsage extends SparkBindings[EmbeddingUsage]

case class EmbeddingUsage(prompt_tokens: Long,
                          total_tokens: Long)

object EmbeddingResponse extends SparkBindings[EmbeddingResponse]

case class EmbeddingResponse(`object`: String,
                             data: Seq[EmbeddingObject],
                             model: String,
                             usage: Option[EmbeddingUsage])

case class EmbeddingObject(`object`: String,
                           embedding: Array[Double],
                           index: Int)

case class OpenAIMessage(role: String, content: String, name: Option[String] = None)

case class OpenAIChatChoice(message: OpenAIMessage,
                            index: Long,
                            finish_reason: String)

case class TokenDetails(audio_tokens: Option[Long] = None,
                        cached_tokens: Option[Long] = None,
                        reasoning_tokens: Option[Long] = None,
                        accepted_prediction_tokens: Option[Long] = None,
                        rejected_prediction_tokens: Option[Long] = None)

object TokenDetails extends SparkBindings[TokenDetails]

case class ChatUsage(completion_tokens: Long,
                     prompt_tokens: Long,
                     total_tokens: Long,
                     completion_tokens_details: Option[TokenDetails] = None,
                     prompt_tokens_details: Option[TokenDetails] = None)

case class ChatModelResponse(id: String,
                                  `object`: String,
                                  created: String,
                                  model: String,
                                  choices: Seq[OpenAIChatChoice],
                                  system_fingerprint: Option[String],
                                  usage: Option[ChatUsage])

object ChatModelResponse extends SparkBindings[ChatModelResponse]

object OpenAIJsonProtocol extends DefaultJsonProtocol {
  implicit val MessageEnc: RootJsonFormat[OpenAIMessage] = jsonFormat3(OpenAIMessage.apply)
}

case class ResponsesOutputContentComponent(`type`: String, text: String)
case class OpenAIResponsesChoice(content: Seq[ResponsesOutputContentComponent], status: String)

case class ResponsesUsage(output_tokens: Long,
                          input_tokens: Long,
                          total_tokens: Long,
                          output_tokens_details: Option[TokenDetails] = None,
                          input_tokens_details: Option[TokenDetails] = None)

case class ResponsesModelResponse(id: String,
                                  `object`: String,
                                  created_at: String,
                                  model: String,
                                  output: Seq[OpenAIResponsesChoice],
                                  system_fingerprint: Option[String],
                                  usage: Option[ResponsesUsage])

object ResponsesModelResponse extends SparkBindings[ResponsesModelResponse]

private[openai] object OpenAIResponseSchemas {
  // Extend the wire schema without changing public case-class constructors, extractors or encoders.
  private def extend(schema: StructType, path: List[String], additions: StructField*): StructType = path match {
    case Nil => StructType(schema.fields ++ additions)
    case head :: tail =>
      StructType(schema.fields.map { field =>
        if (field.name != head) {
          field
        } else {
          val dataType = field.dataType match {
            case struct: StructType => extend(struct, tail, additions: _*)
            case ArrayType(struct: StructType, nullable) => ArrayType(extend(struct, tail, additions: _*), nullable)
            case other => throw new IllegalArgumentException(s"Cannot extend $head with type $other")
          }
          field.copy(dataType = dataType)
        }
      })
  }

  private val TokenProbability = new StructType()
    .add("token", StringType).add("bytes", ArrayType(IntegerType)).add("logprob", DoubleType)
  private val TokenProbabilities = ArrayType(TokenProbability.add("top_logprobs", ArrayType(TokenProbability)))
  private val CacheWrites = StructField("cache_write_tokens", LongType)
  private val TextTokens = StructField("text_tokens", LongType)
  private val ImageTokens = StructField("image_tokens", LongType)
  private val CommonFields = Seq(
    StructField("service_tier", StringType),
    StructField("metadata", MapType(StringType, StringType))
  )

  lazy val Chat: StructType = {
    val inputUsage = extend(ChatModelResponse.schema, List("usage", "prompt_tokens_details"),
      CacheWrites, TextTokens, ImageTokens)
    val usage = extend(inputUsage, List("usage", "completion_tokens_details"), TextTokens)
    val choices = extend(usage, List("choices"),
      StructField("logprobs", new StructType().add("content", TokenProbabilities).add("refusal", TokenProbabilities)))
    val message = extend(choices, List("choices", "message"), StructField("refusal", StringType))
    extend(message, Nil, CommonFields: _*)
  }

  lazy val Responses: StructType = {
    val usage = extend(ResponsesModelResponse.schema, List("usage", "input_tokens_details"),
      CacheWrites, StructField("cache_write_tokens_details",
        new StructType().add("ttl_30m_tokens", LongType).add("ttl_12h_tokens", LongType)))
    val content = extend(usage, List("output", "content"),
      StructField("logprobs", TokenProbabilities), StructField("refusal", StringType))
    val output = extend(content, List("output"),
      StructField("id", StringType), StructField("type", StringType), StructField("role", StringType),
      StructField("encrypted_content", StringType),
      StructField("summary", ArrayType(new StructType().add("type", StringType).add("text", StringType))))
    val responseFields = Seq(
      StructField("status", StringType),
      StructField("error", new StructType().add("code", StringType).add("message", StringType)),
      StructField("incomplete_details", new StructType().add("reason", StringType)),
      StructField("prompt_cache_key", StringType),
      StructField("prompt_cache_retention", StringType),
      StructField("prompt_cache_options", new StructType()
        .add("mode", StringType).add("ttl", StringType).add("comparison_response_id", StringType)),
      StructField("prompt_cache_diagnostics", new StructType()
        .add("type", StringType).add("reason", StringType).add("cache_missed_tokens", LongType)
        .add("comparison_reusable_tokens", LongType)),
      StructField("reasoning", new StructType()
        .add("effort", StringType).add("context", StringType).add("mode", StringType).add("summary", StringType))
    )
    extend(output, Nil, (CommonFields ++ responseFields): _*)
  }
}

case class OpenAICompositeMessage(
  role: String,
  content: Seq[Map[String, String]],
  name: Option[String] = None
)
