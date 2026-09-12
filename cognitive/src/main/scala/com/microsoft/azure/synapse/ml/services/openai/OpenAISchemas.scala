// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.schema.SparkBindings
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

case class OpenAIChatFunctionCall(name: String, arguments: String)

case class OpenAIChatToolCall(id: String,
                              function: OpenAIChatFunctionCall,
                              `type`: String = "function")

case class OpenAIChatMessage(role: String,
                             content: Option[String] = None,
                             name: Option[String] = None,
                             tool_calls: Option[Seq[OpenAIChatToolCall]] = None,
                             tool_call_id: Option[String] = None)

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

private[openai] case class ChatFunctionCallV2(name: String,
                                              arguments: String)

private[openai] case class ChatCustomToolCallV2(name: String,
                                                input: String)

private[openai] case class ChatMessageToolCallV2(id: String,
                                                 `type`: String,
                                                 function: Option[ChatFunctionCallV2] = None,
                                                 custom: Option[ChatCustomToolCallV2] = None)

private[openai] case class OpenAIChatMessageV2(role: String,
                                               content: String,
                                               name: Option[String] = None,
                                               refusal: Option[String] = None,
                                               tool_calls: Option[Seq[ChatMessageToolCallV2]] = None)

private[openai] case class OpenAIChatChoiceV2(message: OpenAIChatMessageV2,
                                              index: Long,
                                              finish_reason: String)

private[openai] case class ChatModelResponseV2(id: String,
                                               `object`: String,
                                               created: String,
                                               model: String,
                                               choices: Seq[OpenAIChatChoiceV2],
                                               system_fingerprint: Option[String],
                                               usage: Option[ChatUsage])

private[openai] object ChatModelResponseV2 extends SparkBindings[ChatModelResponseV2]

object OpenAIJsonProtocol extends DefaultJsonProtocol {
  implicit val MessageEnc: RootJsonFormat[OpenAIMessage] = jsonFormat3(OpenAIMessage.apply)
}

case class ResponsesOutputContentComponent(`type`: String, text: String)

case class ResponsesSummaryPart(`type`: Option[String] = None,
                                text: Option[String] = None)

case class OpenAIResponsesChoice(content: Seq[ResponsesOutputContentComponent],
                                 status: String)

case class ResponsesUsage(output_tokens: Long,
                          input_tokens: Long,
                          total_tokens: Long,
                          output_tokens_details: Option[TokenDetails] = None,
                          input_tokens_details: Option[TokenDetails] = None)

case class ResponsesIncompleteDetails(reason: Option[String] = None)

case class ResponsesError(code: Option[String] = None,
                          message: Option[String] = None)

case class ResponsesModelResponse(id: String,
                                  `object`: String,
                                  created_at: String,
                                  model: String,
                                  output: Seq[OpenAIResponsesChoice],
                                  system_fingerprint: Option[String],
                                  usage: Option[ResponsesUsage])

object ResponsesModelResponse extends SparkBindings[ResponsesModelResponse]

private[openai] case class TokenDetailsV2(
  audio_tokens: Option[Long] = None,
  cached_tokens: Option[Long] = None,
  reasoning_tokens: Option[Long] = None,
  accepted_prediction_tokens: Option[Long] = None,
  rejected_prediction_tokens: Option[Long] = None,
  cache_write_tokens: Option[Long] = None)

private[openai] case class ResponsesOutputContentComponentV2(
  `type`: String,
  text: String,
  refusal: Option[String] = None)

private[openai] case class OpenAIResponsesChoiceV2(
  content: Seq[ResponsesOutputContentComponentV2],
  status: String,
  `type`: Option[String] = None,
  id: Option[String] = None,
  role: Option[String] = None,
  phase: Option[String] = None,
  call_id: Option[String] = None,
  name: Option[String] = None,
  arguments: Option[String] = None,
  summary: Option[Seq[ResponsesSummaryPart]] = None,
  encrypted_content: Option[String] = None)

private[openai] case class ResponsesUsageV2(
  output_tokens: Long,
  input_tokens: Long,
  total_tokens: Long,
  output_tokens_details: Option[TokenDetailsV2] = None,
  input_tokens_details: Option[TokenDetailsV2] = None)

private[openai] case class ResponsesModelResponseV2(
  id: String,
  `object`: String,
  created_at: String,
  model: String,
  output: Seq[OpenAIResponsesChoiceV2],
  system_fingerprint: Option[String],
  usage: Option[ResponsesUsageV2],
  status: Option[String] = None,
  incomplete_details: Option[ResponsesIncompleteDetails] = None,
  error: Option[ResponsesError] = None,
  output_text: Option[String] = None)

private[openai] object ResponsesModelResponseV2 extends SparkBindings[ResponsesModelResponseV2]

case class OpenAICompositeMessage(
  role: String,
  content: Seq[Map[String, String]],
  name: Option[String] = None
)
