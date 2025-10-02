// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.schema.SparkBindings
import org.apache.spark.sql.Row
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

object CompletionResponse extends SparkBindings[CompletionResponse]

case class CompletionResponse(id: String,
                              `object`: String,
                              created: String,
                              model: String,
                              choices: Seq[OpenAIChoice])

case class OpenAIChoice(text: String,
                        index: Long,
                        logprobs: Option[OpenAILogProbs],
                        finish_reason: String)

case class OpenAILogProbs(tokens: Seq[String],
                          token_logprobs: Seq[Double],
                          top_logprobs: Seq[Map[String, Double]],
                          text_offset: Seq[Long])

object EmbeddingResponse extends SparkBindings[EmbeddingResponse]

case class EmbeddingResponse(`object`: String,
                             data: Seq[EmbeddingObject],
                             model: String)

case class EmbeddingObject(`object`: String,
                           embedding: Array[Double],
                           index: Int)

case class OpenAIMessage(role: String, content: String, name: Option[String] = None)

case class OpenAIChatChoice(message: OpenAIMessage,
                            index: Long,
                            finish_reason: String)

case class ChatUsage(completion_tokens: Long, prompt_tokens: Long, total_tokens: Long)

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

case class ResponsesUsage(output_tokens: Long, input_tokens: Long, total_tokens: Long)

case class ResponsesModelResponse(id: String,
                                  `object`: String,
                                  created_at: String,
                                  model: String,
                                  output: Seq[OpenAIResponsesChoice],
                                  system_fingerprint: Option[String],
                                  usage: Option[ResponsesUsage])

object ResponsesModelResponse extends SparkBindings[ResponsesModelResponse]

case class OpenAICompositeMessage(
  role: String,
  content: Seq[Map[String, String]],
  name: Option[String] = None
)
