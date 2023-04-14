// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive.openai

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

case class ChatCompletionResponse(id: String,
                                  `object`: String,
                                  created: String,
                                  model: String,
                                  choices: Seq[OpenAIChatChoice])

object ChatCompletionResponse extends SparkBindings[ChatCompletionResponse]

object OpenAIJsonProtocol extends DefaultJsonProtocol {
  implicit val MessageEnc: RootJsonFormat[OpenAIMessage] = jsonFormat3(OpenAIMessage.apply)
}
