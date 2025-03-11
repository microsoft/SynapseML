// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.schema.SparkBindings
import com.microsoft.azure.synapse.ml.param.ArrayMapJsonProtocol.MapJsonFormat
import org.apache.spark.sql.Row
import spray.json._

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

object OpenAIMessageJsonProtocol extends DefaultJsonProtocol {

  implicit val ArrayEnc: RootJsonFormat[Array[Map[String, Any]]] =
    new RootJsonFormat[Array[Map[String, Any]]] {
      def write(arr: Array[Map[String, Any]]) =
        JsArray(arr.map(MapJsonFormat.write).toVector)
      def read(jsVal: JsValue) = jsVal match {
        case JsArray(elements) =>
          elements.map(MapJsonFormat.read).toArray
        case other => deserializationError("Expected array, got: " + other)
      }
    }
}


object OpenAIMessage {

  def apply(role: String, content: String): OpenAIMessage =
    new OpenAIMessage(role, content, None)

  def apply(role: String, content: String, name: Option[String]): OpenAIMessage =
    new OpenAIMessage(role, content, name)

  def apply(role: String, arr: Array[Map[String, Any]]): OpenAIMessage = {
    import OpenAIMessageJsonProtocol._ // need implicit formats
    val jsonStr = arr.toJson.compactPrint
    new OpenAIMessage(role, jsonStr, None)
  }

  def apply(role: String, arr: Array[Map[String, Any]], name: Option[String]): OpenAIMessage = {
    import OpenAIMessageJsonProtocol._
    val jsonStr = arr.toJson.compactPrint
    new OpenAIMessage(role, jsonStr, name)
  }

  def create(
              role: String,
              content: String,
              name: Option[String]
            ): OpenAIMessage = new OpenAIMessage(role, content, name)
}



case class OpenAIChatChoice(message: OpenAIMessage,
                            index: Long,
                            finish_reason: String)

case class OpenAIUsage(completion_tokens: Long, prompt_tokens: Long, total_tokens: Long)

case class ChatCompletionResponse(id: String,
                                  `object`: String,
                                  created: String,
                                  model: String,
                                  choices: Seq[OpenAIChatChoice],
                                  system_fingerprint: Option[String],
                                  usage: Option[OpenAIUsage])

object ChatCompletionResponse extends SparkBindings[ChatCompletionResponse]

object OpenAIJsonProtocol extends DefaultJsonProtocol {
  implicit val MessageEnc: RootJsonFormat[OpenAIMessage] = jsonFormat3(OpenAIMessage.create)
}
