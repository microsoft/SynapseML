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

case class OpenAIMessage(
                                role: String,
                                content: Option[String] = None,
                                contentList: Option[Seq[OpenAIContentItem]] = None,
                                name: Option[String] = None
                              )
case class ImageUrl(url: String)

case class OpenAIContentItem(`type`: String,
                             text: Option[String] = None,
                             image_url: Option[ImageUrl] = None)

object OpenAIMessage {

  def apply(role: String, content: String): OpenAIMessage =
    new OpenAIMessage(role, content = Some(content), name = None)

  def apply(role: String, content: String, name: Option[String]): OpenAIMessage =
    new OpenAIMessage(role, content = Some(content), name = name)

  def apply(role: String, seq: Seq[OpenAIContentItem]): OpenAIMessage = {
    new OpenAIMessage(role, contentList = Some(seq), name = None)
  }

  def apply(role: String, seq: Seq[OpenAIContentItem], name: Option[String]): OpenAIMessage = {
    new OpenAIMessage(role, contentList = Some(seq), name = name)
  }

  def create(
              role: String,
              content: Option[String],
              contentList: Option[Seq[OpenAIContentItem]],
              name: Option[String]
            ): OpenAIMessage = new OpenAIMessage(role, content, contentList, name)
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
  implicit val ImageUrlEnc: RootJsonFormat[ImageUrl] = jsonFormat1(ImageUrl)
  implicit val OpenAIContentItemEnc: RootJsonFormat[OpenAIContentItem] = jsonFormat3(OpenAIContentItem)

  implicit object MessageEnc extends RootJsonFormat[OpenAIMessage] {
    override def write(msg: OpenAIMessage): JsValue = {
      val baseFields = Map(
        "role" -> JsString(msg.role)
      ) ++ msg.name.map("name" -> JsString(_))

      // Decide how to write 'content':
      val contentField: (String, JsValue) = (msg.content, msg.contentList) match {
        case (Some(text), None) =>
          "content" -> JsString(text)

        case (None, Some(items)) =>
          "content" -> JsArray(items.map(_.toJson).toVector)

        case (None, None) =>
          // how can we put these errors in the Error col?
          //serializationError("OpenAIMessage CANNOT have both content & contentItems")
          "content" -> JsString("")

        case (Some(_), Some(_)) =>
          "content" -> JsString("")
          //serializationError("OpenAIMessage cannot have both content & contentItems")
      }

      JsObject(baseFields + contentField)
    }

    override def read(json: JsValue): OpenAIMessage = {
      val obj = json.asJsObject
      val role = obj.fields("role").convertTo[String]
      val name = obj.fields.get("name").map(_.convertTo[String])

      val contentJs = obj.fields.getOrElse("content", JsString(""))

      contentJs match {
        case JsString(s) =>
          OpenAIMessage(role, content = Some(s), contentList = None, name = name)

        case JsArray(elements) =>
          val items = elements.map(_.convertTo[OpenAIContentItem])
          OpenAIMessage(role, content = None, contentList = Some(items), name = name)

        case _ =>
          deserializationError("content must be string or array")
      }
    }
  }
}
