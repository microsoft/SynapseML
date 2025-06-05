// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.AnyJsonFormat.anyFormat
import com.microsoft.azure.synapse.ml.param.ServiceParam
import com.microsoft.azure.synapse.ml.services.{HasCognitiveServiceInput, HasInternalJsonOutputParser}
import org.apache.http.entity.{AbstractHttpEntity, ContentType, StringEntity}
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.util._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.language.existentials

object OpenAIResponseFormat extends Enumeration {
  case class ResponseFormat(paylodName: String, prompt: String) extends super.Val(paylodName)

  val TEXT: ResponseFormat = ResponseFormat("text", "Output must be in text format")
  val JSON: ResponseFormat = ResponseFormat("json_object", "Output must be in JSON format")

  def asStringSet: Set[String] =
    OpenAIResponseFormat.values.map(_.asInstanceOf[OpenAIResponseFormat.ResponseFormat].paylodName)

  def fromResponseFormatString(format: String): OpenAIResponseFormat.ResponseFormat = {
    if (TEXT.paylodName== format) {
      TEXT
    } else if (JSON.paylodName == format) {
      JSON
    } else {
      throw new IllegalArgumentException("Response format must be valid for OpenAI API. " +
                                         "Currently supported formats are " +
                                         asStringSet.mkString(", "))
    }
  }
}

trait HasOpenAITextParamsExtended extends HasOpenAITextParams {
  val responseFormat: ServiceParam[Map[String, String]] = new ServiceParam[Map[String, String]](
    this,
    "responseFormat",
    "Response format for the completion. Can be 'json_object' or 'text'.",
    isRequired = false) {
    override val payloadName: String = "response_format"
  }

  def getResponseFormat: Map[String, String] = getScalarParam(responseFormat)

  def setResponseFormat(value: Map[String, String]): this.type = {
    val allowedFormat = OpenAIResponseFormat.asStringSet

    // This test is to validate that value is properly formatted Map('type' -> '<format>')
    if (value == null || value.size !=1  || !value.contains("type") || value("type").isEmpty) {
      throw new IllegalArgumentException("Response format map must of the form Map('type' -> '<format>')"
                                           + " where <format> is one of " + allowedFormat.mkString(", "))
    }

    // This test is to validate that the format is one of the allowed formats
    if (!allowedFormat.contains(value("type").toLowerCase)) {
      throw new IllegalArgumentException("Response format must be valid for OpenAI API. " +
                                           "Currently supported formats are " +
                                           allowedFormat.mkString(", "))
    }
    setScalarParam(responseFormat, value)
  }

  def setResponseFormat(value: String): this.type = {
    if (value == null || value.isEmpty) {
      this
    } else {
      setResponseFormat(Map("type" -> value.toLowerCase))
    }
  }

  def setResponseFormat(value: OpenAIResponseFormat.ResponseFormat): this.type = {
    setScalarParam(responseFormat, Map("type" -> value.paylodName))
  }

  // override this field to include the new parameter
  override private[openai] val sharedTextParams: Seq[ServiceParam[_]] = Seq(
    maxTokens,
    temperature,
    topP,
    user,
    n,
    echo,
    stop,
    cacheLevel,
    presencePenalty,
    frequencyPenalty,
    bestOf,
    logProbs,
    responseFormat
    )
}

object OpenAIChatCompletion extends ComplexParamsReadable[OpenAIChatCompletion]

class OpenAIChatCompletion(override val uid: String) extends OpenAIServicesBase(uid)
  with HasOpenAITextParamsExtended with HasMessagesInput with HasCognitiveServiceInput
  with HasInternalJsonOutputParser with SynapseMLLogging {
  logClass(FeatureNames.AiServices.OpenAI)

  def this() = this(Identifiable.randomUID("OpenAIChatCompletion"))

  def urlPath: String = ""

  override private[ml] def internalServiceType: String = "openai"

  override def setCustomServiceName(v: String): this.type = {
    setUrl(s"https://$v.openai.azure.com/" + urlPath.stripPrefix("/"))
  }

  override protected def prepareUrlRoot: Row => String = { row =>
    s"${getUrl}openai/deployments/${getValue(row, deploymentName)}/chat/completions"
  }

  override protected[openai] def prepareEntity: Row => Option[AbstractHttpEntity] = {
    r =>
      lazy val optionalParams: Map[String, Any] = getOptionalParams(r)
      val messages = r.getAs[Seq[Row]](getMessagesCol)
      Some(getStringEntity(messages, optionalParams))
  }

  override val subscriptionKeyHeaderName: String = "api-key"

  override def shouldSkip(row: Row): Boolean =
    super.shouldSkip(row) || Option(row.getAs[Row](getMessagesCol)).isEmpty

  override protected def getVectorParamMap: Map[String, String] = super.getVectorParamMap
    .updated("messages", getMessagesCol)

  override def responseDataType: DataType = ChatCompletionResponse.schema

  private[openai] def getStringEntity(
                                       messages: Seq[Row],
                                       optionalParams: Map[String, Any]
                                     ): StringEntity = {
    import OpenAIJsonProtocol._

    val mappedMessages = messages.map { row =>
      val role  = row.getAs[String]("role")
      val name  = row.getAs[String]("name")

      val maybeContent = Option(row.getAs[String]("content"))
      val maybeItems   = Option(row.getAs[Seq[Row]]("contentList")).map { rows =>
        rows.map { r =>
          val ctype = r.getAs[String]("type")
          val text  = r.getAs[String]("text")
          val imgUrlRow = r.getAs[Row]("image_url")
          val maybeImgUrl = Option(imgUrlRow).map { irow =>
            ImageUrl(irow.getAs[String]("url"))
          }
          OpenAIContentItem(ctype, Option(text), maybeImgUrl)
        }
      }

      OpenAIMessage(
        role     = role,
        content  = maybeContent,
        contentList = maybeItems,
        name     = Option(name)
      )
    }

    val messagesJson: JsValue = mappedMessages.toJson
    val paramsObj = optionalParams.toJson.asJsObject
    val jsonString = JsObject(paramsObj.fields + ("messages" -> messagesJson)).compactPrint
    new StringEntity(jsonString, ContentType.APPLICATION_JSON)
  }

}



