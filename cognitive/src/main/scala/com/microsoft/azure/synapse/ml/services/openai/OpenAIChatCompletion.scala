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

object OpenAIChatCompletion extends ComplexParamsReadable[OpenAIChatCompletion]

object OpenAIResponseFormat extends Enumeration {
  case class ResponseFormat(name: String, prompt: String) extends super.Val(name)
  val TEXT: ResponseFormat = ResponseFormat("text", "Output must be in text format")
  val JSON: ResponseFormat = ResponseFormat("json_object", "Output must be in JSON format")
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
    if (!OpenAIResponseFormat.values.map(_.asInstanceOf[OpenAIResponseFormat.ResponseFormat].name)
      .contains(value("type"))) {
      throw new IllegalArgumentException("Response format must be 'text' or 'json_object'")
    }
    setScalarParam(responseFormat, value)
  }

  def setResponseFormat(value: String): this.type = {
    if (value.isEmpty) {
      this
    } else {
      val normalizedValue = value.toLowerCase match {
        case "json" => "json_object"
        case other => other
      }
      // Validate the normalized value using the OpenAIResponseFormat enum
      if (!OpenAIResponseFormat.values
        .map(_.asInstanceOf[OpenAIResponseFormat.ResponseFormat].name)
        .contains(normalizedValue)) {
        throw new IllegalArgumentException("Response format must be valid for OpenAI API. " +
          "Currently supported formats are " + OpenAIResponseFormat.values
          .map(_.asInstanceOf[OpenAIResponseFormat.ResponseFormat].name)
          .mkString(", "))
      }

      setScalarParam(responseFormat, Map("type" -> normalizedValue))
    }
  }

  def setResponseFormat(value: OpenAIResponseFormat.ResponseFormat): this.type = {
    // this method should throw an excption if the openAiCompletion is not a ChatCompletion
    this.setResponseFormat(value.name)
  }

  def getResponseFormatCol: String = getVectorParam(responseFormat)

  def setResponseFormatCol(value: String): this.type = setVectorParam(responseFormat, value)


  // Recreating the sharedTextParams sequence to include additional parameter responseFormat
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
    responseFormat // Additional parameter
  )
}

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

  private[this] def getStringEntity(messages: Seq[Row], optionalParams: Map[String, Any]): StringEntity = {
    var mappedMessages: Seq[Map[String, String]] = messages.map { m =>
      Seq("role", "content", "name").map(n =>
        n -> Option(m.getAs[String](n))
      ).toMap.filter(_._2.isDefined).mapValues(_.get)
    }

    // if the optionalParams contains "response_format" key, and it's value contains "json_object",
    // then we need to add a message to instruct openAI to return the response in JSON format
    if (optionalParams.get("response_format")
                      .exists(_.asInstanceOf[Map[String, String]]("type")
                      .contentEquals("json_object"))) {
      mappedMessages :+= Map("role" -> "system", "content" -> OpenAIResponseFormat.JSON.prompt)
    }

    val fullPayload = optionalParams.updated("messages", mappedMessages)
    new StringEntity(fullPayload.toJson.compactPrint, ContentType.APPLICATION_JSON)
  }

}



