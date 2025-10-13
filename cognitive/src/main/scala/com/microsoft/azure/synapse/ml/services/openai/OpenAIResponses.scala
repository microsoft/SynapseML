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
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.JavaConverters._
import scala.language.existentials
import com.microsoft.azure.synapse.ml.services.HasCustomHeaders

object OpenAIResponseFormat extends Enumeration {
  case class ResponseFormat(paylodName: String, prompt: String) extends super.Val(paylodName)

  val TEXT: ResponseFormat = ResponseFormat("text", "Output must be in text format")
  val JSON: ResponseFormat = ResponseFormat("json_object", "Output must be in JSON format")

  def asStringSet: Set[String] =
    OpenAIResponseFormat.values.map(_.asInstanceOf[OpenAIResponseFormat.ResponseFormat].paylodName)

  // Note: 'json_schema' is intentionally not represented here because it does not
  // require a system prompt injection. We still accept it in validation elsewhere.
  def fromResponseFormatString(format: String): OpenAIResponseFormat.ResponseFormat = {
    if (TEXT.paylodName == format) {
      TEXT
    } else if (JSON.paylodName == format) {
      JSON
    } else {
      throw new IllegalArgumentException("Response format must be valid for OpenAI API. " +
        "Currently supported formats are " + asStringSet.mkString(", "))
    }
  }
}

trait HasOpenAITextParamsResponses extends HasOpenAITextParams {
  // Responses API accepts 'text', 'json_object', and now 'json_schema'.
  val responseFormat: ServiceParam[Map[String, Any]] = new ServiceParam[Map[String, Any]](
    this,
    "responseFormat",
    "Response format. Can be 'json_object', 'json_schema', or 'text'. For 'json_schema' also provide key 'json_schema' with nested schema.",
    isRequired = false) {
    override val payloadName: String = "response_format"
  }

  def getResponseFormat: Map[String, Any] = getScalarParam(responseFormat)

  def setResponseFormat(value: Map[String, Any]): this.type = {
    // Accept formats: text, json_object, json_schema (with nested json_schema)
    if (value == null || !value.contains("type") || value("type") == null ||
      value("type").toString.trim.isEmpty) {
      throw new IllegalArgumentException(
        "Response format map must contain non-empty key 'type' with one of 'text', 'json_object', or 'json_schema'.")
    }

    val tpe = value("type").toString.toLowerCase
    tpe match {
      case "text" | "json_object" =>
        // Allow additional keys to pass through; service will ignore unknown keys.
        setScalarParam(responseFormat, value)
      case "json_schema" =>
        if (!value.contains("json_schema")) {
          throw new IllegalArgumentException(
            "When type == 'json_schema', key 'json_schema' must be provided.")
        }
        setScalarParam(responseFormat, value)
      case _ =>
        throw new IllegalArgumentException(
          "Response format must be valid for OpenAI API. Currently supported formats are text, json_object, json_schema.")
    }
  }

  def setResponseFormat(value: String): this.type = {
    if (value == null || value.trim.isEmpty) {
      this
    } else {
      val trimmed = value.trim.toLowerCase
      if (trimmed == "json_schema") {
        throw new IllegalArgumentException(
          "To use json_schema pass a dict (Python) or Map (Scala) that complies with OpenAI responses response_format. " +
          "Example: setResponseFormat(Map(\"type\"->\"json_schema\", ...))")
      }
      setResponseFormat(Map("type" -> trimmed))
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

object OpenAIResponses extends ComplexParamsReadable[OpenAIResponses]

class OpenAIResponses(override val uid: String) extends OpenAIServicesBase(uid)
  with HasOpenAITextParamsResponses with HasMessagesInput with HasCognitiveServiceInput
  with HasInternalJsonOutputParser with SynapseMLLogging with HasCustomHeaders
  with HasRAIContentFilter with HasTextOutput {
  logClass(FeatureNames.AiServices.OpenAI)

  def this() = this(Identifiable.randomUID("OpenAIResponses"))

  def urlPath: String = ""

  override private[ml] def internalServiceType: String = "openai"

  override def setCustomServiceName(v: String): this.type = {
    setUrl(s"https://$v.openai.azure.com/" + urlPath.stripPrefix("/"))
  }

  override protected def prepareUrlRoot: Row => String = { row =>
    s"${getUrl}openai/responses"
  }

  override protected[openai] def prepareEntity: Row => Option[AbstractHttpEntity] = {
    r =>
      lazy val optionalParams: Map[String, Any] = getOptionalParams(r)
      val messages = r.getAs[Seq[Row]](getMessagesCol)
      Some(getStringEntity(messages, optionalParams))
  }

  override private[ml] def getOptionalParams(r: Row): Map[String, Any] = {
    val params = super.getOptionalParams(r)
    getValueOpt(r, deploymentName) match {
      case Some(modelName) if modelName != null && modelName.nonEmpty =>
        params.updated("model", modelName)
      case _ => params
    }
  }

  override val subscriptionKeyHeaderName: String = "api-key"

  override def shouldSkip(row: Row): Boolean =
    super.shouldSkip(row) || Option(row.getAs[Row](getMessagesCol)).isEmpty

  override protected def getVectorParamMap: Map[String, String] = super.getVectorParamMap
    .updated("input", getMessagesCol)

  override def responseDataType: DataType = ResponsesModelResponse.schema

  private[openai] def getStringEntity(messages: Seq[Row], optionalParams: Map[String, Any]): StringEntity = {
    val mappedMessages = encodeMessagesToMap(messages)
      .map(_.filter { case (_, value) => value != null })
    val fullPayload = optionalParams.updated("input", mappedMessages)
    new StringEntity(fullPayload.toJson.compactPrint, ContentType.APPLICATION_JSON)
  }

  override private[openai] def getOutputMessageText(outputColName: String): org.apache.spark.sql.Column = {
    F.element_at(F.element_at(F.col(outputColName).getField("output"), 1)
      .getField("content"), 1).getField("text")
  }

  override private[openai] def isContentFiltered(outputRow: Row): Boolean = {
    val result = ResponsesModelResponse.makeFromRowConverter(outputRow)
    val firstOutput = result.output.head
    Option(firstOutput.content).isEmpty
  }

  override private[openai] def getFilterReason(outputRow: Row): String = {
    val result = ResponsesModelResponse.makeFromRowConverter(outputRow)
    result.output.head.status
  }

}
