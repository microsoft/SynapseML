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
import org.apache.spark.sql.{functions => F, Row}
import org.apache.spark.sql.types._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.language.existentials


trait HasOpenAITextParamsExtended extends HasOpenAITextParams {
  // Updated to Map[String, Any] to allow nested json_schema structure
  val responseFormat: ServiceParam[Map[String, Any]] = new ServiceParam[Map[String, Any]](
    this,
    "responseFormat",
    "Response format for the completion. Can be 'json_object', 'json_schema', or 'text'. " +
      "For 'json_schema' you must also provide key 'json_schema' with nested JSON schema definition.",
    isRequired = false) {
    override val payloadName: String = "response_format"
  }

  def getResponseFormat: Map[String, Any] = getScalarParam(responseFormat)

  def setResponseFormat(value: Map[String, Any]): this.type = {
    requireValidResponseFormatType(value)
    if (isJsonSchema(value)) {
      requireValidJsonSchemaShape(value)
    }
    setScalarParam(responseFormat, value)
  }

  private def isJsonSchema(value: Map[String, Any]): Boolean =
    value.get("type").exists(_.toString.equalsIgnoreCase("json_schema"))

  private def requireValidResponseFormatType(value: Map[String, Any]): Unit = {
    val tOpt = value.get("type").map(_.toString.trim.toLowerCase)
    if (tOpt.isEmpty || tOpt.exists(_.isEmpty)) {
      throw new IllegalArgumentException("response_format map must contain non-empty key 'type'")
    }
    val tpe = tOpt.get
    val ok = tpe == "text" || tpe == "json_object" || tpe == "json_schema"
    if (!ok) {
      val msg = "Unsupported response_format type. Use 'text', 'json_object', or 'json_schema'."
      throw new IllegalArgumentException(msg)
    }
  }

  private def requireValidJsonSchemaShape(value: Map[String, Any]): Unit = {
    val hasNested = value.contains("json_schema")
    val hasFlat = value.contains("name") && value.contains("schema")
    if (!hasNested && !hasFlat) {
      val msg = "json_schema requires nested 'json_schema' or top-level 'name' and 'schema'."
      throw new IllegalArgumentException(msg)
    }
  }

  // Supported String values: "text", "json_object".
  // For "json_schema" caller must use Map form with full structure (no parsing performed here).
  // Any other String value is rejected to avoid implicit parsing/assumptions.
  def setResponseFormat(value: String): this.type = {
    Option(value).map(_.trim).filter(_.nonEmpty) match {
      case None => this
      case Some(trimmed) =>
        if (trimmed.equalsIgnoreCase("json_schema")) {
          val msgParts = Seq(
            "To use json_schema pass a dict (Python) or Map (Scala) with required fields.",
            "Chat: Map('type'->'json_schema','json_schema'-> {...});",
            "Responses: Map('type'->'json_schema','name'->...,'schema'-> {...})"
          )
          throw new IllegalArgumentException(msgParts.mkString(" "))
        }
        trimmed.toLowerCase match {
          case "text" | "json_object" =>
            setResponseFormat(Map("type" -> trimmed.toLowerCase))
          case _ =>
            throw new IllegalArgumentException(
              "Unsupported response_format String. Use 'text', 'json_object', or a Map for 'json_schema'.")
        }
    }
  }

  // Enumeration overload removed; users pass String or Map[String, Any].

  def getResponseFormatType: String = Option(getResponseFormat)
    .flatMap(m => Option(m.getOrElse("type", "").toString))
    .getOrElse("")

  // override this field to include the new parameters
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
    verbosity,
    reasoningEffort,
    responseFormat,
    seed
  )
}

object OpenAIChatCompletion extends ComplexParamsReadable[OpenAIChatCompletion]

class OpenAIChatCompletion(override val uid: String) extends OpenAIServicesBase(uid)
  with HasOpenAITextParamsExtended with HasMessagesInput with HasCognitiveServiceInput
  with HasInternalJsonOutputParser with SynapseMLLogging with HasRAIContentFilter with HasTextOutput {
  logClass(FeatureNames.AiServices.OpenAI)

  def this() = this(Identifiable.randomUID("OpenAIChatCompletion"))

  def urlPath: String = ""

  override private[ml] def internalServiceType: String = "openai"

  // Default API version for Chat Completions
  setDefault(apiVersion -> Left("2025-04-01-preview"))

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

  override def responseDataType: DataType = ChatModelResponse.schema

  private[openai] def getStringEntity(messages: Seq[Row], optionalParams: Map[String, Any]): StringEntity = {
    val mappedMessages = encodeMessagesToMap(messages)
      .map(_.filter { case (_, value) => value != null })
    val fullPayload = optionalParams.updated("messages", mappedMessages)
    new StringEntity(fullPayload.toJson.compactPrint, ContentType.APPLICATION_JSON)
  }

  override private[openai] def getOutputMessageText(outputColName: String): org.apache.spark.sql.Column = {
    F.element_at(F.col(outputColName).getField("choices"), 1)
      .getField("message").getField("content")
  }

  override private[openai] def isContentFiltered(outputRow: Row): Boolean = {
    val result = ChatModelResponse.makeFromRowConverter(outputRow)
    val firstChoice = result.choices.head
    Option(firstChoice.message.content).isEmpty
  }

  override private[openai] def getFilterReason(outputRow: Row): String = {
    val result = ChatModelResponse.makeFromRowConverter(outputRow)
    result.choices.head.finish_reason
  }

}
