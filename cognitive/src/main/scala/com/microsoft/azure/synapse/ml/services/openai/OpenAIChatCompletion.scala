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

/**
 * Minimal response_format handling:
 * - Accept any non-empty 'type' (delegated to service for validation).
 * - Special case: bare 'json_schema' string is rejected (must include full json_schema object).
 * - If type == json_schema then a 'json_schema' key must be present.
 */

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
    if (value == null || !value.contains("type") ||
      value("type") == null ||
      value("type").asInstanceOf[Any].toString.trim.isEmpty) {
      throw new IllegalArgumentException("response_format map must contain non-empty key 'type'")
    }
    val tpe = value("type").toString.toLowerCase
    if (tpe == "json_schema" && !value.contains("json_schema")) {
      throw new IllegalArgumentException("When type == 'json_schema', key 'json_schema' must be provided.")
    }
    setScalarParam(responseFormat, value)
  }

  // Simplified: only allow plain type tokens via String for common simple formats.
  // Supported String values: "text", "json_object".
  // For "json_schema" caller must use Map form with full structure (no parsing performed here).
  // Any other String value is rejected to avoid implicit parsing/assumptions.
  def setResponseFormat(value: String): this.type = {
    Option(value).map(_.trim).filter(_.nonEmpty) match {
      case None => this
      case Some(trimmed) =>
        if (trimmed.equalsIgnoreCase("json_schema")) {
          throw new IllegalArgumentException(
            "Provide json_schema via Map: setResponseFormat(Map(\"type\"->\"json_schema\",\"json_schema\"-> {...}))")
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
    responseFormat,
    seed
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

  private[openai] def getStringEntity(messages: Seq[Row], optionalParams: Map[String, Any]): StringEntity = {
    val mappedMessages: Seq[Map[String, String]] = messages.map { m =>
      Seq("role", "content", "name").map(n =>
        n -> Option(m.getAs[String](n))
      ).toMap.filter(_._2.isDefined).mapValues(_.get)
    }
    val fullPayload = optionalParams.updated("messages", mappedMessages)
    new StringEntity(fullPayload.toJson.compactPrint, ContentType.APPLICATION_JSON)
  }

}
