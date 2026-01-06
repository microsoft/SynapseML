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
  case class ResponseFormat(paylodName: String) extends super.Val(paylodName)

  val TEXT: ResponseFormat = ResponseFormat("text")
  val JSON: ResponseFormat = ResponseFormat("json_object")

  def asStringSet: Set[String] = OpenAIResponseFormat.values.map(
    _.asInstanceOf[OpenAIResponseFormat.ResponseFormat].paylodName)

  def fromResponseFormatString(format: String): OpenAIResponseFormat.ResponseFormat = {
    if (TEXT.paylodName == format) TEXT
    else if (JSON.paylodName == format) JSON
    else throw new IllegalArgumentException("Response format must be one of: " + asStringSet.mkString(", "))
  }
}

trait HasOpenAITextParamsResponses extends HasOpenAITextParams {
  val responseFormat: ServiceParam[Map[String, Any]] = new ServiceParam[Map[String, Any]](
    this,
    "responseFormat",
    "Response format. One of 'text', 'json_object', 'json_schema'.",
    isRequired = false) {
    override val payloadName: String = "text"
  }

  def getResponseFormat: Map[String, Any] = getScalarParam(responseFormat)

  def setResponseFormat(value: Map[String, Any]): this.type = {
    val normalized = ResponseFormatUtils.normalize(value)
    val formatted = Map("format" -> normalized)
    setScalarParam(responseFormat, formatted)
  }

  // Validation helpers moved into ResponseFormatUtils

  def setResponseFormat(value: String): this.type = {
    if (value == null || value.trim.isEmpty) {
      this
    } else {
      val trimmed = value.trim.toLowerCase
      if (trimmed == "json_schema") {
        throw new IllegalArgumentException("Use a Map with required fields for 'json_schema'.")
      }
      setResponseFormat(Map("type" -> trimmed))
    }
  }

  def setResponseFormat(value: OpenAIResponseFormat.ResponseFormat): this.type = {
    setResponseFormat(Map("type" -> value.paylodName))
  }

  val store: ServiceParam[Boolean] = new ServiceParam[Boolean](
    this,
    "store",
    "Whether to store the generated response for use in model distillation or evals.",
    isRequired = false)

  def getStore: Boolean = getScalarParam(store)

  def setStore(v: Boolean): this.type = setScalarParam(store, v)

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
    store
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

  setDefault(
    apiVersion -> Left("2025-04-01-preview"),
    store -> Left(false)
  )

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
    val base = super.getOptionalParams(r) - "reasoning_effort"
    val withModel = mergeModel(base, r)
    val withText = mergeTextVerbosity(withModel, r)
    val withReasoning = mergeReasoning(withText, r)
    dropSamplingForGpt5(withReasoning)
  }

  private def mergeModel(params: Map[String, Any], r: Row): Map[String, Any] = {
    getValueOpt(r, deploymentName) match {
      case Some(m) if m != null && m.nonEmpty => params.updated("model", m)
      case _ => params
    }
  }

  private def mergeTextVerbosity(params: Map[String, Any], r: Row): Map[String, Any] = {
    getValueOpt(r, verbosity) match {
      case Some(v) =>
        params.get("text") match {
          case Some(t: Map[_, _]) =>
            params.updated("text", t.asInstanceOf[Map[String, Any]].updated("verbosity", v))
          case _ =>
            params.updated("text", Map("verbosity" -> v))
        }
      case _ => params
    }
  }

  private def mergeReasoning(params: Map[String, Any], r: Row): Map[String, Any] = {
    getValueOpt(r, reasoningEffort) match {
      case Some(effort) =>
        val existing = params.get("reasoning").collect {
          case m: Map[_, _] => m.asInstanceOf[Map[String, Any]]
        }.getOrElse(Map.empty)
        params.updated("reasoning", existing.updated("effort", effort))
      case _ => params
    }
  }

  private def dropSamplingForGpt5(params: Map[String, Any]): Map[String, Any] = {
    val isGpt5 = params.get("model").exists {
      case s: String => s.toLowerCase.contains("gpt-5")
      case _ => false
    }
    if (isGpt5) params - "temperature" - "top_p" - "seed" else params
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
      .map { m =>
        // For Responses API, ensure content is an array of parts with type 'input_text'
        m.get("content") match {
          case Some(s: String) =>
            m.updated("content", Seq(Map("type" -> "input_text", "text" -> s)))
          case _ => m
        }
      }
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
