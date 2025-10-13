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
    "Response format. Can be 'json_object', 'json_schema', or 'text'. For Responses API this is sent under 'text': {'format': {...}}.",
    isRequired = false) {
    override val payloadName: String = "text"
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
    val formatted: Map[String, Any] = tpe match {
      case "text" | "json_object" =>
        Map("format" -> Map("type" -> tpe))
      case "json_schema" =>
        // Build flattened 'format' object: { type: 'json_schema', name, schema, strict? }
        val hasNested = value.contains("json_schema")
        val hasFlattened = value.contains("name") && value.contains("schema")
        if (!hasNested && !hasFlattened) {
          throw new IllegalArgumentException(
            "When type == 'json_schema', provide nested key 'json_schema' or top-level 'name' and 'schema'.")
        }
        val inner: Map[String, Any] = if (hasFlattened) {
          Map(
            "type" -> "json_schema",
            "name" -> value("name"),
            "schema" -> value("schema")
          ) ++ (if (value.contains("strict")) Map("strict" -> value("strict")) else Map.empty)
        } else {
          val js = value("json_schema").asInstanceOf[Map[String, Any]]
          Map(
            "type" -> "json_schema",
            "name" -> js.getOrElse("name", throw new IllegalArgumentException("json_schema.name required")),
            "schema" -> js.getOrElse("schema", throw new IllegalArgumentException("json_schema.schema required"))
          ) ++ js.get("strict").map(v => Map("strict" -> v)).getOrElse(Map.empty)
        }
        Map("format" -> inner)
      case _ =>
        throw new IllegalArgumentException(
          "Response format must be valid for OpenAI API. Currently supported formats are text, json_object, json_schema.")
    }
    setScalarParam(responseFormat, formatted)
  }

  def setResponseFormat(value: String): this.type = {
    if (value == null || value.trim.isEmpty) {
      this
    } else {
      val trimmed = value.trim.toLowerCase
      if (trimmed == "json_schema") {
        throw new IllegalArgumentException(
          "To use json_schema pass a dict (Python) or Map (Scala) that complies with OpenAI responses text.format. " +
          "Example: setResponseFormat(Map(\"type\"->\"json_schema\", \"name\"->\"my_schema\", \"strict\"->true, \"schema\"->Map(...)))")
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

  // Default API version for Responses
  setDefault(apiVersion -> Left("2025-04-01-preview"))

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
    val base0 = super.getOptionalParams(r)
    // Remove flat reasoning_effort; Responses uses nested reasoning.effort
    val base = base0 - "reasoning_effort"
    // Merge model mapping
    val withModel = getValueOpt(r, deploymentName) match {
      case Some(modelName) if modelName != null && modelName.nonEmpty =>
        base.updated("model", modelName)
      case _ => base
    }
    // Merge verbosity under text alongside format for Responses API
    val maybeVerbosity = getValueOpt(r, verbosity)
    val withText = (withModel.get("text"), maybeVerbosity) match {
      case (Some(t: Map[_, _]), Some(v)) =>
        val textMap = t.asInstanceOf[Map[String, Any]]
        withModel.updated("text", textMap.updated("verbosity", v))
      case (None, Some(v)) =>
        withModel.updated("text", Map("verbosity" -> v))
      case _ => withModel
    }
    // Map reasoning_effort -> reasoning.effort for Responses API
    val withReasoning = getValueOpt(r, reasoningEffort) match {
      case Some(effort) =>
        val existing = withText.get("reasoning") match {
          case Some(m: Map[_, _]) => m.asInstanceOf[Map[String, Any]]
          case _ => Map.empty[String, Any]
        }
        withText.updated("reasoning", existing.updated("effort", effort))
      case _ => withText
    }
    // If a GPT-5 style model is used, include reasoning controls and drop classic sampling.
    val isGpt5 = withReasoning.get("model").exists {
      case s: String => s.toLowerCase.contains("gpt-5")
      case _ => false
    }
    if (isGpt5) {
      withReasoning - "temperature" - "top_p" - "seed"
    } else withReasoning
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
