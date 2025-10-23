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
    if (value == null) {
      throw new IllegalArgumentException("Response format must be a non-null Map.")
    }
    def looksLikeInnerSchema(m: Map[String, Any]): Boolean = {
      val keys = m.keySet.map(_.toString)
      val schemaHints = Set("properties", "required", "additionalProperties", "$schema", "items")
      keys.exists(schemaHints.contains)
    }

    val typeOpt = value.get("type").map(_.toString.trim.toLowerCase)
    val isKnownRfType = typeOpt.exists(t => t == "text" || t == "json_object" || t == "json_schema")
    val isJsonSchemaType = typeOpt.exists(t => Set(
      "object", "array", "string", "number", "integer", "boolean", "null").contains(t))
    val treatAsInnerSchema = (!isKnownRfType && (looksLikeInnerSchema(value) || isJsonSchemaType))

    val formatObj: Map[String, Any] = if (isKnownRfType) {
      buildFormatObject(value)
    } else if (treatAsInnerSchema) {
      // Inner JSON Schema detected; flatten for Responses API
      val name = value.get("name").map(_.toString).getOrElse("response_schema")
      val strictOpt = value.get("strict")
      val innerSchema = value - "name" - "strict"
      val flat = Map(
        "type" -> "json_schema",
        "name" -> name,
        "schema" -> innerSchema
      ) ++ strictOpt.map(v => Map("strict" -> v)).getOrElse(Map.empty)
      flat
    } else if (typeOpt.isEmpty) {
      // Treat as bare schema Map with required keys (name + schema); flatten for Responses API
      requireBareJsonSchemaShape(value)
      val flat = Map("type" -> "json_schema") ++ value
      buildJsonSchemaFormat(flat)
    } else {
      // Has a 'type' but not recognized as response_format or JSON Schema -> invalid
      val bad = typeOpt.get
      throw new IllegalArgumentException(
        s"Unsupported response format type: '$bad'. Allowed: 'text','json_object','json_schema'. " +
          "For inner JSON Schema, either omit top-level 'type' or use a schema with keys like 'properties' " +
          "or a JSON Schema type ('object','array',...)."
      )
    }
    val formatted = Map("format" -> formatObj)
    setScalarParam(responseFormat, formatted)
  }

  private def buildFormatObject(value: Map[String, Any]): Map[String, Any] = {
    val tpe = value("type").toString.toLowerCase
    tpe match {
      case "text" | "json_object" => Map("type" -> tpe)
      case "json_schema" => buildJsonSchemaFormat(value)
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported response format type: '$tpe'. Allowed: 'text','json_object','json_schema'. " +
            "If you're passing an inner JSON Schema (e.g., type:'object', properties:...), " +
            "omit the top-level 'type' or set type:'json_schema' and include either nested " +
            "'json_schema':{name,schema,strict?} or top-level name/schema."
        )
    }
  }

  private def buildJsonSchemaFormat(value: Map[String, Any]): Map[String, Any] = {
    val hasNested = value.contains("json_schema")
    val hasFlat = value.contains("name") && value.contains("schema")
    if (!hasNested && !hasFlat) {
      throw new IllegalArgumentException("json_schema requires nested 'json_schema' or top-level 'name' and 'schema'.")
    }
    if (hasFlat) {
      Map(
        "type" -> "json_schema",
        "name" -> value("name"),
        "schema" -> value("schema")
      ) ++ (if (value.contains("strict")) Map("strict" -> value("strict")) else Map.empty)
    } else {
      val js = value("json_schema").asInstanceOf[Map[String, Any]]
      val base = Map(
        "type" -> "json_schema",
        "name" -> js.getOrElse("name", throw new IllegalArgumentException("json_schema.name required")),
        "schema" -> js.getOrElse("schema", throw new IllegalArgumentException("json_schema.schema required"))
      )
      js.get("strict").map(v => base ++ Map("strict" -> v)).getOrElse(base)
    }
  }

  private def requireBareJsonSchemaShape(value: Map[String, Any]): Unit = {
    val hasName = value.contains("name")
    val hasSchema = value.contains("schema")
    if (!hasName || !hasSchema) {
      throw new IllegalArgumentException("Bare schema Map must include 'name' and 'schema' keys.")
    }
  }

  def setResponseFormat(value: String): this.type = {
    if (value == null || value.trim.isEmpty) {
      this
    } else {
      val trimmed = value.trim.toLowerCase
      if (trimmed == "json_schema") {
        val msg = "To use json_schema pass a dict (Python) or Map (Scala) with required fields. " +
          "Responses: Map('type'->'json_schema','name'->...,'schema'-> {...}, 'strict'->true?)"
        throw new IllegalArgumentException(msg)
      }
      setResponseFormat(Map("type" -> trimmed))
    }
  }

  def setResponseFormat(value: OpenAIResponseFormat.ResponseFormat): this.type = {
    setScalarParam(responseFormat, Map("type" -> value.paylodName))
  }

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
