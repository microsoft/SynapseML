// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.io.http.{HTTPOutputParser, JSONOutputParser}
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.AnyJsonFormat.anyFormat
import com.microsoft.azure.synapse.ml.param.ServiceParam
import com.microsoft.azure.synapse.ml.services.{HasCognitiveServiceInput, HasInternalJsonOutputParser}
import org.apache.http.entity.{AbstractHttpEntity, ContentType, StringEntity}
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset, functions => F, Row}
import org.apache.spark.sql.types._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.immutable.ListMap
import scala.language.existentials
import scala.util.control.NonFatal


trait HasOpenAITextParamsExtended extends HasOpenAITextParams {
  val responseFormat: ServiceParam[Map[String, Any]] = new ServiceParam[Map[String, Any]](
    this,
    "responseFormat",
    "Response format for the completion. One of 'text', 'json_object', or 'json_schema'.",
    isRequired = false) {
    override val payloadName: String = "response_format"
  }

  def getResponseFormat: Map[String, Any] = getScalarParam(responseFormat)

  def setResponseFormat(value: Map[String, Any]): this.type = {
    val normalized = ResponseFormatUtils.normalize(value)
    val payload =
      if (normalized.get("type").exists(_.toString.equalsIgnoreCase("json_schema"))) {
        Map(
          "type" -> "json_schema",
          "json_schema" -> {
            val base = Map(
              "name" -> normalized("name"),
              "schema" -> normalized("schema")
            )
            normalized.get("strict").map(v => base ++ Map("strict" -> v)).getOrElse(base)
          }
        )
      } else normalized
    setScalarParam(responseFormat, payload)
  }


  // Supported String values: "text", "json_object". Use Map for "json_schema" or inner JSON Schema.
  def setResponseFormat(value: String): this.type = {
    Option(value).map(_.trim).filter(_.nonEmpty) match {
      case None => this
      case Some(trimmed) =>
        if (trimmed.equalsIgnoreCase("json_schema")) {
          throw new IllegalArgumentException(
            "Use a Map for 'json_schema' or pass an inner JSON Schema map.")
        }
        trimmed.toLowerCase match {
          case "text" | "json_object" =>
            setResponseFormat(Map("type" -> trimmed.toLowerCase))
          case _ =>
            throw new IllegalArgumentException(
              "Unsupported response_format String. Use 'text', 'json_object', or pass a Map for schemas."
            )
        }
    }
  }

  def getResponseFormatType: String = Option(getResponseFormat)
    .flatMap(m => Option(m.getOrElse("type", "").toString))
    .getOrElse("")

  override private[openai] val sharedTextParams: Seq[ServiceParam[_]] = Seq(
    maxTokens,
    maxCompletionTokens,
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
  with HasOpenAITextParamsExtended with HasOpenAICommonToolParams with HasMessagesInput
  with HasCognitiveServiceInput with HasOpenAIFabricHeaders with HasInternalJsonOutputParser
  with SynapseMLLogging with HasRAIContentFilter with HasTextOutput with HasOpenAIToolCallOutput {
  logClass(FeatureNames.AiServices.OpenAI)

  def this() = this(Identifiable.randomUID("OpenAIChatCompletion"))

  private[openai] def generatedPythonClass: String = pythonClass()

  def urlPath: String = ""

  override private[ml] def internalServiceType: String = "openai"

  setDefault(apiVersion -> Left("2025-04-01-preview"))

  override def setCustomServiceName(v: String): this.type = {
    setUrl(s"https://$v.openai.azure.com/" + urlPath.stripPrefix("/"))
  }

  override protected def prepareUrlRoot: Row => String = { row =>
    if (isOpenAIV1BaseUrl) {
      endpointUrl("chat/completions")
    } else {
      endpointUrl(s"openai/deployments/${getValue(row, deploymentName)}/chat/completions")
    }
  }

  override private[ml] def getOptionalParams(r: Row): Map[String, Any] = {
    val base = super.getOptionalParams(r)
    mergeChatToolPayload(resolveMaxTokens(base, "max_completion_tokens"), r)
  }

  override protected[openai] def prepareEntity: Row => Option[AbstractHttpEntity] = {
    r =>
      lazy val optionalParams: Map[String, Any] = getOptionalParams(r)
      val messages = r.getAs[scala.collection.Seq[Row]](getMessagesCol).toSeq
      Some(getStringEntity(messages, withV1DeploymentModel(optionalParams, r)))
  }

  override val subscriptionKeyHeaderName: String = "api-key"

  override def shouldSkip(row: Row): Boolean =
    super.shouldSkip(row) ||
      Option(row.getAs[scala.collection.Seq[Row]](getMessagesCol)).forall(_.isEmpty)

  override protected def getVectorParamMap: Map[String, String] = super.getVectorParamMap
    .updated("messages", getMessagesCol)

  override def responseDataType: DataType = ChatModelResponse.schema

  override protected def getInternalOutputParser(schema: StructType): HTTPOutputParser =
    new JSONOutputParser().setDataType(ChatModelResponseV2.schema)

  override def toolCallsColumn(structColName: String): org.apache.spark.sql.Column =
    OpenAIToolColumns.chatToolCallsColumn(structColName)

  private def encodeMessageValue(value: Any): Any = value match {
    case row: Row => encodeMessageRow(row)
    case values: scala.collection.Seq[_] => values.map(encodeMessageValue).toSeq
    case values: Array[_] => values.map(encodeMessageValue).toSeq
    case values: scala.collection.Map[_, _] =>
      values.map { case (key, item) => key.toString -> encodeMessageValue(item) }.toMap
    case other => other
  }

  private def encodeMessageRow(row: Row): Map[String, Any] =
    ListMap(row.schema.fieldNames.zipWithIndex.flatMap { case (name, index) =>
      Option(row.get(index)).map(value => name -> encodeMessageValue(value))
    }: _*)

  private def encodeChatMessagesToMap(messages: Seq[Row]): Seq[Map[String, Any]] =
    messages.map(encodeMessageRow)

  private[openai] def getStringEntity(messages: Seq[Row], optionalParams: Map[String, Any]): StringEntity = {
    val mappedMessages = encodeChatMessagesToMap(messages)
      .map(_.filter { case (_, value) => value != null })
      .map { m =>
        // Chat Completions expects string content; collapse any content parts into a single text string
        m.get("content") match {
          case Some(parts: scala.collection.Seq[_]) =>
            val textChunks = parts.collect {
              case mp: Map[_, _] => mp.asInstanceOf[Map[String, Any]].get("text").map(_.toString)
            }.flatten
            val combined = textChunks.mkString("\n")
            m.updated("content", combined)
          case _ => m
        }
      }
    val fullPayload = optionalParams.updated("messages", mappedMessages)
    new StringEntity(fullPayload.toJson.compactPrint, ContentType.APPLICATION_JSON)
  }

  override private[openai] def getOutputMessageText(outputColName: String): org.apache.spark.sql.Column = {
    F.element_at(F.col(outputColName).getField("choices"), 1)
      .getField("message").getField("content")
  }

  override private[openai] def isContentFiltered(outputRow: Row): Boolean = {
    getFilterReason(outputRow) == OpenAIToolUtils.ContentFilterReason
  }

  override private[openai] def getFilterReason(outputRow: Row): String = {
    outputRow.getAs[scala.collection.Seq[Row]]("choices").head
      .getAs[String]("finish_reason")
  }

  private def transformPrepared(dataset: Dataset[_]): DataFrame =
    super.transform(dataset)

  private def vectorStringColumn(param: ServiceParam[String]): Option[String] =
    get(param).orElse(getDefault(param)).flatMap(_.right.toOption)

  private def hasToolRowValidation: Boolean =
    vectorStringColumn(tools).isDefined || vectorStringColumn(toolChoice).isDefined

  private def toolRowValidationErrorColumn: org.apache.spark.sql.Column = {
    val scalarTools = get(tools).flatMap(_.left.toOption).map(OpenAIToolUtils.parseTools)
    val scalarChoice = get(toolChoice).flatMap(_.left.toOption)
      .flatMap(OpenAIToolUtils.parseToolChoice)
    val toolsJson = vectorStringColumn(tools).map(F.col)
      .getOrElse(F.lit(null).cast(StringType)) //scalastyle:ignore null
    val choiceJson = vectorStringColumn(toolChoice).map(F.col)
      .getOrElse(F.lit(null).cast(StringType)) //scalastyle:ignore null
    val validate = F.udf((toolsValue: String, choiceValue: String) => {
      try {
        val effectiveTools = Option(toolsValue).map(_.trim).filter(_.nonEmpty)
          .map(OpenAIToolUtils.parseTools)
          .orElse(scalarTools)
        val effectiveChoice = OpenAIToolUtils.parseToolChoice(choiceValue)
          .orElse(scalarChoice)
        validateResolvedToolSetup(
          effectiveTools,
          effectiveChoice,
          effectiveTools.isDefined)
        null //scalastyle:ignore null
      } catch {
        case NonFatal(error) =>
          Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
      }
    })
    validate(toolsJson, choiceJson)
  }

  private def transformWithToolRowValidation(dataset: Dataset[_]): DataFrame = {
    if (!hasToolRowValidation) {
      transformPrepared(dataset)
    } else {
      val errorInputCol = DatasetExtensions.findUnusedColumnName(
        "openaiChatToolValidationError",
        dataset.schema)
      val messagesShadow = DatasetExtensions.findUnusedColumnName(
        s"${getMessagesCol}_validated",
        dataset.schema)
      val prepared = dataset.toDF.withColumn(errorInputCol, toolRowValidationErrorColumn)
      val messagesType = prepared.schema(getMessagesCol).dataType
      val shadowed = prepared.withColumn(
        messagesShadow,
        F.when(F.col(errorInputCol).isNull, F.col(getMessagesCol))
          .otherwise(F.lit(null).cast(messagesType))) //scalastyle:ignore null
      val worker = copy(ParamMap.empty).asInstanceOf[OpenAIChatCompletion]
        .setMessagesCol(messagesShadow)
      val transformed = worker.transformPrepared(shadowed)
      val errorType = transformed.schema(getErrorCol).dataType.asInstanceOf[StructType]
      val validationError = F.when(
        F.col(errorInputCol).isNotNull,
        F.struct(
          F.col(errorInputCol).as("response"),
          F.lit(null).cast(errorType("status").dataType).as("status") //scalastyle:ignore null
        ))
      transformed
        .withColumn(getErrorCol, F.coalesce(F.col(getErrorCol), validationError))
        .drop(errorInputCol, messagesShadow)
    }
  }

  private def validateChatToolSetup(schema: StructType): Unit = {
    get(toolCallsCol).foreach { columnName =>
      require(
        !schema.fieldNames.contains(columnName),
        s"Column '$columnName' already exists in the input DataFrame")
    }
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    validateToolSetup()
    validateChatToolSetup(dataset.schema)
    val result = transformWithToolRowValidation(dataset)
    if (isSet(toolCallsCol)) {
      result.withColumn(getToolCallsCol, toolCallsColumn(getOutputCol))
    } else {
      result
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    validateToolSetup()
    validateChatToolSetup(schema)
    val baseSchema = super.transformSchema(schema)
    if (isSet(toolCallsCol)) {
      baseSchema.add(getToolCallsCol, OpenAIToolColumns.ToolCallStructType)
    } else {
      baseSchema
    }
  }

  override def pyAdditionalMethods: String =
    super.pyAdditionalMethods + OpenAIToolPythonOverrides.ChatMethods
}
