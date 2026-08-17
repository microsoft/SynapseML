// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions.{findUnusedColumnName => newCol}
import com.microsoft.azure.synapse.ml.io.http.ErrorUtils
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.AnyJsonFormat.anyFormat
import com.microsoft.azure.synapse.ml.param.ServiceParam
import com.microsoft.azure.synapse.ml.services.{HasCognitiveServiceInput, HasInternalJsonOutputParser}
import org.apache.http.entity.{AbstractHttpEntity, ContentType, StringEntity}
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset, Row, functions => F}
import org.apache.spark.sql.types._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.JavaConverters._
import scala.collection.{Seq => CollectionSeq}
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
    "Whether to store the generated model response for later retrieval via API.",
    isRequired = false)

  def getStore: Boolean = getScalarParam(store)

  def setStore(v: Boolean): this.type = setScalarParam(store, v)

  val previousResponseId: ServiceParam[String] = new ServiceParam[String](
    this,
    "previousResponseId",
    "The ID of a previous response to use as context for chaining requests. " +
      "Use this for multi-turn conversations or follow-up requests.",
    isRequired = false) {
    override val payloadName: String = "previous_response_id"
  }

  def getPreviousResponseId: String = getScalarParam(previousResponseId)

  def setPreviousResponseId(v: String): this.type = setScalarParam(previousResponseId, v)

  def getPreviousResponseIdCol: String = getVectorParam(previousResponseId)

  def setPreviousResponseIdCol(v: String): this.type = setVectorParam(previousResponseId, v)

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
    responseFormat,
    store,
    previousResponseId
  )
}

object OpenAIResponses extends ComplexParamsReadable[OpenAIResponses]

class OpenAIResponses(override val uid: String) extends OpenAIServicesBase(uid)
  with HasOpenAITextParamsResponses with HasMessagesInput with HasCognitiveServiceInput
  with HasOpenAIFabricHeaders with HasInternalJsonOutputParser with SynapseMLLogging with HasCustomHeaders
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
    if (isOpenAIV1BaseUrl) {
      endpointUrl("responses")
    } else {
      endpointUrl("openai/responses")
    }
  }

  override protected[openai] def prepareEntity: Row => Option[AbstractHttpEntity] = {
    r =>
      lazy val optionalParams: Map[String, Any] = getOptionalParams(r)
      val messages = r.getAs[Seq[Row]](getMessagesCol)
      Some(getStringEntity(messages, optionalParams))
  }

  override private[ml] def getOptionalParams(r: Row): Map[String, Any] = {
    val base = super.getOptionalParams(r) - "reasoning_effort"
    val withTokens = resolveMaxTokens(base, "max_output_tokens")
    val withModel = mergeModel(withTokens, r)
    val withText = mergeTextVerbosity(withModel, r)
    val withReasoning = mergeReasoning(withText, r)
    dropSamplingForGpt5(withReasoning)
  }

  private def mergeModel(params: Map[String, Any], r: Row): Map[String, Any] = {
    getValueOpt(r, deploymentName) match {
      case Some(m) if m != null && m.nonEmpty => params.updated("model", m)
      case _ if isOpenAIV1BaseUrl && !params.contains("model") =>
        throw new IllegalArgumentException(
          "No deployment/model name provided for OpenAI v1 endpoint. Set the 'deploymentName' param.")
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

  private def runtimeType(value: Any): String = {
    Option(value).map(_.getClass.getSimpleName).getOrElse("null")
  }

  private def encodeStruct(value: Any, structType: StructType): Map[String, Any] = {
    value match {
      case row: Row =>
        if (row.length != structType.length) {
          throw new IllegalArgumentException("Struct content part does not match its declared schema")
        }
        structType.fields.zipWithIndex.flatMap { case (field, index) =>
          if (row.isNullAt(index)) {
            None
          } else {
            Some(field.name -> encodeStructuredValue(row.get(index), field.dataType))
          }
        }.toMap
      case other =>
        throw new IllegalArgumentException(
          s"Expected struct content part but found ${runtimeType(other)}")
    }
  }

  private def encodeArray(value: Any, elementType: DataType): Seq[Any] = {
    value match {
      case values: CollectionSeq[_] => values.map(encodeStructuredValue(_, elementType)).toSeq
      case values: Array[_] => values.toSeq.map(encodeStructuredValue(_, elementType))
      case other =>
        throw new IllegalArgumentException(
          s"Expected array content but found ${runtimeType(other)}")
    }
  }

  private def encodeMap(value: Any, valueType: DataType): Map[String, Any] = {
    value match {
      case values: scala.collection.Map[_, _] =>
        values.iterator.map {
          case (key: String, entryValue) => key -> encodeStructuredValue(entryValue, valueType)
          case _ => throw new IllegalArgumentException("Content part map keys must be strings")
        }.filter { case (_, entryValue) => entryValue != null }.toMap
      case other =>
        throw new IllegalArgumentException(
          s"Expected map content part but found ${runtimeType(other)}")
    }
  }

  private def encodeStructuredValue(value: Any, dataType: DataType): Any = {
    if (value == null) {
      null // scalastyle:ignore null
    } else {
      dataType match {
        case structType: StructType => encodeStruct(value, structType)
        case ArrayType(elementType, _) => encodeArray(value, elementType)
        case MapType(StringType, valueType, _) => encodeMap(value, valueType)
        case _: MapType =>
          throw new IllegalArgumentException("Content part map keys must have string type")
        case _ => value
      }
    }
  }

  private def invalidRoleError(messageIndex: Int): IllegalArgumentException = {
    new IllegalArgumentException(s"messages[$messageIndex].role must be a non-empty string")
  }

  private def validatedRole(message: Row, messageIndex: Int): String = {
    val roleIndex = message.schema.fieldNames.indexOf("role")
    if (roleIndex < 0 || roleIndex >= message.length ||
        message.schema.fields(roleIndex).dataType != StringType || message.isNullAt(roleIndex)) {
      throw invalidRoleError(messageIndex)
    }
    message.get(roleIndex) match {
      case role: String if role.trim.nonEmpty => role
      case _ => throw invalidRoleError(messageIndex)
    }
  }

  private def encodedMessageContent(message: Row, messageIndex: Int): Any = {
    val contentIndex = message.schema.fieldNames.indexOf("content")
    if (contentIndex < 0 || contentIndex >= message.length) {
      throw new IllegalArgumentException(
        s"messages[$messageIndex].content must be a string or an array of content part objects")
    }

    message.schema.fields(contentIndex).dataType match {
      case StringType =>
        if (message.isNullAt(contentIndex)) {
          null // scalastyle:ignore null
        } else {
          message.get(contentIndex) match {
            case text: String => text
            case _ =>
              throw new IllegalArgumentException(s"messages[$messageIndex].content must be a string")
          }
        }
      case arrayType: ArrayType => encodeStructuredValue(message.get(contentIndex), arrayType)
      case other =>
        throw new IllegalArgumentException(
          s"messages[$messageIndex].content has unsupported type ${other.typeName}")
    }
  }

  private def encodeResponsesMessages(messages: Seq[Row]): Seq[Map[String, Any]] = {
    messages.zipWithIndex.map { case (row, messageIndex) =>
      if (row == null) {
        throw new IllegalArgumentException(s"messages[$messageIndex] must be an object")
      }
      Map(
        "role" -> validatedRole(row, messageIndex),
        "content" -> encodedMessageContent(row, messageIndex)
      )
    }
  }

  private def validatePublicColumnNames(): Unit = {
    require(
      getMessagesCol != getOutputCol,
      s"messagesCol '${getMessagesCol}' must be different from outputCol '${getOutputCol}'"
    )
    require(
      getMessagesCol != getErrorCol,
      s"messagesCol '${getMessagesCol}' must be different from errorCol '${getErrorCol}'"
    )
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    transferGlobalParamsToParamMap()
    validatePublicColumnNames()
    logTransform[DataFrame]({
      val df = dataset.toDF()
      val colsToAvoid = df.schema.fieldNames.toSet ++ Set(getErrorCol, getOutputCol)
      val originalMessagesCol = newCol("originalMessages")(colsToAvoid)
      val validationErrorCol = newCol("responsesMessageValidationError")(colsToAvoid + originalMessagesCol)
      val messagesDataType = df.schema(getMessagesCol).dataType

      val validationErrorUDF = UDFUtils.oldUdf(
        (messages: CollectionSeq[Row]) => structuredMessageValidationError(messages).map(message =>
          Row(message, null) //scalastyle:ignore null
        ).orNull,
        ErrorUtils.ErrorSchema
      )

      val validatedMessages = df
        .withColumn(originalMessagesCol, F.col(getMessagesCol))
        .withColumn(validationErrorCol, validationErrorUDF(F.col(originalMessagesCol)))
        .withColumn(
          getMessagesCol,
          F.when(F.col(validationErrorCol).isNotNull, F.lit(null).cast(messagesDataType)) //scalastyle:ignore null
            .otherwise(F.col(originalMessagesCol))
        )

      val validatedWithErrors = if (df.columns.contains(getErrorCol)) {
        validatedMessages.withColumn(
          getErrorCol,
          F.coalesce(F.col(getErrorCol), F.col(validationErrorCol))
        )
      } else {
        validatedMessages.withColumn(getErrorCol, F.col(validationErrorCol))
      }

      getInternalTransformer(validatedWithErrors.schema).transform(validatedWithErrors)
        .drop(validationErrorCol)
        .withColumn(getMessagesCol, F.col(originalMessagesCol))
        .drop(originalMessagesCol)
    }, dataset.columns.length)
  }

  override def transformSchema(schema: StructType): StructType = {
    validatePublicColumnNames()
    super.transformSchema(schema)
  }

  private def requireOnlyFields(part: Map[String, Any], allowed: Set[String], location: String): Unit = {
    if ((part.keySet -- allowed).nonEmpty) {
      throw new IllegalArgumentException(s"$location contains unsupported fields")
    }
  }

  private def requireNonEmptyString(value: Any, errorMessage: String): Unit = {
    value match {
      case text: String if text.trim.nonEmpty =>
      case _ => throw new IllegalArgumentException(errorMessage)
    }
  }

  private def validateInputTextPart(part: Map[String, Any], location: String): Unit = {
    requireOnlyFields(part, Set("type", "text"), location)
    part.get("text") match {
      case Some(_: String) =>
      case _ => throw new IllegalArgumentException(s"$location requires a string 'text' field")
    }
  }

  private def validateInputImagePart(part: Map[String, Any], location: String): Unit = {
    requireOnlyFields(part, Set("type", "image_url", "file_id", "detail"), location)
    val imageUrlDefined = part.get("image_url").exists { value =>
      requireNonEmptyString(value, s"$location requires a non-empty string 'image_url' or 'file_id' field")
      true
    }
    val fileIdDefined = part.get("file_id").exists { value =>
      requireNonEmptyString(value, s"$location requires a non-empty string 'image_url' or 'file_id' field")
      true
    }
    if (!imageUrlDefined && !fileIdDefined) {
      throw new IllegalArgumentException(
        s"$location requires a non-empty string 'image_url' or 'file_id' field")
    }
    part.get("detail").foreach(value =>
      requireNonEmptyString(value, s"$location 'detail' must be a non-empty string when provided"))
  }

  private def validateInputFilePart(part: Map[String, Any], location: String): Unit = {
    requireOnlyFields(part, Set("type", "file_data", "file_id", "filename"), location)
    val fileDataDefined = part.get("file_data").exists { value =>
      requireNonEmptyString(value, s"$location requires a non-empty string 'file_data' or 'file_id' field")
      true
    }
    val fileIdDefined = part.get("file_id").exists { value =>
      requireNonEmptyString(value, s"$location requires a non-empty string 'file_data' or 'file_id' field")
      true
    }
    if (!fileDataDefined && !fileIdDefined) {
      throw new IllegalArgumentException(
        s"$location requires a non-empty string 'file_data' or 'file_id' field")
    }
    part.get("filename").foreach(value =>
      requireNonEmptyString(value, s"$location 'filename' must be a non-empty string when provided"))
  }

  private def validateContentPart(part: Any, messageIndex: Int, partIndex: Int): Unit = {
    val location = s"messages[$messageIndex].content[$partIndex]"
    val fields = part match {
      case values: scala.collection.Map[_, _] =>
        values.asInstanceOf[scala.collection.Map[String, Any]].toMap
      case _ => throw new IllegalArgumentException(s"$location must be an object")
    }

    fields.get("type") match {
      case Some("input_text") => validateInputTextPart(fields, location)
      case Some("input_image") => validateInputImagePart(fields, location)
      case Some("input_file") => validateInputFilePart(fields, location)
      case Some(value: String) if value.trim.nonEmpty =>
      case _ => throw new IllegalArgumentException(s"$location requires a non-empty string 'type' field")
    }
  }

  private def contentItems(value: Any, messageIndex: Int): CollectionSeq[Any] = {
    val items = value match {
      case values: CollectionSeq[_] => values
      case values: Array[_] => values.toSeq
      case _ =>
        throw new IllegalArgumentException(
          s"messages[$messageIndex].content must be an array of content part objects")
    }
    if (items.isEmpty) {
      throw new IllegalArgumentException(s"messages[$messageIndex].content must not be empty")
    }
    items
  }

  private def validateEncodedMessage(message: Map[String, Any], messageIndex: Int): Unit = {
    message.get("role") match {
      case Some(role: String) if role.trim.nonEmpty =>
      case _ =>
        throw new IllegalArgumentException(s"messages[$messageIndex].role must be a non-empty string")
    }

    message.get("content") match {
      case Some(_: String) =>
      case Some(content) =>
        contentItems(content, messageIndex).zipWithIndex.foreach {
          case (part, partIndex) => validateContentPart(part, messageIndex, partIndex)
        }
      case _ =>
        throw new IllegalArgumentException(
          s"messages[$messageIndex].content must be a string or an array of content part objects")
    }
  }

  private def validateMessages(messages: CollectionSeq[Row]): Unit = {
    if (messages.isEmpty) {
      throw new IllegalArgumentException("messages must not be empty")
    }
    messages.zipWithIndex.foreach { case (message, messageIndex) =>
      if (message == null) {
        throw new IllegalArgumentException(s"messages[$messageIndex] must be an object")
      }
      val encoded = encodeResponsesMessages(Seq(message)).head
      validateEncodedMessage(encoded, messageIndex)
    }
  }

  private def structuredMessageValidationError(messages: CollectionSeq[Row]): Option[String] = {
    Option(messages).flatMap { messageRows =>
      try {
        validateMessages(messageRows)
        None
      } catch {
        case e: IllegalArgumentException => Some(e.getMessage)
      }
    }
  }

  private[openai] def getStringEntity(messages: Seq[Row], optionalParams: Map[String, Any]): StringEntity = {
    val mappedMessages = encodeResponsesMessages(messages)
      .zipWithIndex
      .map { case (message, messageIndex) =>
        validateEncodedMessage(message, messageIndex)
        message
      }
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
    val outputEntries = F.col(outputColName).getField("output")
    val lastOutputEntry = F.element_at(outputEntries, -1)
    val textValues = F.transform(lastOutputEntry.getField("content"), part => part.getField("text"))
    val definedTextValues = F.filter(textValues, text => text.isNotNull)
    F.element_at(definedTextValues, 1)
  }

  private def outputEntries(outputRow: Row): Seq[Row] = {
    Option(outputRow).flatMap(r => Option(r.getAs[Seq[Row]]("output"))).getOrElse(Seq.empty)
  }

  private def lastOutputEntry(outputRow: Row): Option[Row] = {
    outputEntries(outputRow).lastOption
  }

  private def firstDefinedText(contentParts: Seq[Row]): Option[String] = {
    contentParts.iterator
      .flatMap(part => Option(part.getAs[String]("text")))
      .toSeq
      .headOption
  }

  private def lastOutputText(outputRow: Row): Option[String] = {
    lastOutputEntry(outputRow).flatMap { outputEntry =>
      firstDefinedText(Option(outputEntry.getAs[Seq[Row]]("content")).getOrElse(Seq.empty))
    }
  }

  override private[openai] def isContentFiltered(outputRow: Row): Boolean = {
    lastOutputText(outputRow).isEmpty
  }

  override private[openai] def getFilterReason(outputRow: Row): String = {
    lastOutputEntry(outputRow).iterator
      .flatMap(outputEntry => Option(outputEntry.getAs[String]("status")))
      .find(_.nonEmpty)
      .getOrElse("content_filtered_or_empty")
  }

}
