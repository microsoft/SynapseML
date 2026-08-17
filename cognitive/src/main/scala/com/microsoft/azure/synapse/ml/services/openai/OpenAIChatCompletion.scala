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

import scala.collection.{Seq => CollectionSeq}
import scala.language.existentials


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
  with HasOpenAITextParamsExtended with HasMessagesInput with HasCognitiveServiceInput
  with HasOpenAIFabricHeaders with HasInternalJsonOutputParser
  with SynapseMLLogging with HasRAIContentFilter with HasTextOutput {
  logClass(FeatureNames.AiServices.OpenAI)

  def this() = this(Identifiable.randomUID("OpenAIChatCompletion"))

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
    resolveMaxTokens(base, "max_completion_tokens")
  }

  override protected[openai] def prepareEntity: Row => Option[AbstractHttpEntity] = {
    r =>
      lazy val optionalParams: Map[String, Any] = getOptionalParams(r)
      val messages = r.getAs[Seq[Row]](getMessagesCol)
      Some(getStringEntity(messages, withV1DeploymentModel(optionalParams, r)))
  }

  override val subscriptionKeyHeaderName: String = "api-key"

  override def shouldSkip(row: Row): Boolean =
    super.shouldSkip(row) || row.get(row.fieldIndex(getMessagesCol)) == null

  override protected def getVectorParamMap: Map[String, String] = super.getVectorParamMap
    .updated("messages", getMessagesCol)

  override def responseDataType: DataType = ChatModelResponse.schema

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
      val validationErrorCol = newCol("structuredMessageValidationError")(colsToAvoid + originalMessagesCol)
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
        // Null only structurally invalid rows so the inherited shouldSkip bypasses request construction/HTTP.
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

  private def runtimeType(value: Any): String = {
    Option(value).map(_.getClass.getSimpleName).getOrElse("null")
  }

  private def encodeStruct(nestedRow: Row, structType: StructType): Map[String, Any] = {
    if (nestedRow.length != structType.length) {
      throw new IllegalArgumentException("Struct content part does not match its declared schema")
    }
    structType.fields.zipWithIndex.flatMap { case (field, index) =>
      if (nestedRow.isNullAt(index)) {
        None
      } else {
        Some(field.name -> encodeStructuredValue(nestedRow.get(index), field.dataType))
      }
    }.toMap
  }

  private def encodeStructuredArray(value: Any, elementType: DataType): Seq[Any] = {
    value match {
      case values: CollectionSeq[_] => values.map(item => encodeStructuredValue(item, elementType)).toSeq
      case values: Array[_] => values.toSeq.map(item => encodeStructuredValue(item, elementType))
      case other =>
        throw new IllegalArgumentException(
          s"Expected array content but found ${runtimeType(other)}")
    }
  }

  private def encodeStructuredMap(value: Any, valueType: DataType): Map[String, Any] = {
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
    dataType match {
      case structType: StructType =>
        value match {
          case nestedRow: Row => encodeStruct(nestedRow, structType)
          case other =>
            throw new IllegalArgumentException(
              s"Expected struct content part but found ${runtimeType(other)}")
        }
      case ArrayType(elementType, _) => encodeStructuredArray(value, elementType)
      case MapType(StringType, valueType, _) => encodeStructuredMap(value, valueType)
      case _: MapType =>
        throw new IllegalArgumentException("Content part map keys must have string type")
      case _ => value
    }
  }

  private def collapseContentPartsToText(value: Any): String = {
    val parts = value match {
      case values: CollectionSeq[_] => values
      case values: Array[_] => values.toSeq
      case other =>
        throw new IllegalArgumentException(
          s"Expected array content but found ${runtimeType(other)}")
    }

    parts.collect {
      case rawPart: scala.collection.Map[_, _] =>
        rawPart.asInstanceOf[scala.collection.Map[String, Any]].get("text").flatMap(Option(_)).map(_.toString)
    }.flatten.mkString("\n")
  }

  private def mapBackedContentParts(value: Any, messageIndex: Int): CollectionSeq[Map[String, Any]] = {
    val parts = value match {
      case values: CollectionSeq[_] => values
      case values: Array[_] => values.toSeq
      case _ =>
        throw new IllegalArgumentException(
          s"messages[$messageIndex].content must be an array of content part objects")
    }
    parts.zipWithIndex.map {
      case (rawPart: scala.collection.Map[_, _], _) =>
        rawPart.iterator.map {
          case (key: String, entryValue) => key -> entryValue
          case _ =>
            throw new IllegalArgumentException(
              s"messages[$messageIndex].content map keys must be strings")
        }.filter { case (_, entryValue) => entryValue != null }.toMap
      case (_, partIndex) =>
        throw new IllegalArgumentException(s"messages[$messageIndex].content[$partIndex] must be an object")
    }
  }

  private def validateMapBackedTextParts(
      parts: CollectionSeq[Map[String, Any]],
      messageIndex: Int
  ): Unit = {
    parts.zipWithIndex.foreach { case (part, partIndex) =>
      val location = s"messages[$messageIndex].content[$partIndex]"
      requireOnlyFields(part, Set("type", "text"), location)
      part.get("type") match {
        case Some("text") =>
        case Some(_: String) =>
          throw new IllegalArgumentException(
            s"$location has an unsupported type; supported types are 'text' and 'image_url'")
        case _ => throw new IllegalArgumentException(s"$location requires a string 'type' field")
      }
    }
  }

  private def encodedMapBackedContent(value: Any, messageIndex: Int): Any = {
    val parts = mapBackedContentParts(value, messageIndex)
    if (!parts.exists(_.get("type").contains("image_url"))) {
      validateMapBackedTextParts(parts, messageIndex)
      collapseContentPartsToText(value)
    } else {
      parts.zipWithIndex.map { case (part, partIndex) =>
        val location = s"messages[$messageIndex].content[$partIndex]"
        val encoded = part.get("type") match {
          case Some("image_url") =>
            requireOnlyFields(part, Set("type", "image_url", "detail"), location)
            val imageUrl = part.get("image_url") match {
              case Some(url: String) if url.trim.nonEmpty =>
                Map("url" -> url) ++ part.get("detail").map("detail" -> _)
              case _ =>
                throw new IllegalArgumentException(
                  s"messages[$messageIndex].content[$partIndex].image_url " +
                    "requires a non-empty string 'url' field")
            }
            Map("type" -> "image_url", "image_url" -> imageUrl)
          case _ => part
        }
        validateContentPart(encoded, messageIndex, partIndex)
        encoded
      }
    }
  }

  private def requireOnlyFields(part: Map[String, Any], allowed: Set[String], location: String): Unit = {
    if ((part.keySet -- allowed).nonEmpty) {
      throw new IllegalArgumentException(s"$location contains unsupported fields")
    }
  }

  private def validateTextPart(part: Map[String, Any], location: String): Unit = {
    requireOnlyFields(part, Set("type", "text"), location)
    part.get("text") match {
      case Some(_: String) =>
      case _ => throw new IllegalArgumentException(s"$location requires a string 'text' field")
    }
  }

  private def validateImageUrlPart(part: Map[String, Any], location: String): Unit = {
    requireOnlyFields(part, Set("type", "image_url"), location)
    part.get("image_url") match {
      case Some(rawImageUrl: Map[_, _]) =>
        val imageUrl = rawImageUrl.asInstanceOf[Map[String, Any]]
        requireOnlyFields(imageUrl, Set("url", "detail"), s"$location.image_url")
        imageUrl.get("url") match {
          case Some(url: String) if url.trim.nonEmpty =>
          case _ =>
            throw new IllegalArgumentException(
              s"$location.image_url requires a non-empty string 'url' field")
        }
        imageUrl.get("detail").foreach {
          case detail: String if detail.trim.nonEmpty =>
          case _ =>
            throw new IllegalArgumentException(
              s"$location.image_url 'detail' must be a non-empty string when provided")
        }
      case _ =>
        throw new IllegalArgumentException(s"$location requires an 'image_url' object")
    }
  }

  private def validateContentPart(part: Any, messageIndex: Int, partIndex: Int): Unit = {
    val location = s"messages[$messageIndex].content[$partIndex]"
    val fields = part match {
      case values: Map[_, _] => values.asInstanceOf[Map[String, Any]]
      case _ => throw new IllegalArgumentException(s"$location must be an object")
    }

    fields.get("type") match {
      case Some("text") => validateTextPart(fields, location)
      case Some("image_url") => validateImageUrlPart(fields, location)
      case Some(_: String) =>
        throw new IllegalArgumentException(
          s"$location has an unsupported type; supported types are 'text' and 'image_url'")
      case _ => throw new IllegalArgumentException(s"$location requires a string 'type' field")
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

  private def encodedContentPart(
      value: Any,
      elementType: StructType,
      messageIndex: Int,
      partIndex: Int
  ): Map[String, Any] = {
    val location = s"messages[$messageIndex].content[$partIndex]"
    if (value == null) {
      throw new IllegalArgumentException(s"$location must be an object")
    }
    val encoded = try {
      encodeStructuredValue(value, elementType)
    } catch {
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$location is invalid: ${e.getMessage}")
    }
    encoded match {
      case part: Map[_, _] =>
        val fields = part.asInstanceOf[Map[String, Any]]
        validateContentPart(fields, messageIndex, partIndex)
        fields
      case _ => throw new IllegalArgumentException(s"$location must be an object")
    }
  }

  private def invalidRoleError(messageIndex: Int): IllegalArgumentException = {
    new IllegalArgumentException(s"messages[$messageIndex].role must be a non-empty string")
  }

  private def validatedRole(message: Row, messageIndex: Int): String = {
    val roleIndex = message.schema.fieldNames.indexOf("role")
    if (roleIndex < 0 || roleIndex >= message.length) {
      throw invalidRoleError(messageIndex)
    }

    message.schema.fields(roleIndex).dataType match {
      case StringType =>
      case _ => throw invalidRoleError(messageIndex)
    }

    if (message.isNullAt(roleIndex)) {
      throw invalidRoleError(messageIndex)
    }

    message.get(roleIndex) match {
      case role: String if role.trim.nonEmpty => role
      case _ => throw invalidRoleError(messageIndex)
    }
  }

  private def contentField(message: Row, messageIndex: Int): (Int, DataType) = {
    val fieldIndex = message.schema.fieldNames.indexOf("content")
    if (fieldIndex < 0 || fieldIndex >= message.length) {
      throw new IllegalArgumentException(
        s"messages[$messageIndex].content must be a string or an array of content part objects")
    }
    fieldIndex -> message.schema.fields(fieldIndex).dataType
  }

  private def validateMessages(messages: CollectionSeq[Row]): Unit = {
    if (messages.isEmpty) {
      throw new IllegalArgumentException("messages must not be empty")
    }
    messages.zipWithIndex.foreach { case (message, messageIndex) =>
      if (message == null) {
        throw new IllegalArgumentException(s"messages[$messageIndex] must be an object")
      }
      validatedRole(message, messageIndex)
      val (fieldIndex, dataType) = contentField(message, messageIndex)
      dataType match {
        case StringType =>
        case ArrayType(elementType: StructType, _) =>
          contentItems(message.get(fieldIndex), messageIndex).zipWithIndex.foreach {
            case (part, partIndex) =>
              encodedContentPart(part, elementType, messageIndex, partIndex)
          }
        case ArrayType(_: MapType, _) =>
          encodedMapBackedContent(message.get(fieldIndex), messageIndex)
        case ArrayType(other, _) =>
          throw new IllegalArgumentException(
            s"Unsupported content part type: ${other.typeName}. Expected struct or map")
        case other =>
          throw new IllegalArgumentException(s"Unsupported content type: ${other.typeName}")
      }
    }
  }

  private def encodedMessageContent(message: Row, messageIndex: Int): Any = {
    val (fieldIndex, dataType) = contentField(message, messageIndex)
    dataType match {
      case StringType =>
        message.getAs[String]("content")
      case ArrayType(elementType: StructType, _) =>
        contentItems(message.get(fieldIndex), messageIndex).zipWithIndex.map {
          case (part, partIndex) =>
            encodedContentPart(part, elementType, messageIndex, partIndex)
        }
      case ArrayType(_: MapType, _) =>
        encodedMapBackedContent(message.get(fieldIndex), messageIndex)
      case ArrayType(other, _) =>
        throw new IllegalArgumentException(
          s"Unsupported content part type: ${other.typeName}. Expected struct or map")
      case other =>
        throw new IllegalArgumentException(s"Unsupported content type: ${other.typeName}")
    }
  }

  private def encodedMessageMaps(messages: CollectionSeq[Row]): Seq[Map[String, Any]] = {
    messages.zipWithIndex.map { case (message, messageIndex) =>
      if (message == null) {
        throw new IllegalArgumentException(s"messages[$messageIndex] must be an object")
      }

      val role = validatedRole(message, messageIndex)
      Map(
        "role" -> role,
        "content" -> encodedMessageContent(message, messageIndex)
      ).filter { case (_, value) => value != null }
    }.toSeq
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
    getStringEntityCollectionSeq(messages, optionalParams)
  }

  private def getStringEntityCollectionSeq(
      messages: CollectionSeq[Row],
      optionalParams: Map[String, Any]
  ): StringEntity = {
    val mappedMessages = encodedMessageMaps(messages)
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
