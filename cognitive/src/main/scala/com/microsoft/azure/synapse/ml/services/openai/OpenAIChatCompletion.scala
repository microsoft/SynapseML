// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.io.http.ErrorUtils
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.AnyJsonFormat.anyFormat
import com.microsoft.azure.synapse.ml.param.ServiceParam
import com.microsoft.azure.synapse.ml.services.{HasCognitiveServiceInput, HasInternalJsonOutputParser}
import org.apache.http.entity.{AbstractHttpEntity, ContentType, StringEntity}
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.util._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{functions => F, DataFrame, Dataset, Row}
import org.apache.spark.sql.types._
import spray.json.DefaultJsonProtocol._
import spray.json._

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

object OpenAIChatCompletion extends ComplexParamsReadable[OpenAIChatCompletion] {
  private val AllowedContentTypes = Set("text", "image_url")
  private val AllowedPartKeys = Set("type", "text", "image_url", "detail")

  /** Row-local validation of structured Chat content. Returns an ErrorUtils.ErrorSchema-shaped Row
    * (human-readable response message, null status) describing the first malformed message, else None.
    * Never throws from the request path: any unexpected failure is itself turned into a row-local error,
    * so both the request UDF and shouldSkip can treat malformed content as a skip instead of crashing. */
  private[openai] def validateMessagesForError(messages: scala.collection.Seq[Row]): Option[Row] = {
    try {
      val errors = Option(messages).getOrElse(Seq.empty).iterator.zipWithIndex
        .flatMap { case (message, idx) => validateMessage(message, idx) }
      if (errors.hasNext) Some(Row(errors.next(), null)) else None //scalastyle:ignore null
    } catch {
      case NonFatal(e) =>
        Some(Row(s"Invalid chat message content: ${e.getMessage}", null)) //scalastyle:ignore null
    }
  }

  // A null element inside a non-null messages array is a row-local error (it would otherwise NPE during
  // serialization). Each message must also carry a String role and a String/Array content field; anything
  // else is malformed and is flagged here so it can never throw inside the request UDF (Finding 1).
  private def validateMessage(message: Row, idx: Int): Option[String] = {
    if (message == null) { //scalastyle:ignore null
      Some(s"message $idx is null")
    } else {
      validateRole(message, idx).orElse(validateContent(message, idx))
    }
  }

  // The Chat wire requires a present, String role. A missing/null/non-string role would otherwise make
  // encodeMessagesToMap's row.getAs[String]("role") throw (or silently drop the role), so it is a row-local
  // error. The role value itself is deliberately NOT restricted -- the service validates role names.
  private def validateRole(message: Row, idx: Int): Option[String] =
    fieldIndex(message, "role") match {
      case None => Some(s"message $idx is missing a role field")
      case Some(i) => message.get(i) match {
        case null => Some(s"message $idx has a null role") //scalastyle:ignore null
        case _: String => None
        case other => Some(s"message $idx has a non-string role of type ${typeName(other)}")
      }
    }

  // Content must be non-null and declared as a String or an Array of parts -- the only shapes
  // encodeMessagesToMap can serialize. Invalid values are rejected before the request UDF.
  private def validateContent(message: Row, idx: Int): Option[String] =
    fieldIndex(message, "content") match {
      case None => Some(s"message $idx is missing a content field")
      case Some(i) =>
        message.schema.fields(i).dataType match {
          case _: StringType if message.get(i) == null => //scalastyle:ignore null
            Some(s"message $idx has null content")
          case _: StringType => None
          case _: ArrayType if message.get(i) == null => //scalastyle:ignore null
            Some(s"message $idx has null content")
          case _: ArrayType => validateContentParts(message.get(i))
          case other => Some(s"message $idx has an unsupported content type: ${other.typeName}")
        }
    }

  private def validateContentParts(content: Any): Option[String] = content match {
    case null => None //scalastyle:ignore null
    case parts: scala.collection.Seq[_] =>
      val errors = parts.iterator.zipWithIndex
        .flatMap { case (part, idx) => validateContentPart(part, idx) }
      if (errors.hasNext) Some(errors.next()) else None
    case other =>
      Some(s"message has unsupported content type: ${typeName(other)}")
  }

  private def validateContentPart(part: Any, idx: Int): Option[String] = part match {
    case null => Some(s"content part $idx is null") //scalastyle:ignore null
    case m: scala.collection.Map[_, _] =>
      val sm = m.asInstanceOf[scala.collection.Map[String, Any]]
      unsupportedFieldError(idx, sm.keys)
        .orElse(validatePartFields(idx, optString(sm.get("type")), sm.get("text"), sm.get("image_url")))
    case r: Row =>
      unsupportedFieldError(idx, r.schema.fieldNames.toSeq)
        .orElse(validatePartFields(idx, rowFieldString(r, "type"),
          rowFieldPresent(r, "text"), rowFieldPresent(r, "image_url")))
    case other =>
      Some(s"content part $idx has an unsupported element type: ${typeName(other)}")
  }

  // The Chat wire contract for a content part is exactly {type, text, image_url, detail}; any other
  // field (e.g. a Long/Timestamp/Binary leaf) would otherwise reach AnyJsonFormat and abort the whole
  // Spark job, so an unsupported field is a row-local error here (defect: unsupported extra leaf fields).
  private def unsupportedFieldError(idx: Int, keys: Iterable[String]): Option[String] =
    keys.find(k => !AllowedPartKeys.contains(k))
      .map(k => s"content part $idx has an unsupported field '$k'")

  private def validatePartFields(idx: Int,
                                 typeOpt: Option[String],
                                 textPresence: Option[Any],
                                 imageUrlValue: Option[Any]): Option[String] = {
    // Require an exact canonical type: a noncanonical string such as " image_url " is a row-local error
    // (not silently trimmed), so validation and the exact-match serializer agree (defect: canonicalization).
    val partType = typeOpt.getOrElse("")
    if (partType.trim.isEmpty) {
      Some(s"content part $idx is missing a type")
    } else if (!AllowedContentTypes.contains(partType)) {
      Some(s"content part $idx has an unsupported type '$partType'")
    } else if (partType == "image_url") {
      validateImageUrlField(idx, imageUrlValue)
    } else {
      validateTextField(idx, textPresence) // partType == "text"
    }
  }

  // A "text" part must carry a present, String text value so it serializes as a valid {type,text} wire
  // part rather than an invalid bare {"type":"text"} (or an NPE on a present-null value). Empty and
  // whitespace strings stay valid so OpenAIPrompt's injected system text part -- even for an empty or
  // whitespace systemPrompt -- keeps the legacy empty-string Chat behavior (wave-2 backward-compat).
  private def validateTextField(idx: Int, textPresence: Option[Any]): Option[String] = textPresence match {
    case None => Some(s"content part $idx is missing a text value")
    case Some(null) => Some(s"content part $idx has a null text value") //scalastyle:ignore null
    case Some(_: String) => None
    case Some(other) => Some(s"content part $idx has a non-string text value of type ${typeName(other)}")
  }

  // Some(value) (possibly Some(null)) when the field exists in the row's schema and arity; None when it is
  // absent. Unlike rowFieldValue this preserves a present-but-null value so text presence can be checked.
  private def rowFieldPresent(row: Row, name: String): Option[Any] =
    fieldIndex(row, name).map(row.get)

  private def fieldIndex(row: Row, name: String): Option[Int] =
    Some(row.schema.fieldNames.indexOf(name)).filter(i => i >= 0 && i < row.length)

  private def validateImageUrlField(idx: Int, value: Option[Any]): Option[String] = value match {
    case None | Some(null) => Some(s"content part $idx has an empty image_url") //scalastyle:ignore null
    case Some(s: String) =>
      if (s.trim.isEmpty) Some(s"content part $idx has an empty image_url") else None
    case Some(m: scala.collection.Map[_, _]) =>
      validateNestedImageUrl(idx, m.asInstanceOf[scala.collection.Map[String, Any]].get("url"))
    case Some(r: Row) => validateNestedImageUrl(idx, rowFieldPresent(r, "url"))
    case Some(other) =>
      Some(s"content part $idx has a non-string image_url of type ${typeName(other)}")
  }

  private def validateNestedImageUrl(idx: Int, value: Option[Any]): Option[String] = value match {
    case None | Some(null) => Some(s"content part $idx has an empty image_url") //scalastyle:ignore null
    case Some(s: String) =>
      if (s.trim.isEmpty) Some(s"content part $idx has an empty image_url") else None
    case Some(other) =>
      Some(s"content part $idx has a non-string image_url.url of type ${typeName(other)}")
  }

  private def typeName(value: Any): String =
    if (value == null) "null" else value.getClass.getSimpleName //scalastyle:ignore null

  private def optString(value: Option[Any]): Option[String] =
    value.flatMap(v => Option(v)).map(_.toString)

  private def rowFieldValue(row: Row, name: String): Option[Any] =
    fieldIndex(row, name).flatMap(i => Option(row.get(i)))

  private def rowFieldString(row: Row, name: String): Option[String] =
    rowFieldValue(row, name).map(_.toString)
}

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

  override def shouldSkip(row: Row): Boolean = {
    if (super.shouldSkip(row)) {
      true
    } else {
      // Re-validate here (the errorCol is not part of the request struct) so malformed content skips
      // HTTP: the request UDF returns None and the row keeps its row-local validation error.
      val messages = row.getAs[scala.collection.Seq[Row]](getMessagesCol)
      Option(messages).isEmpty || OpenAIChatCompletion.validateMessagesForError(messages).isDefined
    }
  }

  private lazy val validateMessagesUDF: UserDefinedFunction =
    UDFUtils.oldUdf(OpenAIChatCompletion.validateMessagesForError _, ErrorUtils.ErrorSchema)

  private def columnsMatch(left: String, right: String): Boolean = SQLConf.get.resolver(left, right)

  private def validatePublicColumnNames(): Unit = {
    val named = Seq(
      get(messagesCol).map("messagesCol" -> _),
      Some("outputCol" -> getOutputCol),
      Some("errorCol" -> getErrorCol)
    ).flatten
    val collisions = named.combinations(2).collect {
      case Seq(left, right) if columnsMatch(left._2, right._2) => Seq(left, right)
    }.toSeq
    require(collisions.isEmpty,
      "messagesCol, outputCol, and errorCol must reference distinct columns, but found collisions: " +
        collisions.map(entries => s"${entries.map(_._1).mkString(" == ")} -> '${entries.head._2}'")
          .mkString("; "))
  }

  override def transformSchema(schema: StructType): StructType = {
    validatePublicColumnNames()
    validateMessagesSchema(schema)
    super.transformSchema(schema)
  }

  // Fail fast on a messages schema that is deterministically incompatible: when the element struct
  // declares a role field it must be a String (a non-string role would ClassCastException in the request
  // UDF). Per-row value problems (null role, malformed parts, off-spec content values) stay row-local at
  // runtime -- transform never invokes this, so no data row can abort the Spark job (Finding 1).
  private def validateMessagesSchema(schema: StructType): Unit =
    schema.fields.find(f => columnsMatch(f.name, getMessagesCol)).map(_.dataType).foreach {
      case ArrayType(element: StructType, _) =>
        element.fields.find(f => columnsMatch(f.name, "role")).foreach { roleField =>
          require(roleField.dataType.isInstanceOf[StringType],
            s"The '$getMessagesCol' messages column requires a String 'role' field, but found " +
              s"${roleField.dataType.simpleString}.")
        }
      case _ => ()
    }

  override def transform(dataset: Dataset[_]): DataFrame = {
    validatePublicColumnNames()
    // Populate the row-local errorCol with any structured-content validation error before delegating to
    // the base transformer. Pre-existing errorCol values win (coalesce). No temp/saved columns and no
    // messages mutation are needed: shouldSkip re-validates to bypass HTTP, and the base transformer
    // coalesces service errors into this same errorCol, so messagesCol and public column ordering are
    // preserved.
    val df = dataset.toDF()
    val validation = validateMessagesUDF(F.col(getMessagesCol))
    val withErrors =
      df.columns.find(columnsMatch(_, getErrorCol)) match {
        case Some(existingErrorCol) =>
          df.withColumn(getErrorCol, F.coalesce(F.col(existingErrorCol), validation))
        case None =>
          df.withColumn(getErrorCol, validation)
      }
    super.transform(withErrors)
  }

  override protected def getVectorParamMap: Map[String, String] = super.getVectorParamMap
    .updated("messages", getMessagesCol)

  override def responseDataType: DataType = ChatModelResponse.schema

  private[openai] def getStringEntity(messages: Seq[Row], optionalParams: Map[String, Any]): StringEntity = {
    val mappedMessages = encodeMessagesToMap(messages)
      .map(_.filter { case (_, value) => value != null }) //scalastyle:ignore null
      .map { m =>
        m.get("content") match {
          case Some(parts: scala.collection.Seq[_]) => m.updated("content", serializeChatContent(parts))
          case _ => m
        }
      }
    val fullPayload = optionalParams.updated("messages", mappedMessages)
    new StringEntity(fullPayload.toJson.compactPrint, ContentType.APPLICATION_JSON)
  }

  // Chat Completions content handling: when any part is an image_url, preserve the structured content
  // as a JSON array (reshaping a flat image_url String into the nested {url, detail?} wire object);
  // otherwise collapse to a single text string to keep the legacy wire shape (issue #2246).
  private def serializeChatContent(parts: scala.collection.Seq[Any]): Any = {
    val partMaps = parts.collect {
      case mp: scala.collection.Map[_, _] => mp.asInstanceOf[scala.collection.Map[String, Any]]
    }
    val hasImage = partMaps.exists(_.get("type").contains("image_url"))
    if (hasImage) {
      partMaps.iterator.map(reshapeImagePart).toList
    } else {
      partMaps.flatMap(_.get("text").flatMap(Option(_)).map(_.toString)).mkString("\n")
    }
  }

  // Emit ONLY the canonical Chat wire fields for a part: an image_url part becomes
  // {type, image_url:{url, detail?}} and any other part becomes {type, text}. Projecting to these
  // fields keeps unsupported extra leaf fields (Long/Timestamp/Binary, sibling or nested) out of
  // AnyJsonFormat, so a malformed part can never abort the Spark job (defect: unsupported extra leaf).
  private def reshapeImagePart(part: scala.collection.Map[String, Any]): Map[String, Any] = {
    part.get("type") match {
      case Some("image_url") =>
        Map("type" -> "image_url", "image_url" -> nestedImageUrl(part))
      case _ =>
        part.get("type").map("type" -> _).toMap ++ part.get("text").flatMap(Option(_)).map(t => "text" -> t.toString)
    }
  }

  // Build the nested {url, detail?} wire object from a flat image_url String (folding in an optional
  // top-level detail) or from an already-nested struct/map, keeping only the canonical url/detail keys.
  private def nestedImageUrl(part: scala.collection.Map[String, Any]): Map[String, Any] = {
    val (url, nestedDetail) = part.get("image_url").flatMap(Option(_)) match {
      case Some(m: scala.collection.Map[_, _]) =>
        val nm = m.asInstanceOf[scala.collection.Map[String, Any]]
        (nm.get("url").flatMap(Option(_)).map(_.toString), nm.get("detail").collect { case d: String => d })
      case Some(other) => (Some(other.toString), None)
      case None => (None, None)
    }
    val detail = nestedDetail.orElse(part.get("detail").collect { case d: String => d })
    Map[String, Any]() ++ url.map("url" -> _) ++ detail.map("detail" -> _)
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
