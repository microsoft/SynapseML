// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.services.openai.OpenAIAttachmentUtils
import org.apache.spark.sql.{DataFrame, Row, functions => F}
import org.apache.spark.sql.types.{StringType, StructType}
import spray.json._

private[ai] object AIDurableInput {
  private val ExternalBindings = Set(
    "previous_response_id", "conversation", "conversation_id", "file_id", "file_ids",
    "tools", "tool_choice", "tool_resources", "functions", "function_call", "retrieve", "run_id",
    "data_sources", "web_search_options", "prompt", "tool_call_id")
  private val OutputSchemas = Map(
    "chat_completions" -> Set(List("response_format", "json_schema", "schema")),
    "responses" -> Set(List("text", "format", "schema")))

  private def externalBindings(value: JsValue, path: List[String] = Nil,
                               outputSchemas: Set[List[String]] = Set.empty): Unit = value match {
    case JsObject(fields) if !outputSchemas(path) =>
      val bindings = fields.iterator.collect {
        case (name, entry) if ExternalBindings(name) && entry != JsNull => name
      }.toSeq.sorted
      require(bindings.isEmpty,
        "Durable requests cannot depend on provider state, retrieval, tools, or external resources; " +
          s"restricted fields: ${bindings.mkString(", ")}")
      content(value)
      fields.foreach { case (name, entry) => externalBindings(entry, path :+ name, outputSchemas) }
    case JsArray(items) => items.foreach(externalBindings(_))
    case _ =>
  }

  private def body(value: JsValue, api: String): Unit =
    externalBindings(JsObject(AIJson.obj(value).fields - "metadata"),
      outputSchemas = OutputSchemas.getOrElse(api, Set.empty))

  private def responseFormatBody(value: Any, api: String): JsObject = {
    val format = AIRequestAdapter.responseFormat(value, api)
    if (api == "responses") JsObject("text" -> JsObject("format" -> format))
    else JsObject("response_format" -> format)
  }

  private def questions(api: String, value: JsValue): Unit = (api, value) match {
    case ("systemone", JsObject(named)) =>
      named.values.foreach {
        // Question names and criteria labels are user-defined text, not provider binding fields.
        case JsObject(fields) =>
          externalBindings(JsObject(fields - "criteria"))
          fields.get("criteria").foreach {
            case JsObject(criteria) => criteria.values.foreach(externalBindings(_))
            case other => externalBindings(other)
          }
        case other => externalBindings(other)
      }
    case _ => externalBindings(value)
  }

  def validate(stage: AIPrompt, schema: StructType): Unit = {
    val api = stage.getApiType
    Seq(stage.subscriptionKey, stage.AADToken, stage.CustomAuthHeader,
      stage.customHeaders, stage.telemHeaders).foreach { p =>
      require(!stage.get(p).orElse(stage.getDefault(p)).exists(_.isRight),
        s"Durable mode rejects ${p.name}Col; rebind scalar credentials on resume")
    }
    require(!stage.isDefined(stage.previousResponseId),
      "Durable mode does not support previousResponseId or previousResponseIdCol")
    stage.get(stage.extraBody).orElse(stage.getDefault(stage.extraBody)).foreach {
      case Left(value) => body(AIJson.fromAny(value), api)
      case _ =>
    }
    stage.get(stage.responseFormat).orElse(stage.getDefault(stage.responseFormat)).foreach {
      case Left(value) => body(responseFormatBody(value, api), api)
      case _ =>
    }
    stage.get(stage.questions).orElse(stage.getDefault(stage.questions)).foreach {
      case Left(value) => questions(api, AIJson.fromAny(value))
      case _ =>
    }
    stage.getColumnTypes.foreach { case (name, _) => AIColumnPlan.resolve(schema, name) }
  }

  private def inline(value: JsValue, literalContent: Boolean = false): Unit = value match {
    case JsString(url) if !literalContent && url.trim.nonEmpty =>
      // Request validation handles malformed inline media as row errors; only external dependencies abort here.
      require(OpenAIAttachmentUtils.isDataUrl(url.trim),
        "Durable media must be inline base64 data URLs, not remote content or paths")
    case JsArray(items) => items.foreach(inline(_))
    case JsObject(fields) => fields.get("url").foreach(inline(_))
    case _ =>
  }

  private def content(part: JsValue): Unit = part match {
    case JsObject(fields) =>
      Seq("image_url", "file_data", "file_url").foreach { name =>
        // A string file_data is literal content, not a location to resolve.
        fields.get(name).foreach(inline(_, literalContent = name == "file_data"))
      }
    case _ =>
  }

  private def messages(value: JsValue): Unit = {
    externalBindings(value)
    value match {
      case JsArray(items) => items.foreach {
        case JsObject(fields) =>
          require(!fields.get("id").exists(_ != JsNull) && !fields.get("type").exists {
            case JsString("item_reference" | "function_call" | "function_call_output") => true
            case _ => false
          }, "Durable messages cannot reference provider items or tool results")
          fields.get("content").foreach {
            case JsArray(parts) => parts.foreach(content)
            case _ =>
          }
        case _ =>
      }
      case _ =>
    }
  }

  private def validateDeferredJson(kind: String, text: String): Unit = {
    val parsed = try Some(AIJson.parse(text)) catch {
      case _: IllegalArgumentException => None
    }
    // Inference parses these cells again and commits malformed values as terminal row errors.
    parsed.foreach { value =>
      if (kind.endsWith("_questions")) questions(kind.stripSuffix("_questions"), value)
      else if (kind == "systemone") externalBindings(value)
      else messages(value)
    }
  }

  private def validateBodyCell(kind: String, text: String, api: String): Unit = {
    // Malformed bodies and formats become terminal row errors; resource checks must not be deferred.
    val parsed = try Some(if (kind == "body") AIJson.objectString(text)
      else responseFormatBody(text, kind.stripSuffix("_format"))) catch {
      case _: IllegalArgumentException => None
    }
    parsed.foreach(body(_, api))
  }

  private def validateCell(kind: String, text: String, api: String): Unit = kind match {
    case "prompt" => () // Selecting the rendered value forces evaluation before any inference requests.
    case "inline" =>
      val normalized = text.trim
      if (normalized.nonEmpty) inline(JsString(normalized))
    case "body" | "chat_completions_format" | "responses_format" => validateBodyCell(kind, text, api)
    // Inference encodes message Rows directly, so parser failures cannot be deferred to row errors.
    case "messages" => messages(AIJson.parse(text))
    case "systemone" | "decisions" | "systemone_questions" | "decisions_questions" =>
      validateDeferredJson(kind, text)
    case _ => throw new IllegalArgumentException("Unsupported durable input kind")
  }

  private def validateCells(kinds: Seq[String], row: Row, api: String): Unit = {
    kinds.zipWithIndex.foreach { case (kind, index) =>
      if (!row.isNullAt(index)) validateCell(kind, row.getString(index), api)
    }
  }

  def validateRows(stage: AIPrompt, snapshot: DataFrame, preparedInput: Option[String] = None): Unit = {
    val api = stage.getApiType
    val kinds = Seq.newBuilder[String]
    val columns = Seq.newBuilder[org.apache.spark.sql.Column]
    def add(name: String, kind: String, json: Boolean = false): Unit = {
      val resolved = AIColumnPlan.resolve(snapshot.schema, name)
      kinds += kind
      val column = AIColumnPlan.column(resolved)
      columns += (if (json) F.to_json(column) else column)
    }
    stage.getColumnTypes.foreach { case (name, kind) => if (kind == "path") add(name, "inline") }
    if (stage.isSet(stage.messagesCol)) add(stage.getMessagesCol, "messages", json = true)
    stage.get(stage.extraBody).orElse(stage.getDefault(stage.extraBody)).flatMap(_.right.toOption)
      .foreach(add(_, "body"))
    stage.get(stage.responseFormat).orElse(stage.getDefault(stage.responseFormat)).flatMap(_.right.toOption)
      .foreach { name =>
        val dataType = snapshot.schema(AIColumnPlan.resolve(snapshot.schema, name)).dataType
        add(name, api + "_format", json = dataType != StringType)
      }
    stage.get(stage.questions).orElse(stage.getDefault(stage.questions)).flatMap(_.right.toOption)
      .foreach(add(_, api + "_questions"))
    if (AIRequestAdapter.NativeApis(api) && stage.getInputMode == "json") {
      preparedInput.orElse(stage.get(stage.stateCol)).orElse(stage.get(stage.inputCol))
        .foreach(add(_, api))
    } else preparedInput.foreach(add(_, "prompt"))
    val selectedKinds = kinds.result()
    if (selectedKinds.nonEmpty) {
      snapshot.select(columns.result(): _*).foreachPartition { rows: Iterator[Row] =>
        rows.foreach(validateCells(selectedKinds, _, api))
      }
    }
  }
}
