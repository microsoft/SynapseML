// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.services.openai.UsageUtils
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, DoubleType, IntegerType,
  MapType, StringType, StructType}
import spray.json.{JsArray, JsBoolean, JsNull, JsNumber, JsObject, JsString, JsValue}

import scala.util.control.NonFatal

/** Fixed public projections; the caller retains the original response separately for forward compatibility. */
private[services] object AIResponseCodec {
  case class Decoded(value: Any, usage: Row, responseId: String, diagnostic: Option[(String, String)])

  private val Missing: Null = None.orNull
  private val ChoiceType = new StructType()
    .add("type", StringType, nullable = false)
    .add("stringValue", StringType)
    .add("booleanValue", BooleanType)
  private val ChoiceProbabilityType = new StructType()
    .add("value", ChoiceType, nullable = false)
    .add("probability", DoubleType, nullable = false)
  private val ScoreProbabilityType = new StructType()
    .add("value", IntegerType, nullable = false)
    .add("label", StringType, nullable = false)
    .add("probability", DoubleType, nullable = false)
  private val AnswerType = new StructType()
    .add("type", StringType, nullable = false)
    .add("name", StringType)
    .add("probability", DoubleType)
    .add("choice", ChoiceType)
    .add("score", DoubleType)
    .add("confidence", DoubleType)
    .add("choiceProbabilities", ArrayType(ChoiceProbabilityType, containsNull = false))
    .add("scoreProbabilities", ArrayType(ScoreProbabilityType, containsNull = false))
  private val DecisionsType = new StructType()
    .add("model", StringType, nullable = false)
    .add("answers", ArrayType(AnswerType, containsNull = false), nullable = false)
    .add("usage", UsageUtils.UsageStructType)

  // The System One guide specifies no complete answer or usage member schema. JSON strings are
  // SynapseML's lossless representation, not an assertion that the provider returns string values.
  private val SystemOneType = new StructType()
    .add("model", StringType, nullable = false)
    .add("answers", MapType(StringType, StringType, valueContainsNull = false), nullable = false)
    .add("usage", StringType)

  def outputType(apiType: String): DataType = apiType match {
    case "chat_completions" | "responses" => StringType
    case "embeddings" => VectorType
    case "systemone" => SystemOneType
    case "decisions" => DecisionsType
    case _ => throw new IllegalArgumentException("Unsupported AI response API type")
  }

  /** Parsing/type failures have a constant, non-sensitive diagnostic; no provider text enters errors. */
  def decode(apiType: String, raw: String): Decoded = {
    outputType(apiType)
    try {
      val parsed = if (apiType == "responses") AIJson.parseResponsesResponse(raw) else AIJson.parse(raw)
      val fields = obj(parsed)
      val metadata = new Metadata
      val usage = metadata.read(readUsage(apiType, fields)).orNull
      val id = metadata.text(fields, "id").orNull
      metadata.text(fields, "model")
      val providerFailed = metadata.read(providerFailure(fields)).contains(true)
      val responseFailed = apiType == "responses" && metadata.text(fields, "status").contains("failed")
      val failed = providerFailed || responseFailed
      val decoded = try decodeValue(apiType, fields, usage, id, failed, metadata) catch {
        case NonFatal(_) => result(Missing, usage, id, Some(if (failed) "provider_error" else "parse_error"))
      }
      // An existing provider diagnostic takes precedence over malformed optional metadata.
      if (metadata.malformed && decoded.diagnostic.isEmpty) {
        result(decoded.value, usage, id, Some("parse_error"))
      } else decoded
    } catch {
      case NonFatal(_) => result(Missing, Missing, Missing, Some("parse_error"))
    }
  }

  private class Metadata {
    var malformed: Boolean = false

    def read[A](value: => A): Option[A] = try Some(value) catch {
      case NonFatal(_) =>
        malformed = true
        None
    }

    def text(fields: Map[String, JsValue], name: String): Option[String] =
      read(optionalString(fields, name)).flatten
  }

  private def decodeValue(apiType: String,
                          fields: Map[String, JsValue],
                          usage: Row,
                          id: String,
                          failed: Boolean,
                          metadata: Metadata): Decoded = {
    apiType match {
      case "chat_completions" => chat(fields, usage, id, failed, metadata)
      case "responses" => responses(fields, usage, id, failed, metadata)
      case "embeddings" =>
        if (failed) result(Missing, usage, id, Some("provider_error"))
        else result(embedding(fields), usage, id, None)
      case "systemone" => systemOne(fields, usage, id, failed)
      case "decisions" => decisions(fields, usage, id, failed)
      case _ => invalid()
    }
  }

  private def result(value: Any, usage: Row, id: String, code: Option[String]): Decoded =
    Decoded(value, usage, id, code.map { code =>
      val message = code match {
        case "provider_error" => "The provider reported a failure"
        case "refusal" => "The provider refused one or more answers"
        case "incomplete" => "The provider returned incomplete output"
        case "no_text" => "The response contained no assistant text"
        case "parse_error" => "The provider response did not match the supported JSON schema"
      }
      code -> message
    })

  private def invalid(): Nothing = throw new IllegalArgumentException("Invalid AI response field")

  private def obj(value: JsValue): Map[String, JsValue] = value match {
    case JsObject(fields) => fields
    case _ => invalid()
  }

  private def array(value: JsValue): Vector[JsValue] = value match {
    case JsArray(values) => values
    case _ => invalid()
  }

  private def string(value: JsValue): String = value match {
    case JsString(s) => s
    case _ => invalid()
  }

  private def required(fields: Map[String, JsValue], name: String): JsValue =
    fields.getOrElse(name, invalid())

  private def optional(fields: Map[String, JsValue], name: String): Option[JsValue] =
    fields.get(name).filterNot(_ == JsNull)

  private def optionalString(fields: Map[String, JsValue], name: String): Option[String] =
    optional(fields, name).map(string)

  private def count(value: JsValue): Long = value match {
    case JsNumber(n) if n.isWhole && n.isValidLong && n >= 0 => n.toLong
    case _ => invalid()
  }

  private def number(value: JsValue): Double = value match {
    case JsNumber(n) if !n.toDouble.isInfinity && !n.toDouble.isNaN => n.toDouble
    case _ => invalid()
  }

  private def probability(value: JsValue): Double = value match {
    case JsNumber(n) if n >= 0 && n <= 1 => number(value)
    case _ => invalid()
  }

  private def providerFailure(fields: Map[String, JsValue]): Boolean = optional(fields, "error").exists { value =>
    val error = obj(value)
    Seq("code", "message", "type", "param").foreach(optionalString(error, _))
    true
  }

  private def readUsage(apiType: String, fields: Map[String, JsValue]): Row = {
    optional(fields, "usage").map { value =>
      val usage = obj(value)
      val chatNames = apiType match {
        case "chat_completions" | "embeddings" => true
        case "responses" | "systemone" | "decisions" => false
        case _ => invalid()
      }
      val input = if (chatNames) "prompt_tokens" else "input_tokens"
      val output = if (chatNames) "completion_tokens" else "output_tokens"
      val inputDetails = if (chatNames) "prompt_tokens_details" else "input_tokens_details"
      val outputDetails = if (chatNames) "completion_tokens_details" else "output_tokens_details"
      def tokens(key: String): java.lang.Long = usage.get(key).map(v => Long.box(count(v))).orNull
      def details(key: String, known: Set[String]): Map[String, Long] =
        optional(usage, key).map { value =>
          obj(value).collect { case (name, tokenCount) if known(name) && tokenCount != JsNull =>
            name -> count(tokenCount)
          }
        }.getOrElse(Map.empty)
      Row(tokens(input), tokens(output), tokens("total_tokens"),
        details(inputDetails, Set("cached_tokens", "cache_write_tokens", "audio_tokens")),
        details(outputDetails, Set("reasoning_tokens", "audio_tokens",
          "accepted_prediction_tokens", "rejected_prediction_tokens")))
    }.orNull
  }

  private def chat(fields: Map[String, JsValue],
                   usage: Row,
                   id: String,
                   failed: Boolean,
                   metadata: Metadata): Decoded = {
    val choices = optional(fields, "choices").map(array).getOrElse {
      if (failed) Vector.empty else invalid()
    }
    // Deliberately project only the first choice, while checking known fields on every choice.
    val parsed = choices.map { choice =>
      if (failed) metadata.read(chatChoice(choice, metadata)) else Some(chatChoice(choice, metadata))
    }
    val first = parsed.headOption.flatten
    val text = first.flatMap(_._1)
    val code = if (failed) Some("provider_error")
    else if (first.exists(_._2)) Some("refusal")
    else if (first.exists(_._3)) Some("incomplete")
    else if (text.isEmpty) Some("no_text")
    else None
    result(text.orNull, usage, id, code)
  }

  private def chatChoice(choice: JsValue, metadata: Metadata): (Option[String], Boolean, Boolean) = {
    val fields = obj(choice)
    count(required(fields, "index"))
    val message = obj(required(fields, "message"))
    if (string(required(message, "role")) != "assistant") invalid()
    val content = optionalString(message, "content")
    val refusal = metadata.text(message, "refusal").exists(_.nonEmpty)
    val finish = metadata.text(fields, "finish_reason")
    (content, refusal || finish.contains("content_filter"), finish.contains("length"))
  }

  private def incompleteResponse(status: Option[String], details: Option[JsValue]): Boolean =
    details.isDefined || status.exists(s => Set("incomplete", "in_progress", "queued", "cancelled")(s))

  private def responses(fields: Map[String, JsValue],
                        usage: Row,
                        id: String,
                        failed: Boolean,
                        metadata: Metadata): Decoded = {
    val status = metadata.text(fields, "status")
    val incomplete = optional(fields, "incomplete_details")
    incomplete.foreach { value => metadata.read(optionalString(obj(value), "reason")) }
    val output = optional(fields, "output").map(array).getOrElse {
      if (failed) Vector.empty else invalid()
    }
    val messages = output.flatMap { item =>
      if (failed) metadata.read(responseMessage(item, metadata, failed)).toSeq.flatten
      else responseMessage(item, metadata, failed).toSeq
    }
    val parts = messages.flatMap(_._1)
    val texts = parts.flatMap(_._1)
    val code = if (failed) Some("provider_error")
    else if (parts.exists(_._2)) Some("refusal")
    else if (incompleteResponse(status, incomplete) || messages.exists(_._2)) Some("incomplete")
    else if (texts.isEmpty) Some("no_text")
    else None
    result(if (texts.isEmpty) Missing else texts.mkString, usage, id, code)
  }

  private def responseMessage(item: JsValue,
                             metadata: Metadata,
                             failed: Boolean): Option[(Vector[(Option[String], Boolean)], Boolean)] = {
    val fields = obj(item)
    if (string(required(fields, "type")) == "message") {
      val role = string(required(fields, "role"))
      val parts = array(required(fields, "content")).flatMap { part =>
        if (failed) metadata.read(responsePart(part)) else Some(responsePart(part))
      }
      val incomplete = metadata.text(fields, "status").exists(Set("incomplete", "in_progress"))
      if (role == "assistant") Some((parts, incomplete)) else None
    } else None
  }

  private def responsePart(part: JsValue): (Option[String], Boolean) = {
    val fields = obj(part)
    string(required(fields, "type")) match {
      case "output_text" => (Some(string(required(fields, "text"))), false)
      case "refusal" =>
        string(required(fields, "refusal"))
        (None, true)
      case _ => (None, false)
    }
  }

  private def embedding(fields: Map[String, JsValue]): org.apache.spark.ml.linalg.Vector = {
    optional(fields, "object").foreach { value => if (string(value) != "list") invalid() }
    val data = array(required(fields, "data"))
    if (data.size != 1) invalid()
    val item = obj(data.head)
    if (count(required(item, "index")) != 0) invalid()
    optional(item, "object").foreach { value => if (string(value) != "embedding") invalid() }
    val values = array(required(item, "embedding"))
    if (values.isEmpty) invalid()
    Vectors.dense(values.map(number).toArray)
  }

  private def tagged(value: JsValue): Row = value match {
    case JsString(s) => Row("string", s, Missing)
    case JsBoolean(b) => Row("boolean", Missing, b)
    case _ => invalid()
  }

  private def systemOne(fields: Map[String, JsValue], usage: Row, id: String, failed: Boolean): Decoded = {
    if (failed) result(Missing, usage, id, Some("provider_error"))
    else {
      val answers = obj(required(fields, "answers")).map { case (key, value) => key -> value.compactPrint }
      if (answers.isEmpty) invalid()
      val nativeUsage = optional(fields, "usage").map(_.compactPrint).orNull
      result(Row(string(required(fields, "model")), answers, nativeUsage), usage, id, None)
    }
  }

  private def decisions(fields: Map[String, JsValue], usage: Row, id: String, failed: Boolean): Decoded = {
    if (failed) result(Missing, usage, id, Some("provider_error"))
    else {
      val answers = array(required(fields, "answers")).map(answer)
      if (answers.isEmpty) invalid()
      val names = answers.filterNot(_.isNullAt(1)).map(_.getString(1))
      if (names.distinct.size != names.size) invalid()
      val value = Row(string(required(fields, "model")), answers, usage)
      result(value, usage, id, if (answers.exists(_.getString(0) == "refusal")) Some("refusal") else None)
    }
  }

  private def answer(value: JsValue): Row = {
    val fields = obj(value)
    val kind = string(required(fields, "type"))
    val name = optionalString(fields, "name").orNull
    kind match {
      case "predicate" =>
        Row(kind, name, probability(required(fields, "probability")), Missing, Missing, Missing, Missing, Missing)
      case "refusal" => Row(kind, name, Missing, Missing, Missing, Missing, Missing, Missing)
      case "choice" => choiceAnswer(fields, name)
      case "score" => scoreAnswer(fields, name)
      case _ => invalid()
    }
  }

  private def choiceAnswer(fields: Map[String, JsValue], name: String): Row = {
    val choice = tagged(required(fields, "choice"))
    val probabilities = array(required(fields, "probabilities")).map { value =>
      val fields = obj(value)
      Row(tagged(required(fields, "value")), probability(required(fields, "probability")))
    }
    val values = probabilities.map(_.getStruct(0))
    if (values.size < 2 || values.distinct.size != values.size || !values.contains(choice)) invalid()
    Row("choice", name, Missing, choice, Missing, probability(required(fields, "confidence")), probabilities, Missing)
  }

  private def scoreAnswer(fields: Map[String, JsValue], name: String): Row = {
    val probabilities = array(required(fields, "probabilities")).zipWithIndex.map { case (value, index) =>
      val fields = obj(value)
      if (count(required(fields, "value")) != index) invalid()
      Row(index, string(required(fields, "label")), probability(required(fields, "probability")))
    }
    val score = required(fields, "score") match {
      case value @ JsNumber(n) if n >= 0 && n <= probabilities.size - 1 => number(value)
      case _ => invalid()
    }
    Row("score", name, Missing, Missing, score, probability(required(fields, "confidence")), Missing, probabilities)
  }
}
