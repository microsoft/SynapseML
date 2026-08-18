// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import spray.json._

import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.util.Try

/** Pure normalization and validation for OpenAI tool payloads.
  *
  * Function tools may use either the flat Responses shape or the nested Chat Completions
  * shape. They are stored in the flat Responses shape and converted to the target API on
  * request assembly. Other tool types pass through so provider-specific types remain usable.
  */
object OpenAIToolUtils {
  private val FunctionNamePattern = "^[A-Za-z0-9_-]{1,64}$".r

  val DocumentedToolChoices: Seq[String] = Seq("none", "auto", "required")
  val MessageItemType = "message"
  val FunctionCallItemType = "function_call"
  val FunctionCallOutputItemType = "function_call_output"
  val ReasoningItemType = "reasoning"
  val EncryptedContentField = "encrypted_content"
  val ContentFilterReason = "content_filter"
  val MaxOutputTokensReason = "max_output_tokens"
  val MaxMetadataEntries = 16
  val MaxMetadataKeyChars = 64
  val MaxMetadataValueChars = 512
  val MaxSafetyIdentifierChars = 64
  val MaxTopLogprobs = 20

  val AzureV1OnlyFields: Seq[String] = Seq(
    "max_tool_calls",
    "include",
    "top_logprobs",
    "safety_identifier",
    "prompt_cache_key",
    "conversation",
    "reasoning.summary",
    "reasoning.context",
    "reasoning.mode"
  )

  // scalastyle:off cyclomatic.complexity
  def toJsValue(value: Any): JsValue = value match {
    case null => JsNull //scalastyle:ignore null
    case v: JsValue => v
    case v: String => JsString(v)
    case v: Boolean => JsBoolean(v)
    case v: Int => JsNumber(v)
    case v: Long => JsNumber(v)
    case v: Double => JsNumber(v)
    case v: Float => JsNumber(BigDecimal(v.toString))
    case v: BigDecimal => JsNumber(v)
    case v: java.lang.Boolean => JsBoolean(v.booleanValue())
    case v: java.lang.Number => JsNumber(BigDecimal(v.toString))
    case v: Map[_, _] => jsObject(v.toSeq.map { case (k, item) => k.toString -> item })
    case v: Seq[_] => JsArray(v.map(toJsValue).toVector)
    case v: Array[_] => JsArray(v.map(toJsValue).toVector)
    case v: java.util.Map[_, _] =>
      jsObject(v.asScala.toSeq.map { case (k, item) => k.toString -> item })
    case v: java.lang.Iterable[_] => JsArray(v.asScala.toVector.map(toJsValue))
    case other =>
      throw new IllegalArgumentException(
        s"Cannot serialize tool value of type ${other.getClass.getName}")
  }
  // scalastyle:on cyclomatic.complexity

  private def jsObject(pairs: Seq[(String, Any)]): JsObject =
    JsObject(ListMap(pairs.map { case (key, value) => key -> toJsValue(value) }: _*))

  def parseTools(json: String): JsArray = {
    val parsed = Try(json.parseJson).getOrElse {
      throw new IllegalArgumentException(
        "tools must be a JSON array of tool objects; got unparsable text")
    }
    parsed match {
      case JsArray(elements) =>
        val normalized = elements.zipWithIndex.map {
          case (obj: JsObject, index) => normalizeTool(obj, index)
          case (other, index) =>
            throw new IllegalArgumentException(
              s"tools must be a JSON array of tool objects; element $index is ${kindOf(other)}")
        }
        val names = normalized.flatMap(functionName)
        names.zipWithIndex.collectFirst {
          case (name, index) if names.take(index).contains(name) => name
        }.foreach(name =>
          throw new IllegalArgumentException(s"Duplicate function tool name '$name'"))
        JsArray(normalized)
      case other =>
        throw new IllegalArgumentException(
          s"tools must be a JSON array of tool objects; got ${kindOf(other)}")
    }
  }

  def toChatCompletionsTools(tools: JsArray): JsArray = JsArray(tools.elements.map {
    case tool: JsObject if tool.fields.get("type").contains(JsString("function")) =>
      val function = JsObject(ListMap(tool.fields.toSeq.filterNot(_._1 == "type"): _*))
      JsObject(ListMap(
        "type" -> JsString("function"),
        "function" -> function
      ))
    case other => other
  })

  def normalizeTool(tool: JsObject, index: Int): JsObject = {
    val toolType = tool.fields.get("type").collect {
      case JsString(value) if value.trim.nonEmpty => value.trim
    }.getOrElse {
      throw new IllegalArgumentException(s"Tool at index $index is missing 'type'")
    }

    if (toolType != "function") {
      tool
    } else {
      val siblingPairs = tool.fields.toSeq.filterNot {
        case (key, _) => key == "type" || key == "function"
      }
      val innerPairs = tool.fields.get("function") match {
        case Some(obj: JsObject) => obj.fields.toSeq
        case _ => Seq.empty
      }
      val innerKeys = innerPairs.map(_._1).toSet
      val merged = siblingPairs.filterNot { case (key, _) => innerKeys.contains(key) } ++ innerPairs
      val fields = ListMap(merged: _*)
      validateFunctionTool(fields, index)
      JsObject(ListMap((Seq("type" -> JsString("function")) ++ fields.toSeq): _*))
    }
  }

  private def validateFunctionTool(fields: Map[String, JsValue], index: Int): Unit = {
    val name = fields.get("name").collect { case JsString(value) => value }.getOrElse("")
    require(
      FunctionNamePattern.pattern.matcher(name).matches(),
      s"Invalid function tool name '$name' at index $index; must match ^[A-Za-z0-9_-]{1,64}$$")
    fields.get("parameters").foreach {
      case _: JsObject =>
      case JsNull =>
      case _ =>
        throw new IllegalArgumentException(
          s"Tool '$name' parameters must be a JSON Schema object or null")
    }
  }

  def toolsToJson(tools: Seq[Map[String, Any]]): String =
    parseTools(JsArray(tools.map(toJsValue).toVector).compactPrint).compactPrint

  def toolsToJson(tools: java.util.List[java.util.Map[String, Object]]): String =
    parseTools(JsArray(tools.asScala.toVector.map(toJsValue)).compactPrint).compactPrint

  def functionTool(
      name: String,
      description: String,
      parameters: Map[String, Any],
      strict: Boolean): Map[String, Any] =
    ListMap(
      "type" -> "function",
      "name" -> name,
      "description" -> description,
      "parameters" -> parameters,
      "strict" -> strict
    )

  def parseToolChoice(value: String): Option[JsValue] = {
    val trimmed = Option(value).map(_.trim).getOrElse("")
    if (trimmed.isEmpty) {
      None
    } else if (trimmed.startsWith("[")) {
      throw new IllegalArgumentException(
        "toolChoice must be a string or an object with a 'type' field")
    } else if (trimmed.startsWith("{")) {
      val obj = Try(trimmed.parseJson).toOption.collect {
        case value: JsObject => value
      }.getOrElse {
        throw new IllegalArgumentException("toolChoice object must be valid JSON")
      }
      require(
        obj.fields.get("type").exists {
          case JsString(toolType) => toolType.trim.nonEmpty
          case _ => false
        },
        "toolChoice must be a string or an object with a 'type' field")
      if (obj.fields.get("type").contains(JsString("function"))) {
        require(
          functionChoiceName(obj).exists(_.nonEmpty),
          "toolChoice type 'function' requires a function name")
      }
      Some(obj)
    } else {
      Some(JsString(trimmed))
    }
  }

  private def functionChoiceName(choice: JsObject): Option[String] =
    choice.fields.get("name").collect { case JsString(value) => value }
      .orElse(choice.fields.get("function").collect {
        case function: JsObject =>
          function.fields.get("name").collect { case JsString(value) => value }
      }.flatten)

  private def convertFunctionChoice(choice: JsObject, nested: Boolean): JsObject = {
    val name = functionChoiceName(choice).getOrElse {
      throw new IllegalArgumentException("toolChoice type 'function' requires a function name")
    }
    if (nested) {
      JsObject(ListMap(
        "type" -> JsString("function"),
        "function" -> JsObject("name" -> JsString(name))
      ))
    } else {
      JsObject(ListMap(
        "type" -> JsString("function"),
        "name" -> JsString(name)
      ))
    }
  }

  private def allowedToolsConfig(choice: JsObject): JsObject =
    choice.fields.get("allowed_tools").collect {
      case allowed: JsObject => allowed
    }.getOrElse {
      JsObject(choice.fields.filter { case (name, _) => name == "mode" || name == "tools" })
    }

  private def convertAllowedToolsChoice(choice: JsObject, nested: Boolean): JsObject = {
    val allowed = allowedToolsConfig(choice)
    val tools = allowed.fields.get("tools") match {
      case Some(JsArray(values)) =>
        JsArray(values.map {
          case selector: JsObject
              if selector.fields.get("type").contains(JsString("function")) =>
            convertFunctionChoice(selector, nested)
          case other => other
        })
      case other => other.getOrElse(JsArray())
    }
    val converted = JsObject(allowed.fields.updated("tools", tools))
    if (nested) {
      JsObject(ListMap(
        "type" -> JsString("allowed_tools"),
        "allowed_tools" -> converted
      ))
    } else {
      JsObject(ListMap(
        (Seq("type" -> JsString("allowed_tools")) ++ converted.fields.toSeq): _*
      ))
    }
  }

  def toResponsesToolChoice(choice: JsValue): JsValue = choice match {
    case obj: JsObject if obj.fields.get("type").contains(JsString("function")) =>
      convertFunctionChoice(obj, nested = false)
    case obj: JsObject if obj.fields.get("type").contains(JsString("allowed_tools")) =>
      convertAllowedToolsChoice(obj, nested = false)
    case other => other
  }

  def toChatCompletionsToolChoice(choice: JsValue): JsValue = choice match {
    case obj: JsObject if obj.fields.get("type").contains(JsString("function")) =>
      convertFunctionChoice(obj, nested = true)
    case obj: JsObject if obj.fields.get("type").contains(JsString("allowed_tools")) =>
      convertAllowedToolsChoice(obj, nested = true)
    case other => other
  }

  private def selectedFunctionNames(choice: JsValue): Seq[String] = choice match {
    case obj: JsObject if obj.fields.get("type").contains(JsString("function")) =>
      functionChoiceName(obj).toSeq
    case obj: JsObject if obj.fields.get("type").contains(JsString("allowed_tools")) =>
      allowedToolsConfig(obj).fields.get("tools").collect {
        case JsArray(values) =>
          values.collect { case selector: JsObject => functionChoiceName(selector) }.flatten
      }.getOrElse(Vector.empty)
    case _ => Seq.empty
  }

  def validateToolChoiceAgainst(tools: JsArray, choice: JsValue): Unit = {
    val declared = tools.elements.flatMap(functionName)
    selectedFunctionNames(choice).foreach { name =>
      require(
        declared.contains(name),
        s"toolChoice references unknown function '$name'; declared: ${declared.mkString(", ")}")
    }
  }

  def parseInputItems(json: String): Vector[JsValue] =
    Option(json).map(_.trim).filter(_.nonEmpty).map { text =>
      Try(text.parseJson).toOption match {
        case Some(JsArray(items)) => items
        case Some(obj: JsObject) => Vector(obj)
        case _ =>
          throw new IllegalArgumentException(
            "inputItemsCol must contain a JSON array of Responses input items")
      }
    }.getOrElse(Vector.empty)

  def functionName(tool: JsValue): Option[String] = tool match {
    case obj: JsObject if obj.fields.get("type").contains(JsString("function")) =>
      obj.fields.get("name").collect { case JsString(value) => value }
    case _ => None
  }

  private def kindOf(value: JsValue): String = value match {
    case JsNull => "null"
    case other => other.getClass.getSimpleName.stripPrefix("Js").stripSuffix("$").toLowerCase
  }
}
