// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.param.ServiceParam
import com.microsoft.azure.synapse.ml.services.HasServiceParams
import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, functions => F}
import spray.json.DefaultJsonProtocol._
import spray.json._

private[openai] object OpenAIPromptMixins {
  trait ToolEnabledServiceDomain
    extends com.microsoft.azure.synapse.ml.services.HasCustomCogServiceDomain with HasOpenAIPromptToolOutput
}

/** Tool and function-calling configuration shared by Responses and OpenAIPrompt.
  *
  * Tool definitions are trusted configuration. In particular, a tools column must not be
  * populated from untrusted row data because hosted and MCP tools can cause provider-side
  * execution, network access, and cost. SynapseML transports tool declarations and model
  * calls but never executes user functions or resolves calls automatically.
  *
  * Spark HTTP execution is at-least-once. Materialize paid turns before branching, disable
  * speculation, deduplicate by response id, and make external side effects idempotent by call id.
  */
trait HasOpenAIToolParams extends HasServiceParams {
  val tools: ServiceParam[String] = new ServiceParam[String](
    this,
    "tools",
    "Trusted Responses tool definitions as a JSON array. Function tools may use the flat " +
      "Responses shape or the nested Chat Completions shape; hosted and future types pass through.",
    isRequired = false) {
    override val payloadName: String = "tools"
  }

  val toolChoice: ServiceParam[String] = new ServiceParam[String](
    this,
    "toolChoice",
    "How the model selects tools: a free-form string or a JSON object with a type field.",
    isRequired = false) {
    override val payloadName: String = "tool_choice"
  }

  val parallelToolCalls: ServiceParam[Boolean] = new ServiceParam[Boolean](
    this,
    "parallelToolCalls",
    "Whether the model may emit multiple tool calls in one turn. The API default is true.",
    isRequired = false) {
    override val payloadName: String = "parallel_tool_calls"
  }

  val maxToolCalls: ServiceParam[Int] = new ServiceParam[Int](
    this,
    "maxToolCalls",
    "Maximum number of built-in or hosted tool calls. This does not cap function calls.",
    isRequired = false) {
    override val payloadName: String = "max_tool_calls"
  }

  def getTools: String = getScalarParam(tools)

  private[openai] def getToolsParamMode: String =
    get(tools).orElse(getDefault(tools)) match {
      case Some(Left(_)) => "scalar"
      case Some(Right(_)) => "column"
      case None => "unset"
    }

  def setTools(value: String): this.type = {
    require(
      Option(value).exists(_.trim.nonEmpty),
      "tools must be a non-empty JSON array")
    setScalarParam(tools, OpenAIToolUtils.parseTools(value).compactPrint)
  }

  def setTools(value: Seq[Map[String, Any]]): this.type =
    setScalarParam(tools, OpenAIToolUtils.toolsToJson(value))

  def setTools(value: java.util.List[java.util.Map[String, Object]]): this.type =
    setScalarParam(tools, OpenAIToolUtils.toolsToJson(value))

  def getToolsCol: String = getVectorParam(tools)

  def setToolsCol(value: String): this.type = setVectorParam(tools, value)

  def addFunctionTool(
      name: String,
      description: String,
      parameters: Map[String, Any],
      strict: Boolean = true): this.type = {
    if (getToolsParamMode == "column") {
      throw new IllegalArgumentException(
        s"addFunctionTool requires scalar tools; tools is column-bound to '$getToolsCol'")
    }
    val existing = get(tools).flatMap(_.left.toOption)
      .map(OpenAIToolUtils.parseTools(_).elements)
      .getOrElse(Vector.empty)
    val added = OpenAIToolUtils.toJsValue(
      OpenAIToolUtils.functionTool(name, description, parameters, strict))
    setTools(JsArray(existing :+ added).compactPrint)
  }

  def clearTools(): this.type = {
    clear(tools)
    this
  }

  def getToolChoice: String = getScalarParam(toolChoice)

  def setToolChoice(value: String): this.type = {
    require(
      OpenAIToolUtils.parseToolChoice(value).isDefined,
      "toolChoice must be a non-empty string or a JSON object")
    setScalarParam(toolChoice, value.trim)
  }

  def setToolChoice(value: Map[String, Any]): this.type =
    setToolChoice(OpenAIToolUtils.toJsValue(value).compactPrint)

  def setToolChoice(value: java.util.Map[String, Object]): this.type =
    setToolChoice(OpenAIToolUtils.toJsValue(value).compactPrint)

  def setToolChoiceFunction(name: String): this.type =
    setToolChoice(JsObject(
      "type" -> JsString("function"),
      "name" -> JsString(name)
    ).compactPrint)

  def getToolChoiceCol: String = getVectorParam(toolChoice)

  def setToolChoiceCol(value: String): this.type = setVectorParam(toolChoice, value)

  def getParallelToolCalls: Boolean = getScalarParam(parallelToolCalls)

  def setParallelToolCalls(value: Boolean): this.type =
    setScalarParam(parallelToolCalls, value)

  def getParallelToolCallsCol: String = getVectorParam(parallelToolCalls)

  def setParallelToolCallsCol(value: String): this.type =
    setVectorParam(parallelToolCalls, value)

  def getMaxToolCalls: Int = getScalarParam(maxToolCalls)

  def setMaxToolCalls(value: Int): this.type = {
    require(value > 0, "maxToolCalls must be a positive integer")
    setScalarParam(maxToolCalls, value)
  }

  def getMaxToolCallsCol: String = getVectorParam(maxToolCalls)

  def setMaxToolCallsCol(value: String): this.type =
    setVectorParam(maxToolCalls, value)

  private[openai] def toolPayloadParams: Seq[ServiceParam[_]] =
    Seq(parallelToolCalls, maxToolCalls)

  private[openai] def toolParamNames: Seq[String] =
    Seq(tools.name, toolChoice.name, parallelToolCalls.name, maxToolCalls.name)

  @transient @volatile private var toolsMemo: (String, JsArray) = _

  private[openai] def mergeToolPayload(
      params: Map[String, Any],
      row: Row): Map[String, Any] = {
    val withTools = getValueOpt(row, tools).map(_.trim).filter(_.nonEmpty) match {
      case Some(text) => params.updated("tools", memoTools(text))
      case None => params - "tools"
    }
    getValueOpt(row, toolChoice).flatMap(OpenAIToolUtils.parseToolChoice) match {
      case Some(choice) => withTools.updated("tool_choice", choice)
      case None => withTools - "tool_choice"
    }
  }

  private def memoTools(text: String): JsArray = {
    val cached = toolsMemo
    if (cached != null && cached._1 == text) { //scalastyle:ignore null
      cached._2
    } else {
      val parsed = OpenAIToolUtils.parseTools(text)
      toolsMemo = text -> parsed
      parsed
    }
  }

  private[openai] def validateResolvedToolSetup(
      toolsOpt: Option[JsArray],
      choiceOpt: Option[JsValue],
      toolsConfigured: Boolean): Unit = {
    choiceOpt match {
      case Some(choice) if choice != JsString("none") && !toolsConfigured =>
        throw new IllegalArgumentException("toolChoice requires tools to be set")
      case Some(choice) =>
        toolsOpt.foreach(OpenAIToolUtils.validateToolChoiceAgainst(_, choice))
      case None =>
    }
  }

  private[openai] def validateToolSetup(): Unit = {
    val scalarTools = get(tools).flatMap(_.left.toOption).map(OpenAIToolUtils.parseTools)
    val scalarChoice = get(toolChoice).flatMap(_.left.toOption)
      .flatMap(OpenAIToolUtils.parseToolChoice)
    validateResolvedToolSetup(
      scalarTools,
      scalarChoice,
      get(tools).orElse(getDefault(tools)).isDefined)
  }
}

trait HasOpenAIToolCallOutput extends Params {
  val toolCallsCol: Param[String] = new Param[String](
    this,
    "toolCallsCol",
    "Optional ARRAY<STRUCT<call_id,item_id,type,name,arguments,status,index>> output column.")

  def getToolCallsCol: String = $(toolCallsCol)

  def setToolCallsCol(value: String): this.type = set(toolCallsCol, value)

  def toolCallsColumn(structColName: String): Column =
    OpenAIToolColumns.toolCallsColumn(structColName)

  def replayItemsColumn(structColName: String): Column =
    OpenAIToolColumns.replayItemsColumn(structColName)
}

trait HasOpenAIPromptToolOutput extends HasOpenAIToolParams
  with HasOpenAIResponsesModernParams with HasOpenAIToolCallOutput {
  val responseStructCol: Param[String] = new Param[String](
    this,
    "responseStructCol",
    "Optional column retaining the parsed service response struct, not the raw HTTP body.")

  def getResponseStructCol: String = $(responseStructCol)

  def setResponseStructCol(value: String): this.type = set(responseStructCol, value)

  private[openai] def promptResponsesOnlyParamNames: Seq[String] =
    toolParamNames ++ modernParamNames ++ Seq("toolCallsCol", "responseStructCol")

  private[openai] def addPromptToolColumns(
      result: DataFrame,
      serviceOutputCol: String): DataFrame = {
    val withCalls =
      if (isSet(toolCallsCol)) {
        result.withColumn(getToolCallsCol, OpenAIToolColumns.toolCallsColumn(serviceOutputCol))
      } else {
        result
      }
    val withResponse =
      if (isSet(responseStructCol)) {
        withCalls.withColumn(getResponseStructCol, F.col(serviceOutputCol))
      } else {
        withCalls
      }
    withResponse.drop(serviceOutputCol)
  }

  private[openai] def addPromptToolSchema(
      schema: StructType,
      responseField: Option[StructField]): StructType = {
    val withCalls =
      if (isSet(toolCallsCol)) {
        schema.add(getToolCallsCol, OpenAIToolColumns.ToolCallStructType)
      } else {
        schema
      }
    if (isSet(responseStructCol)) {
      responseField.map(field => withCalls.add(getResponseStructCol, field.dataType))
        .getOrElse(withCalls)
    } else {
      withCalls
    }
  }

  private[openai] def validatePromptToolOutputColumns(schema: StructType): Unit = {
    val requested = Seq(get(toolCallsCol), get(responseStructCol)).flatten
    requested.foreach { columnName =>
      require(
        !schema.fieldNames.contains(columnName),
        s"Column '$columnName' already exists in the input DataFrame")
    }
    require(
      requested.distinct.size == requested.size,
      "toolCallsCol and responseStructCol must reference different columns")
  }
}
