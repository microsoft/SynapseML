// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.io.http.{HTTPOutputParser, JSONOutputParser}
import com.microsoft.azure.synapse.ml.param.AnyJsonFormat.anyFormat
import com.microsoft.azure.synapse.ml.param.ServiceParam
import com.microsoft.azure.synapse.ml.services.{HasCognitiveServiceInput, HasInternalJsonOutputParser}
import org.apache.http.entity.{AbstractHttpEntity, ContentType, StringEntity}
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, functions => F}
import org.apache.spark.sql.types._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.JavaConverters._
import scala.language.existentials
import scala.util.control.NonFatal
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

trait HasOpenAITextParamsResponses extends HasOpenAITextParams
  with HasOpenAIToolParams with HasOpenAIResponsesModernParams {
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
  ) ++ toolPayloadParams ++ modernResponsesParams
}

object OpenAIResponses extends ComplexParamsReadable[OpenAIResponses]

class OpenAIResponses(override val uid: String) extends OpenAIServicesBase(uid)
  with HasOpenAITextParamsResponses with HasMessagesInput with HasResponsesInputParams
  with HasCognitiveServiceInput with HasOpenAIFabricHeaders
  with HasInternalJsonOutputParser with SynapseMLLogging with HasCustomHeaders
  with HasRAIContentFilter with HasTextOutput with HasOpenAIToolCallOutput {
  logClass(FeatureNames.AiServices.OpenAI)

  def this() = this(Identifiable.randomUID("OpenAIResponses"))

  private[openai] def generatedPythonClass: String = pythonClass()

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
      val messages = r.getAs[scala.collection.Seq[Row]](getMessagesCol).toSeq
      Some(getStringEntity(messages, optionalParams))
  }

  override private[ml] def getOptionalParams(r: Row): Map[String, Any] = {
    val base = super.getOptionalParams(r) - "reasoning_effort"
    val withTokens = resolveMaxTokens(base, "max_output_tokens")
    val withModel = mergeModel(withTokens, r)
    val withText = mergeTextVerbosity(withModel, r)
    val withReasoning = mergeReasoningExtras(mergeReasoning(withText, r), r)
    val withTools = mergeToolPayload(dropSamplingForGpt5(withReasoning), r)
    mergeContinuationInput(withTools, r)
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

  private def dynamicField[T](
      row: Row,
      isConfigured: Boolean,
      colName: => String): Option[T] = {
    if (!isConfigured) {
      None
    } else {
      val name = colName
      if (row.schema.fieldNames.contains(name)) {
        Option(row.getAs[T](name))
      } else {
        None
      }
    }
  }

  private def mergeContinuationInput(
      params: Map[String, Any],
      row: Row): Map[String, Any] = {
    val inputItems = dynamicField[String](
      row,
      hasInputItemsCol,
      getInputItemsCol
    ).toSeq.flatMap(OpenAIToolUtils.parseInputItems)
    val functionOutputs = dynamicField[scala.collection.Seq[Row]](
      row,
      hasFunctionCallOutputsCol,
      getFunctionCallOutputsCol
    ).map(_.toSeq).map(OpenAIToolColumns.toFunctionCallOutputs).getOrElse(Vector.empty)
    val continuationInput: Seq[Any] = inputItems ++ functionOutputs
    if (continuationInput.nonEmpty) {
      params.updated("input", continuationInput)
    } else {
      params
    }
  }

  override def shouldSkip(row: Row): Boolean = {
    val noMessages = dynamicField[scala.collection.Seq[Row]](
      row,
      isSet(messagesCol),
      getMessagesCol
    ).forall(_.isEmpty)
    val noInputItems = dynamicField[String](
      row,
      hasInputItemsCol,
      getInputItemsCol
    ).forall(_.trim.isEmpty)
    val noFunctionOutputs = dynamicField[scala.collection.Seq[Row]](
      row,
      hasFunctionCallOutputsCol,
      getFunctionCallOutputsCol
    ).forall(_.isEmpty)
    super.shouldSkip(row) || (noMessages && noInputItems && noFunctionOutputs)
  }

  override protected def getVectorParamMap: Map[String, String] = {
    var paramMap = super.getVectorParamMap
    if (isSet(messagesCol)) {
      paramMap = paramMap.updated("input", getMessagesCol)
    }
    if (hasInputItemsCol) {
      paramMap = paramMap.updated("input_items", getInputItemsCol)
    }
    if (hasFunctionCallOutputsCol) {
      paramMap = paramMap.updated("function_call_outputs", getFunctionCallOutputsCol)
    }
    paramMap
  }

  override def responseDataType: DataType = ResponsesModelResponse.schema

  override protected def getInternalOutputParser(schema: StructType): HTTPOutputParser =
    new JSONOutputParser().setDataType(ResponsesModelResponseV2.schema)

  private[openai] def getStringEntity(
      messages: Seq[Row],
      optionalParams: Map[String, Any]): StringEntity = {
    val continuationInput = optionalParams.get("input").collect {
      case input: scala.collection.Seq[_] => input.toSeq
    }.getOrElse(Seq.empty)
    buildStringEntity(messages, continuationInput, optionalParams - "input")
  }

  private[openai] def getStringEntity(
      messages: Seq[Row],
      inputItemsJson: String,
      functionOutputs: Seq[Row],
      optionalParams: Map[String, Any]): StringEntity = {
    val continuationInput: Seq[Any] =
      OpenAIToolUtils.parseInputItems(inputItemsJson) ++
        OpenAIToolColumns.toFunctionCallOutputs(functionOutputs)
    buildStringEntity(messages, continuationInput, optionalParams - "input")
  }

  private def buildStringEntity(
      messages: Seq[Row],
      continuationInput: Seq[Any],
      optionalParams: Map[String, Any]): StringEntity = {
    val mappedMessages = Option(messages).map(encodeMessagesToMap).getOrElse(Seq.empty)
      .map(_.filter { case (_, value) => value != null })
      .map(wrapContentParts)
    val input: Seq[Any] = mappedMessages ++ continuationInput
    val fullPayload = optionalParams.updated("input", input)
    new StringEntity(fullPayload.toJson.compactPrint, ContentType.APPLICATION_JSON)
  }

  private def wrapContentParts(message: Map[String, Any]): Map[String, Any] = {
    message.get("content") match {
      case Some(value: String) =>
        val partType =
          if (message.get("role").contains("assistant")) "output_text" else "input_text"
        message.updated("content", Seq(Map("type" -> partType, "text" -> value)))
      case _ => message
    }
  }

  override private[openai] def getOutputMessageText(outputColName: String): Column = {
    val items = F.col(outputColName).getField("output")
    val typedItems = F.filter(items, item => item.getField("type").isNotNull)
    val typedMessages = F.filter(
      typedItems,
      item => item.getField("type") === OpenAIToolUtils.MessageItemType)
    val legacyMessages = F.filter(items, item => item.getField("type").isNull)
    val messages = F.when(F.size(typedItems) > 0, typedMessages).otherwise(legacyMessages)
    val textValues = F.transform(
      F.element_at(messages, -1).getField("content"),
      part => part.getField("text"))
    val definedTextValues = F.filter(textValues, text => text.isNotNull)
    F.when(
      F.size(messages) > 0,
      F.element_at(definedTextValues, 1)
    )
  }

  private def outputEntries(outputRow: Row): Seq[Row] = {
    Option(outputRow)
      .flatMap(r => Option(r.getAs[scala.collection.Seq[Row]]("output")))
      .getOrElse(Seq.empty).toSeq
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
      firstDefinedText(
        Option(outputEntry.getAs[scala.collection.Seq[Row]]("content"))
          .getOrElse(Seq.empty).toSeq)
    }
  }

  private def hasFunctionCall(outputRow: Row): Boolean =
    outputEntries(outputRow).exists(itemType(_).contains(OpenAIToolUtils.FunctionCallItemType))

  private def incompleteReason(outputRow: Row): Option[String] =
    optionalField[Row](outputRow, "incomplete_details")
      .flatMap(optionalField[String](_, "reason"))

  override private[openai] def isContentFiltered(outputRow: Row): Boolean = {
    incompleteReason(outputRow) match {
      case Some(OpenAIToolUtils.ContentFilterReason) => true
      case Some(OpenAIToolUtils.MaxOutputTokensReason) => false
      case _ if hasFunctionCall(outputRow) => false
      case _ => lastMessageText(outputRow).isEmpty
    }
  }

  override private[openai] def getFilterReason(outputRow: Row): String = {
    if (incompleteReason(outputRow).contains(OpenAIToolUtils.ContentFilterReason)) {
      OpenAIToolUtils.ContentFilterReason
    } else {
      lastMessageEntry(outputRow)
        .flatMap(optionalField[String](_, "status"))
        .filter(_.nonEmpty)
        .getOrElse("content_filtered_or_empty")
    }
  }

  private def optionalField[T](row: Row, fieldName: String): Option[T] = {
    Option(row).filter(_.schema.fieldNames.contains(fieldName))
      .flatMap(value => Option(value.getAs[T](fieldName)))
  }

  private def itemType(row: Row): Option[String] =
    optionalField[String](row, "type")

  private def messageEntries(outputRow: Row): Seq[Row] = {
    val entries = outputEntries(outputRow)
    val typedEntries = entries.filter(itemType(_).isDefined)
    if (typedEntries.nonEmpty) {
      typedEntries.filter(itemType(_).contains(OpenAIToolUtils.MessageItemType))
    } else {
      entries.filter(itemType(_).isEmpty)
    }
  }

  private def lastMessageEntry(outputRow: Row): Option[Row] = {
    messageEntries(outputRow).lastOption
  }

  private def lastMessageText(outputRow: Row): Option[String] = {
    lastMessageEntry(outputRow).flatMap { outputEntry =>
      firstDefinedText(
        optionalField[scala.collection.Seq[Row]](outputEntry, "content")
          .getOrElse(Seq.empty)
          .toSeq)
    }
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    validateToolSetup()
    validateResponsesSetup(dataset.schema)
    val result = transformWithRowValidation(dataset)
    if (isSet(toolCallsCol)) {
      result.withColumn(getToolCallsCol, toolCallsColumn(getOutputCol))
    } else {
      result
    }
  }

  private def transformPrepared(dataset: Dataset[_]): DataFrame =
    super.transform(dataset)

  private def vectorStringColumn(param: ServiceParam[String]): Option[String] =
    get(param).orElse(getDefault(param)).flatMap(_.right.toOption)

  private def hasRowValidation: Boolean =
    vectorStringColumn(tools).isDefined ||
      vectorStringColumn(toolChoice).isDefined ||
      hasInputItemsCol ||
      hasFunctionCallOutputsCol

  private def transformWithRowValidation(dataset: Dataset[_]): DataFrame = {
    if (!hasRowValidation) {
      transformPrepared(dataset)
    } else {
      val errorInputCol = DatasetExtensions.findUnusedColumnName(
        "openaiRowValidationError",
        dataset.schema)
      var prepared = dataset.toDF.withColumn(errorInputCol, rowValidationErrorColumn)
      var temporaryColumns = Seq(errorInputCol)
      val worker = copy(ParamMap.empty).asInstanceOf[OpenAIResponses]

      def shadowInput(configured: Boolean, originalName: => String, setWorker: String => Unit): Unit = {
        if (configured) {
          val name = originalName
          val shadow = DatasetExtensions.findUnusedColumnName(s"${name}_validated", prepared.schema)
          val dataType = prepared.schema(name).dataType
          prepared = prepared.withColumn(
            shadow,
            F.when(F.col(errorInputCol).isNull, F.col(name))
              .otherwise(F.lit(null).cast(dataType))) //scalastyle:ignore null
          temporaryColumns :+= shadow
          setWorker(shadow)
        }
      }

      shadowInput(isSet(messagesCol), getMessagesCol, worker.setMessagesCol)
      if (!isSet(messagesCol)) {
        val emptyMessagesCol = DatasetExtensions.findUnusedColumnName(
          "openaiEmptyMessages",
          prepared.schema)
        prepared = prepared.withColumn(
          emptyMessagesCol,
          F.typedLit(Seq.empty[OpenAIMessage]))
        temporaryColumns :+= emptyMessagesCol
        worker.setMessagesCol(emptyMessagesCol)
      }
      shadowInput(hasInputItemsCol, getInputItemsCol, worker.setInputItemsCol)
      shadowInput(
        hasFunctionCallOutputsCol,
        getFunctionCallOutputsCol,
        worker.setFunctionCallOutputsCol)

      val transformed = worker.transformPrepared(prepared)
      val errorType = transformed.schema(getErrorCol).dataType.asInstanceOf[StructType]
      val validationError = F.when(
        F.col(errorInputCol).isNotNull,
        F.struct(
          F.col(errorInputCol).as("response"),
          F.lit(null).cast(errorType("status").dataType).as("status") //scalastyle:ignore null
        ))
      transformed
        .withColumn(getErrorCol, F.coalesce(F.col(getErrorCol), validationError))
        .drop(temporaryColumns: _*)
    }
  }

  private def rowValidationErrorColumn: Column = {
    val scalarTools = get(tools).flatMap(_.left.toOption).map(OpenAIToolUtils.parseTools)
    val scalarChoice = get(toolChoice).flatMap(_.left.toOption)
      .flatMap(OpenAIToolUtils.parseToolChoice)
    val toolsJson = vectorStringColumn(tools).map(F.col)
      .getOrElse(F.lit(null).cast(StringType)) //scalastyle:ignore null
    val choiceJson = vectorStringColumn(toolChoice).map(F.col)
      .getOrElse(F.lit(null).cast(StringType)) //scalastyle:ignore null
    val inputItems = if (hasInputItemsCol) {
      F.col(getInputItemsCol)
    } else {
      F.lit(null).cast(StringType) //scalastyle:ignore null
    }
    val outputs = if (hasFunctionCallOutputsCol) {
      F.col(getFunctionCallOutputsCol)
    } else {
      F.lit(null).cast(OpenAIToolColumns.FunctionCallOutputStructType) //scalastyle:ignore null
    }
    val validate = F.udf(
      (toolsValue: String, choiceValue: String, itemsValue: String, outputRows: Seq[Row]) => {
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
          OpenAIToolUtils.parseInputItems(itemsValue)
          OpenAIToolColumns.toFunctionCallOutputs(outputRows)
          null //scalastyle:ignore null
        } catch {
          case NonFatal(error) =>
            Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
        }
      })
    validate(toolsJson, choiceJson, inputItems, outputs)
  }

  override def transformSchema(schema: StructType): StructType = {
    validateToolSetup()
    validateResponsesSetup(schema)
    val baseSchema = super.transformSchema(schema)
    if (isSet(toolCallsCol)) {
      baseSchema.add(getToolCallsCol, OpenAIToolColumns.ToolCallStructType)
    } else {
      baseSchema
    }
  }

  private def validateResponsesSetup(schema: StructType): Unit = {
    require(
      isSet(messagesCol) || hasInputItemsCol || hasFunctionCallOutputsCol,
      "Set at least one of messagesCol, inputItemsCol, functionCallOutputsCol")
    require(
      !(isSet(conversation) && isSet(previousResponseId)),
      "conversation and previousResponseId are mutually exclusive")
    validateDistinctInputColumns()
    val hasStoredContext = isSet(previousResponseId) || isSet(conversation)
    validateContinuation(schema, hasStoredContext)
    warnOnContinuationConfiguration()
    get(toolCallsCol).foreach { columnName =>
      require(
        !schema.fieldNames.contains(columnName),
        s"Column '$columnName' already exists in the input DataFrame")
    }
    warnOnAzureFieldSupport()
  }

  private def validateDistinctInputColumns(): Unit = {
    val inputColumns = Seq(
      if (isSet(messagesCol)) Some(getMessagesCol) else None,
      if (hasInputItemsCol) Some(getInputItemsCol) else None,
      if (hasFunctionCallOutputsCol) Some(getFunctionCallOutputsCol) else None
    ).flatten
    inputColumns.groupBy(identity).collect {
      case (columnName, occurrences) if occurrences.size > 1 => columnName
    }.foreach { columnName =>
      throw new IllegalArgumentException(
        "messagesCol, inputItemsCol and functionCallOutputsCol must reference different " +
          s"columns; '$columnName' is used twice")
    }
  }

  private def validateContinuation(schema: StructType, hasStoredContext: Boolean): Unit = {
    if (hasFunctionCallOutputsCol) {
      require(
        hasStoredContext || hasInputItemsCol,
        "functionCallOutputsCol requires previousResponseIdCol, conversation, or inputItemsCol")
      require(
        isSet(tools),
        "Tools are not carried across turns; resend tools on the continuation transform")
      requireFieldShape(
        schema,
        getFunctionCallOutputsCol,
        OpenAIToolColumns.FunctionCallOutputStructType)

      if (hasInputItemsCol && !hasStoredContext) {
        require(
          isSet(messagesCol),
          "stateless replay must resend the originating user message: set messagesCol " +
            "(or use previousResponseIdCol/conversation instead)")
        if (get(store).flatMap(_.left.toOption).contains(true)) {
          logWarning(
            "stateless replay with store=true drops reasoning items without encrypted_content; " +
              "use store=false or previousResponseIdCol")
        }
      }
    }
  }

  private def warnOnContinuationConfiguration(): Unit = {
    if (isSet(previousResponseId) && get(store).flatMap(_.left.toOption).contains(false)) {
      logWarning(
        "previous_response_id requires the turn that produced it to have been sent with " +
          "store=true; this stage has store=false")
    }
    if (isSet(conversation)) {
      logWarning(
        "conversation is server-side mutable state; concurrent Spark partitions appending " +
          "to one conversation are non-deterministic")
    }
  }

  private def requireFieldShape(
      schema: StructType,
      columnName: String,
      expectedType: DataType): Unit = {
    val actualType = schema.find(_.name == columnName).map(_.dataType).getOrElse {
      throw new IllegalArgumentException(
        s"Column '$columnName' was not found in the input DataFrame")
    }
    require(
      DataType.equalsStructurally(actualType, expectedType, ignoreNullability = true),
      s"$columnName must have type ${expectedType.simpleString}; got ${actualType.simpleString}")
  }

  private def warnOnAzureFieldSupport(): Unit = {
    val configuredUrl = get(url).orElse(getDefault(url)).map(_.toLowerCase)
    val isAzure = configuredUrl.exists(_.contains("azure.com"))
    if (isAzure && isSet(serviceTier)) {
      logWarning(
        "serviceTier is not supported by Azure OpenAI and will likely return HTTP 400")
    }
    if (isAzure && !isOpenAIV1BaseUrl) {
      val fields = Seq(
        maxToolCalls -> "max_tool_calls",
        include -> "include",
        topLogprobs -> "top_logprobs",
        safetyIdentifier -> "safety_identifier",
        promptCacheKey -> "prompt_cache_key",
        conversation -> "conversation",
        reasoningSummary -> "reasoning.summary",
        reasoningContext -> "reasoning.context",
        reasoningMode -> "reasoning.mode"
      ).collect { case (param, wireName) if isSet(param) => wireName }
      if (fields.nonEmpty) {
        logWarning(
          s"${fields.mkString(", ")} are documented only on the Azure /openai/v1 surface; " +
            "this dated api-version endpoint may reject them with HTTP 400")
      }
    }
  }

  override def pyAdditionalMethods: String =
    super.pyAdditionalMethods + OpenAIToolPythonOverrides.ResponsesMethods
}
