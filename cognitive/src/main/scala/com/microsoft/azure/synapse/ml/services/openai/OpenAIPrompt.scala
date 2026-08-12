// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.contracts.HasOutputCol
import com.microsoft.azure.synapse.ml.core.spark.Functions
import com.microsoft.azure.synapse.ml.io.http.{ConcurrencyParams, ErrorUtils, HasErrorCol, HasURL}
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.{GlobalParams, HasGlobalParams, ServiceParam, StringStringMapParam}
import com.microsoft.azure.synapse.ml.services._
import com.microsoft.azure.synapse.ml.services.aifoundry.{AIFoundryChatCompletion, HasAIFoundryTextParamsExtended}
import org.apache.hadoop.conf.Configuration
import org.apache.http.entity.AbstractHttpEntity
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.ml.param.{BooleanParam, DoubleParam, Param, ParamMap, ParamValidators}
import org.apache.spark.ml.util.{Identifiable, MLWriter}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoders, Row, functions => F, types => T}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, typedLit, udf}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import spray.json.DefaultJsonProtocol._

import java.net.{URI, URL}

import scala.collection.JavaConverters._
import scala.util.{Try, Using}

object OpenAIPrompt extends ComplexParamsReadable[OpenAIPrompt]
// scalastyle:off number.of.methods
class OpenAIPrompt(override val uid: String) extends Transformer
  with HasAIFoundryTextParamsExtended
  with HasOpenAITextParamsExtended with HasMessagesInput
  with HasOpenAIPromptToolOutput
  with HasErrorCol with HasOutputCol
  with HasURL with HasCustomCogServiceDomain with ConcurrencyParams
  with HasSubscriptionKey with HasAADToken with HasCustomAuthHeader
  with HasCognitiveServiceInput
  with ComplexParamsWritable with SynapseMLLogging with HasGlobalParams {

  logClass(FeatureNames.AiServices.OpenAI)
  def this() = this(Identifiable.randomUID("OpenAIPrompt"))
  private[openai] def generatedPythonClass: String = pythonClass()
  override def copy(extra: ParamMap): Transformer = {
    val copied = defaultCopy(extra).asInstanceOf[OpenAIPrompt]
    copied.postProcessingExplicitlySet =
      postProcessingExplicitlySet || extra.contains(postProcessing)
    if (extra.contains(postProcessingOptions)) {
      copied.setPostProcessingOptions(copied.getPostProcessingOptions)
    } else if (extra.contains(postProcessing)) {
      OpenAIPromptPostProcessing.inferMode(copied.getPostProcessingOptions)
        .foreach { expectedMode =>
          OpenAIPromptPostProcessing.validateModeValue(copied.getPostProcessing, expectedMode)
        }
    }
    copied
  }
  override def write: MLWriter = {
    val delegate = super.write
    new MLWriter {
      override def save(path: String): Unit = {
        OpenAIPrompt.this.getEffectivePostProcessing
        super.save(path)
      }

      override protected def saveImpl(path: String): Unit = {
        delegate.session(sparkSession)
        optionMap.foreach { case (key, value) => delegate.option(key, value) }
        if (shouldOverwrite) {
          delegate.overwrite()
        }
        delegate.save(path)
      }
    }
  }
  def urlPath: String = ""
  override private[ml] def internalServiceType: String = "openai"
  import UsageUtils.{UsageFieldMapping, UsageMappings, UsageStructType}

  val usageCol: Param[String] = new Param[String](
    this, "usageCol",
    "Column to hold usage statistics. Set this parameter to enable usage tracking.")
  def getUsageCol: String = $(usageCol)
  def setUsageCol(value: String): this.type = set(usageCol, value)
  val responseIdCol: Param[String] = new Param[String](
    this, "responseIdCol",
    "Column to hold response ID when store=true. Auto-generated if not explicitly set.")
  setDefault(responseIdCol -> s"${uid}_responseId")
  def getResponseIdCol: String = $(responseIdCol)
  def setResponseIdCol(value: String): this.type = set(responseIdCol, value)
  val promptTemplate = new Param[String](
    this, "promptTemplate", "The prompt. supports string interpolation {col1}: {col2}.")
  def getPromptTemplate: String = $(promptTemplate)

  def setPromptTemplate(value: String): this.type = set(promptTemplate, value)

  val postProcessing = new Param[String](
    this, "postProcessing", "Post processing options: csv, json, regex",
    isValid = ParamValidators.inArray(Array("", "csv", "json", "regex")))

  def getPostProcessing: String = $(postProcessing)
  private var postProcessingExplicitlySet: Boolean = false
  def setPostProcessing(value: String): this.type = {
    OpenAIPromptPostProcessing.inferMode(getPostProcessingOptions)
      .foreach(expectedMode => OpenAIPromptPostProcessing.validateModeValue(value, expectedMode))
    val result = set(postProcessing, value)
    postProcessingExplicitlySet = true
    result
  }

  val postProcessingOptions = new StringStringMapParam(
    this, "postProcessingOptions", "Options (default): delimiter=',', jsonSchema, regex, regexGroup=0")
  def getPostProcessingOptions: Map[String, String] = $(postProcessingOptions)
  def setPostProcessingOptions(value: Map[String, String]): this.type = {
    def setOrValidatePostProcessing(expected: String): Unit = {
      if (isSet(postProcessing)) {
        if (getPostProcessing.isEmpty && !postProcessingExplicitlySet) {
          set(postProcessing, expected)
        } else {
          OpenAIPromptPostProcessing.validateModeValue(getPostProcessing, expected)
        }
      } else {
        set(postProcessing, expected)
        postProcessingExplicitlySet = false
      }
    }

    val inferredMode = OpenAIPromptPostProcessing.inferMode(value)
    inferredMode.foreach(setOrValidatePostProcessing)
    OpenAIPromptPostProcessing.validateModeOptions(
      inferredMode.getOrElse(getPostProcessing),
      value
    )
    set(postProcessingOptions, value)
  }

  def setPostProcessingOptions(v: java.util.HashMap[String, String]): this.type =
    setPostProcessingOptions(v.asScala.toMap)

  override protected def pyParamSetter(p: Param[_]): String = {
    if (p.name == postProcessingOptions.name) {
      OpenAIPromptPythonOverrides.postProcessingOptionsSetter(super.pyParamSetter(p))
    } else if (p.name == postProcessing.name) {
      OpenAIPromptPythonOverrides.postProcessingSetter(super.pyParamSetter(p))
    } else {
      super.pyParamSetter(p)
    }
  }

  override protected def pySetParamsFunc: String =
    OpenAIPromptPythonOverrides.setParamsFunc(super.pySetParamsFunc)

  override def pyAdditionalMethods: String = super.pyAdditionalMethods +
    OpenAIPromptPythonOverrides.AdditionalMethods + OpenAIToolPythonOverrides.PromptMethods

  override def pyInitFunc(): String =
    OpenAIPromptPythonOverrides.initFunc(super.pyInitFunc())

  val dropPrompt = new BooleanParam(
    this, "dropPrompt", "whether to drop the column of prompts after templating (when using legacy models)")

  def getDropPrompt: Boolean = $(dropPrompt)

  def setDropPrompt(value: Boolean): this.type = set(dropPrompt, value)

  val systemPrompt = new Param[String](
    this, "systemPrompt", "The initial system prompt to be used.")

  def getSystemPrompt: String = $(systemPrompt)

  def setSystemPrompt(value: String): this.type = set(systemPrompt, value)

  val apiType = new Param[String](
    this, "apiType", "The OpenAI API type to use: 'chat_completions' or 'responses'",
    isValid = ParamValidators.inArray(Array("chat_completions", "responses")))

  GlobalParams.registerParam(apiType, OpenAIApiTypeKey)

  def getApiType: String = $(apiType)

  def setApiType(value: String): this.type = set(apiType, value)

  val store: ServiceParam[Boolean] = new ServiceParam[Boolean](
    this,
    "store",
    "Whether to store the generated model response for later retrieval via API. " +
      "Only applicable when using the 'responses' API type.",
    isRequired = false)

  def getStore: Boolean = getScalarParam(store)

  def setStore(v: Boolean): this.type = setScalarParam(store, v)

  val previousResponseId: ServiceParam[String] = new ServiceParam[String](
    this,
    "previousResponseId",
    "The ID of a previous response to use as context for chaining requests. " +
      "Use this for multi-turn conversations or follow-up requests. " +
      "Only applicable when using the 'responses' API type.",
    isRequired = false) {
    override val payloadName: String = "previous_response_id"
  }

  def getPreviousResponseId: String = getScalarParam(previousResponseId)

  def setPreviousResponseId(v: String): this.type = setScalarParam(previousResponseId, v)

  def getPreviousResponseIdCol: String = getVectorParam(previousResponseId)

  def setPreviousResponseIdCol(v: String): this.type = setVectorParam(previousResponseId, v)

  val columnTypes = new StringStringMapParam(
    this, "columnTypes", "A map from column names to their types. Supported types are 'text' and 'path'. " +
      "Path inputs may be filesystem paths, HTTP(S) URLs, or base64 data URLs.")
  private def validateColumnType(value: String) = {
    require(value == "text" || value == "path",
      s"Unsupported column type: $value. Supported types are 'text' and 'path'.")
  }

  def getColumnTypes: Map[String, String] = $(columnTypes)

  def setColumnTypes(value: Map[String, String]): this.type = {
    for ((colName, colType) <- value) {
      validateColumnType(colType)
    }
    set(columnTypes, value)
    this
  }

  def setColumnType(columnName: String, columnType: String): this.type = {
    validateColumnType(columnType)
    val updatedMap = getColumnTypes + (columnName -> columnType)
    set(columnTypes, updatedMap)
    this
  }

  def setColumnTypes(v: java.util.HashMap[String, String]): this.type =
    setColumnTypes(v.asScala.toMap)

  val fileSizeLimitMB: Param[Double] = new DoubleParam(
    this, "fileSizeLimitMB",
    "Maximum file size in megabytes for path columns. Files exceeding this limit will produce an error.")

  def getFileSizeLimitMB: Double = $(fileSizeLimitMB)

  def setFileSizeLimitMB(value: Double): this.type = {
    require(value > 0, "File size limit must be positive")
    set(fileSizeLimitMB, value)
  }

  private val defaultSystemPrompt = "You are an AI chatbot who wants to answer user's questions and complete tasks. " +
    "Follow their instructions carefully and be brief if they don't say otherwise."

  setDefault(
    postProcessing -> "",
    postProcessingOptions -> Map.empty,
    outputCol -> (this.uid + "_output"),
    errorCol -> (this.uid + "_error"),
    messagesCol -> (this.uid + "_messages"),
    dropPrompt -> true,
    systemPrompt -> defaultSystemPrompt,
    apiType -> "chat_completions",
    columnTypes -> Map.empty,
    timeout -> 360.0,
    store -> Left(false)
  )

  override def setUrl(value: String): this.type = set(url, value)

  override def setCustomServiceName(v: String): this.type = {
    setUrl(s"https://$v.openai.azure.com/" + urlPath.stripPrefix("/"))
  }

  def setAIFoundryCustomServiceName(v: String): this.type = {
    setUrl(s"https://$v.services.ai.azure.com/" + urlPath.stripPrefix("/"))
  }

  private val localParamNames = Seq(
    "promptTemplate", "outputCol", "postProcessing", "postProcessingOptions", "dropPrompt", "dropMessages",
    "systemPrompt", "apiType", "usageCol", "responseIdCol", "toolCallsCol", "responseStructCol")

  private val textExtensions = Set("md", "csv", "tsv", "json", "xml")
  private val imageExtensions = Set("jpg", "jpeg", "png", "gif", "webp")
  private val audioExtensions = Set("mp3", "wav")

  private def extractFilename = udf { (path: String) =>
    Option(path).map(_.trim).filter(_.nonEmpty)
      .map(p => Try(OpenAIAttachmentUtils.attachmentFilename(p)).getOrElse("attachment")).orNull
  }

  private def addRAIErrors[T <: OpenAIServicesBase with HasRAIContentFilter](
      completion: T, df: DataFrame, errorCol: String, outputCol: String): DataFrame = {
    df.map({ row =>
      val originalOutput = Option(row.getAs[Row](outputCol))
      val isFiltered = originalOutput.exists(completion.isContentFiltered)

      if (isFiltered) {
        val updatedRowSeq = row.toSeq.updated(
          row.fieldIndex(errorCol),
          Row(completion.getFilterReason(originalOutput.get), null) //scalastyle:ignore null
        )
        Row.fromSeq(updatedRowSeq)
      } else {
        row
      }
    })(ExpressionEncoder(df.schema))
  }

  private def configureService(
    service: OpenAIServicesBase with HasTextOutput,
    df: DataFrame,
    messagesCol: Column
  ): (DataFrame, String, OpenAIServicesBase with HasTextOutput) = {
    val messagesService = service.asInstanceOf[HasMessagesInput]

    if (isSet(responseFormat)) {
      // Pass through responseFormat without forcing a single shape here.
      // Each service validates according to its API (chat_completions vs responses).
      service match {
        case cc: OpenAIChatCompletion => cc.setResponseFormat(getResponseFormat)
        case resp: OpenAIResponses => resp.setResponseFormat(getResponseFormat)
      }
    }
    val messageColName = getMessagesCol

    (
      df.withColumn(messageColName, messagesCol),
      messageColName,
      messagesService.setMessagesCol(messageColName).asInstanceOf[OpenAIServicesBase with HasTextOutput]
    )
  }

  private def usageMappingFor(service: OpenAIServicesBase with HasTextOutput)
      : Option[UsageFieldMapping] = service match {
    case _: OpenAIChatCompletion | _: AIFoundryChatCompletion =>
      Some(UsageMappings.ChatCompletions)
    case _: OpenAIResponses =>
      Some(UsageMappings.Responses)
    case _ =>
      None
  }

  private def generateText(
    service: OpenAIServicesBase with HasTextOutput,
    df: DataFrame
  ): DataFrame = {
    val transformed = service match {
      case c: (HasRAIContentFilter with HasMessagesInput) =>
        addRAIErrors(c, service.transform(df), c.getErrorCol, c.getOutputCol)
      case _ => service.transform(df)
    }

    val serviceOutputCol = service.getOutputCol
    val responseCol = F.col(serviceOutputCol)

    val parsedText = getParser.parse(service.getOutputMessageText(serviceOutputCol))
    var result = transformed.withColumn(getOutputCol, F.when(parsedText.isNotNull, parsedText))

    if (isSet(usageCol)) {
      usageMappingFor(service).foreach { mapping =>
        val usage = UsageUtils.normalize(responseCol.getField("usage"), mapping)
        result = result.withColumn(getUsageCol, F.when(responseCol.isNotNull, usage))
      }
    }

    if (service.isInstanceOf[OpenAIResponses] && getStore) {
      result = result.withColumn(getResponseIdCol, F.when(responseCol.isNotNull, responseCol.getField("id")))
    }
    result = addPromptToolColumns(result, serviceOutputCol)
    result.select(result.columns.filter(_ != getErrorCol).map(col) :+ col(getErrorCol): _*)
  }

  private def processPathColumns(df: DataFrame): (DataFrame, Seq[String], Map[String, String], String) = {
    val columnTypeMap = if (isSet(columnTypes)) getColumnTypes else Map.empty[String, String]

    columnTypeMap.foreach { case (colName, colType) =>
      require(colType == "text" || colType == "path",
        s"Unsupported column type '$colType' for column '$colName'. Supported types are 'text' and 'path'.")
    }

    val pathColumnNames = columnTypeMap.collect {
      case (colName, colType) if colType == "path" => colName
    }.toSeq

    pathColumnNames.foreach { colName =>
      require(
        df.columns.contains(colName),
        s"Column '$colName' specified in columnTypes was not found in the DataFrame. " +
        s"Available columns: ${df.columns.mkString(", ")}"
      )
    }

    val (dfWithFilenames, filenameColMapping) = pathColumnNames.foldLeft((df, Map.empty[String, String])) {
      case ((currentDf, mapping), colName) =>
        val filenameCol = OpenAIColumnUtils.findUnusedColumnName(
          s"${colName}_filename")(currentDf.columns.toSet)
        (currentDf.withColumn(filenameCol, extractFilename(F.col(colName))), mapping + (colName -> filenameCol))
    }

    val templateWithFilenameRefs = filenameColMapping.foldLeft(getPromptTemplate) {
      case (template, (colName, filenameCol)) =>
        template.replace(s"{$colName}", s"{$filenameCol}")
    }

    (dfWithFilenames, pathColumnNames, filenameColMapping, templateWithFilenameRefs)
  }

  private def resolvedApiType: String = if (isSet(apiType)) getApiType else "chat_completions"

  private def hasPreviousResponseIdConfigured: Boolean = {
    get(previousResponseId).orElse(getDefault(previousResponseId)).isDefined
  }

  private def validateResponsesApiCompatibility(currentApiType: String): Unit = {
    if (currentApiType == "responses" && hasAIFoundryModel) {
      throw new IllegalArgumentException(
        "apiType='responses' is not supported with AI Foundry chat endpoints. " +
          "Use .setApiType(\"chat_completions\") or configure an OpenAI endpoint with deploymentName.")
    }
  }

  private def validateResponsesOnlyParams(currentApiType: String): Unit = {
    if (currentApiType != "responses" && isSet(store) && getStore) {
      throw new IllegalArgumentException(
        "store parameter requires apiType='responses'. Use .setApiType(\"responses\")")
    }

    if (currentApiType != "responses" && hasPreviousResponseIdConfigured) {
      throw new IllegalArgumentException(
        "previousResponseId requires apiType='responses'. Use .setApiType(\"responses\")")
    }

    if (currentApiType != "responses") {
      promptResponsesOnlyParamNames.filter(name => isSet(getParam(name))).foreach { name =>
        throw new IllegalArgumentException(
          s"""$name requires apiType='responses'. Use .setApiType("responses")""")
      }
    }
  }

  private def validateUsageColSupport(currentApiType: String): Unit = {
    if (isSet(usageCol) && !hasAIFoundryModel &&
        currentApiType != "chat_completions" && currentApiType != "responses") {
      throw new IllegalArgumentException(
        s"usageCol not supported for apiType='$currentApiType'. " +
          "Use 'chat_completions', 'responses', or AI Foundry chat APIs.")
    }
  }

  private def validateResponsesApiParams(): Unit = {
    val currentApiType = resolvedApiType
    validateResponsesApiCompatibility(currentApiType)
    validateResponsesOnlyParams(currentApiType)
    validateUsageColSupport(currentApiType)
  }
  private def validatePublicColumnNames(): Unit =
    OpenAIColumnUtils.validateDistinctColumns(
      "messagesCol" -> getMessagesCol,
      "outputCol" -> getOutputCol,
      "errorCol" -> getErrorCol
    )

  private def attachmentsColumn(pathColumnNames: Seq[String]): Column = {
    if (pathColumnNames.nonEmpty) {
      val mapEntries = pathColumnNames.flatMap { columnName =>
        Seq(F.lit(columnName), F.col(columnName).cast(T.StringType))
      }
      F.map(mapEntries: _*)
    } else {
      typedLit(Map.empty[String, String])
    }
  }

  private def createMessagesUDF(pathColumnNames: Seq[String]): UserDefinedFunction = {
    udf[(Seq[OpenAICompositeMessage], String), String, Map[String, String]] {
      (userMessage, attachmentMap) =>
        if (userMessage == null) (null, null) //scalastyle:ignore null
        else Try {
          createMessagesForRow(
            userMessage,
            Option(attachmentMap).getOrElse(Map.empty[String, String]),
            pathColumnNames
          )
        } match {
          case scala.util.Success(msgs) => (msgs, null) //scalastyle:ignore null
          case scala.util.Failure(e) => (null, e.getMessage) //scalastyle:ignore null
        }
    }
  }

  private def addPromptMessages(
      df: DataFrame,
      promptCol: Column,
      pathColumnNames: Seq[String]
  ): DataFrame = {
    val fileResultColName = OpenAIColumnUtils.findUnusedColumnName(
      s"${uid}_file_result")(df.columns.toSet)
    val dfWithFileResult = df.withColumn(
      fileResultColName,
      createMessagesUDF(pathColumnNames)(promptCol, attachmentsColumn(pathColumnNames)))
    val fileResultCol = F.col(fileResultColName)
    val fileErrorStruct = toErrorStruct(fileResultCol.getField("_2"))
    val combinedFileError =
      OpenAIColumnUtils.existingColumnOfType(df, getErrorCol, ErrorUtils.ErrorSchema) match {
        case Some(existingErrorCol) => F.coalesce(F.col(existingErrorCol), fileErrorStruct)
        case None => fileErrorStruct
      }
    dfWithFileResult.withColumn(getMessagesCol, fileResultCol.getField("_1"))
      .withColumn(getErrorCol, combinedFileError)
      .drop(fileResultColName)
  }

  private def dropFilenameColumns(df: DataFrame, filenameColMapping: Map[String, String]): DataFrame = {
    filenameColMapping.values.foldLeft(df) { (current, colName) =>
      if (current.columns.contains(colName)) current.drop(colName) else current
    }
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    transferGlobalParamsToParamMap()
    validateResponsesApiParams()
    validatePublicColumnNames()
    validatePromptToolOutputColumns(dataset.schema)
    logTransform[DataFrame]({
      val df = dataset.toDF
      val service = getOpenAIChatService

      val (dfWithFilenames, pathColumnNames, filenameColMapping, templateWithFilenameRefs) =
        processPathColumns(df)

      val promptCol = Functions.template(templateWithFilenameRefs)
      val dfWithFile = addPromptMessages(dfWithFilenames, promptCol, pathColumnNames)

      val (dfTemplated, inputColName, serviceConfigured) =
        configureService(service, dfWithFile, F.col(getMessagesCol))
      val result = generateText(serviceConfigured, dfTemplated)
      val resultCleaned = dropFilenameColumns(result, filenameColMapping)

      if (getDropPrompt) resultCleaned.drop(inputColName) else resultCleaned
    }, dataset.columns.length)
  }

  private def toErrorStruct(errorStr: Column): Column = {
    val statusSchema = ErrorUtils.ErrorSchema("status").dataType
    F.when(errorStr.isNotNull, F.struct(
      errorStr.as("response"),
      F.lit(null).cast(statusSchema).as("status") //scalastyle:ignore null
    ))
  }

  private[openai] def stringMessageWrapper(str: String): Map[String, String] = {
    if (this.getApiType == "responses") {
      Map("type" -> "input_text", "text" -> str)
    } else {
      Map("type" -> "text", "text" -> str)
    }
  }

  private[openai] def getPromptsForMessage(content: Either[Seq[Map[String, String]], String]) = {
    val stringWrapper = (s: String) => Seq(stringMessageWrapper(s))
    Seq(
      OpenAICompositeMessage("system", stringWrapper(getSystemPrompt)),
      OpenAICompositeMessage("user", content match {
        case Left(parts) => parts
        case Right(text) => stringWrapper(text)
      })
    )
  }

  private[openai] def createMessagesForRow(
    userMessage: String,
    attachmentMap: Map[String, String],
    attachmentOrder: Seq[String]
  ): Seq[OpenAICompositeMessage] = {
    // Filter to get only non-null, non-empty path values
    val orderedAttachments = attachmentOrder.flatMap { columnName =>
      attachmentMap.get(columnName).flatMap(v => Option(v).map(_.trim).filter(_.nonEmpty))
    }

    // If there are path columns but all are null/empty, pass through null
    if (attachmentOrder.nonEmpty && orderedAttachments.isEmpty) {
      null //scalastyle:ignore null
    } else {
      val contentParts = buildContentParts(userMessage, orderedAttachments)
      val messages = getPromptsForMessage(Left(contentParts))
      messages
    }
  }

  private def buildContentParts(promptText: String, attachmentPaths: Seq[String]): Seq[Map[String, String]] = {
    var parts = Seq(stringMessageWrapper(promptText))
    if (!attachmentPaths.isEmpty) {
      parts = parts ++ attachmentPaths.flatMap(wrapFileToMessagesList)
    }
    parts
  }

  private def prepareFile(filePathStr: String): (String, Array[Byte], String, String) = {
    val limit = if (isSet(fileSizeLimitMB)) Some(getFileSizeLimitMB) else None
    OpenAIAttachmentUtils.prepareFile(
      filePathStr, limit, imageExtensions, audioExtensions, textExtensions)
  }

  private def makeResponsesFileMessage(
    fileName: String,
    fileBytes: Array[Byte],
    fileType: String,
    mimeType: String
  ): Map[String, String] = {
    OpenAIAttachmentUtils.responsesMessage(fileName, fileBytes, fileType, mimeType, stringMessageWrapper)
  }

  private def makeChatCompletionsFileMessage(
    fileName: String,
    fileBytes: Array[Byte],
    fileType: String,
    mimeType: String
  ): Map[String, String] = {
    OpenAIAttachmentUtils.chatCompletionsMessage(fileBytes, fileType, mimeType, stringMessageWrapper)
  }

  private def wrapFileToMessagesList(filePathStr: String): Seq[Map[String, String]] = {
    val limit = if (isSet(fileSizeLimitMB)) Some(getFileSizeLimitMB) else None
    val fileMessage = if (OpenAIAttachmentUtils.isDataUrl(filePathStr)) {
      this.getApiType match {
        case "responses" =>
          OpenAIAttachmentUtils.responsesDataUrlMessage(
            filePathStr, limit, imageExtensions, audioExtensions, textExtensions, stringMessageWrapper)
        case "chat_completions" =>
          OpenAIAttachmentUtils.chatCompletionsDataUrlMessage(
            filePathStr, limit, imageExtensions, audioExtensions, textExtensions, stringMessageWrapper)
      }
    } else {
      val (fileName, fileBytes, fileType, mimeType) = prepareFile(filePathStr)
      this.getApiType match {
        case "responses" =>
          makeResponsesFileMessage(fileName, fileBytes, fileType, mimeType)
        case "chat_completions" =>
          makeChatCompletionsFileMessage(fileName, fileBytes, fileType, mimeType)
      }
    }
    Seq(fileMessage)
  }

  private def isAIFoundryEndpoint: Boolean = {
    val host = get(url).orElse(getDefault(url)).flatMap { raw =>
      Try(new URI(raw).getHost).toOption.orElse(Try(new URL(raw).getHost).toOption)
    }
    host.exists(_.toLowerCase.endsWith("services.ai.azure.com"))
  }

  private def isOpenAIV1Endpoint: Boolean = {
    get(url).orElse(getDefault(url)).exists(OpenAIEndpointUtils.isV1BaseUrl)
  }

  private[openai] def hasAIFoundryModel: Boolean =
    this.isDefined(model) && isAIFoundryEndpoint && !isOpenAIV1Endpoint

  //deployment name can be set by user, it doesn't have to match with model name
  private[openai] def getOpenAIChatService: OpenAIServicesBase with HasTextOutput = {
    val completion: OpenAIServicesBase with HasTextOutput =
      if (hasAIFoundryModel) {
        new AIFoundryChatCompletion()
      } else {
        // Use the apiType parameter to decide between chat_completions and responses
        getApiType match {
          case "responses" => new OpenAIResponses()
          case "chat_completions" | _ => new OpenAIChatCompletion()
        }
      }

    extractParamMap().toSeq
      .filter(p => !localParamNames.contains(p.param.name) && completion.hasParam(p.param.name))
      .foreach(p => completion.set(completion.getParam(p.param.name), p.value))

    if (this.isDefined(model) &&
        get(deploymentName).orElse(getDefault(deploymentName)).isEmpty &&
        (isOpenAIV1Endpoint || completion.isInstanceOf[OpenAIResponses])) {
      completion.setDeploymentName(getModel)
    }

    completion
  }

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = {
    r =>
      getOpenAIChatService match {
        case chatCompletion: OpenAIResponses =>
          chatCompletion.prepareEntity(r)
        case chatCompletion: AIFoundryChatCompletion =>
          chatCompletion.prepareEntity(r)
        case chatCompletion: OpenAIChatCompletion =>
          chatCompletion.prepareEntity(r)
      }
  }

  private def getEffectivePostProcessing: String = {
    val opts = getPostProcessingOptions
    val effectivePostProcessing = OpenAIPromptPostProcessing.inferMode(opts) match {
      case Some(inferredMode) =>
        val configuredMode = get(postProcessing)
          .getOrElse(throw new IllegalArgumentException(s"postProcessing must be '$inferredMode'"))
        if (configuredMode.isEmpty && postProcessingExplicitlySet) {
          throw new IllegalArgumentException(s"postProcessing must be '$inferredMode'")
        }
        if (configuredMode.nonEmpty) {
          OpenAIPromptPostProcessing.validateModeValue(configuredMode, inferredMode)
        }
        inferredMode
      case None => getPostProcessing
    }
    OpenAIPromptPostProcessing.validateModeOptions(effectivePostProcessing, opts)
    effectivePostProcessing
  }

  private def getParser: OutputParser = {
    val opts = getPostProcessingOptions
    val effectivePostProcessing = getEffectivePostProcessing
    effectivePostProcessing.toLowerCase match {
      case "csv" => new DelimiterParser(opts.getOrElse("delimiter", ","))
      case "json" => new JsonParser(opts("jsonSchema"), Map.empty)
      case "regex" => new RegexParser(opts("regex"), opts("regexGroup").toInt)
      case "" => new PassThroughParser()
      case _ => throw new IllegalArgumentException(s"Unsupported postProcessing type: '$effectivePostProcessing'")
    }
  }

  private def promptMessagesDataType: DataType =
    T.ArrayType(Encoders.product[OpenAICompositeMessage].schema, containsNull = true)

  private def schemaWithPromptMessages(schema: StructType): StructType = {
    val messagesField = StructField(getMessagesCol, promptMessagesDataType, nullable = true)
    OpenAIColumnUtils.replaceOrAppendField(schema, messagesField)
  }

  override def transformSchema(schema: StructType): StructType = {
    validateResponsesApiParams()
    validatePublicColumnNames()
    validatePromptToolOutputColumns(schema)
    val outputDataType: DataType = getParser.outputSchema
    val service = getOpenAIChatService
    val inputSchema = schemaWithPromptMessages(schema)
    val serviceSchema = service match {
      case chatCompletion: OpenAIResponses =>
        chatCompletion.transformSchema(inputSchema)
      case chatCompletion: AIFoundryChatCompletion =>
        chatCompletion.transformSchema(inputSchema)
      case chatCompletion: OpenAIChatCompletion =>
        chatCompletion.transformSchema(inputSchema)
    }

    val responseStructField = serviceSchema.fields.find(_.name == service.getOutputCol)
    val fieldsToDrop = Set(service.getOutputCol) ++
      (if (getDropPrompt) Set(getMessagesCol) else Set.empty[String])
    val withoutServiceOutput = StructType(
      serviceSchema.filterNot(field => fieldsToDrop(field.name)))
    var resultSchema = withoutServiceOutput.add(getOutputCol, outputDataType)

    if (isSet(usageCol) && usageMappingFor(service).isDefined) {
      resultSchema = resultSchema.add(getUsageCol, UsageStructType)
    }

    val isResponsesApi = service.isInstanceOf[OpenAIResponses]
    if (isResponsesApi && getStore) {
      resultSchema = resultSchema.add(getResponseIdCol, T.StringType)
    }
    resultSchema = addPromptToolSchema(resultSchema, responseStructField)

    val errorFieldOpt: Option[StructField] = resultSchema.fields.find(_.name == getErrorCol)
    val fieldsWithoutError: Array[StructField] = resultSchema.fields.filterNot(_.name == getErrorCol)
    StructType(fieldsWithoutError ++ errorFieldOpt.toSeq)
  }
}
// scalastyle:on number.of.methods
