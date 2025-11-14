// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.contracts.HasOutputCol
import com.microsoft.azure.synapse.ml.core.spark.Functions
import com.microsoft.azure.synapse.ml.io.binary.BinaryFileReader
import com.microsoft.azure.synapse.ml.io.http.{ConcurrencyParams, HasErrorCol, HasURL}
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.{HasGlobalParams, StringStringMapParam}
import com.microsoft.azure.synapse.ml.services._
import com.microsoft.azure.synapse.ml.services.aifoundry.{AIFoundryChatCompletion, HasAIFoundryTextParamsExtended}
import HasReturnUsage.{UsageFieldMapping, UsageMappings}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HPath}
import org.apache.http.entity.AbstractHttpEntity
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.ml.param.{BooleanParam, Param, ParamMap, ParamValidators}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, functions => F, types => T}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, typedLit, udf}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import spray.json.DefaultJsonProtocol._


import java.io.ByteArrayInputStream
import java.net.{URI, URL, URLConnection}
import java.nio.charset.StandardCharsets
import java.util.Base64

import scala.collection.JavaConverters._
import scala.util.{Try, Using}

object OpenAIPrompt extends ComplexParamsReadable[OpenAIPrompt]

class OpenAIPrompt(override val uid: String) extends Transformer
  with HasAIFoundryTextParamsExtended
  with HasOpenAITextParamsExtended with HasMessagesInput
  with HasErrorCol with HasOutputCol
  with HasURL with HasCustomCogServiceDomain with ConcurrencyParams
  with HasSubscriptionKey with HasAADToken with HasCustomAuthHeader
  with HasCognitiveServiceInput
  with ComplexParamsWritable with SynapseMLLogging with HasGlobalParams
  with HasReturnUsage {

  logClass(FeatureNames.AiServices.OpenAI)

  def this() = this(Identifiable.randomUID("OpenAIPrompt"))

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  def urlPath: String = ""

  override private[ml] def internalServiceType: String = "openai"

  val promptTemplate = new Param[String](
    this, "promptTemplate", "The prompt. supports string interpolation {col1}: {col2}.")

  def getPromptTemplate: String = $(promptTemplate)

  def setPromptTemplate(value: String): this.type = set(promptTemplate, value)

  val postProcessing = new Param[String](
    this, "postProcessing", "Post processing options: csv, json, regex",
    isValid = ParamValidators.inArray(Array("", "csv", "json", "regex")))

  def getPostProcessing: String = $(postProcessing)

  def setPostProcessing(value: String): this.type = set(postProcessing, value)

  val postProcessingOptions = new StringStringMapParam(
    this, "postProcessingOptions", "Options (default): delimiter=',', jsonSchema, regex, regexGroup=0")

  def getPostProcessingOptions: Map[String, String] = $(postProcessingOptions)

  def setPostProcessingOptions(value: Map[String, String]): this.type = {
    // Helper method to set or validate the postProcessing parameter
    def setOrValidatePostProcessing(expected: String): Unit = {
      if (isSet(postProcessing)) {
        require(getPostProcessing == expected, s"postProcessing must be '$expected'")
      } else {
        set(postProcessing, expected)
      }
    }

    // Match on the keys in the provided value map to set the appropriate post-processing option
    value match {
      case v if v.contains("delimiter") =>
        setOrValidatePostProcessing("csv")
      case v if v.contains("jsonSchema") =>
        setOrValidatePostProcessing("json")
      case v if v.contains("regex") =>
        require(v.contains("regexGroup"), "regexGroup must be specified with regex")
        setOrValidatePostProcessing("regex")
      case _ =>
        throw new IllegalArgumentException("Invalid post processing options")
    }

    // Set the postProcessingOptions parameter with the provided value map
    set(postProcessingOptions, value)
  }

  def setPostProcessingOptions(v: java.util.HashMap[String, String]): this.type =
    set(postProcessingOptions, v.asScala.toMap)

  val dropPrompt = new BooleanParam(
    this, "dropPrompt", "whether to drop the column of prompts after templating")

  def getDropPrompt: Boolean = $(dropPrompt)

  def setDropPrompt(value: Boolean): this.type = set(dropPrompt, value)

  val systemPrompt = new Param[String](
    this, "systemPrompt", "The initial system prompt to be used.")

  def getSystemPrompt: String = $(systemPrompt)

  def setSystemPrompt(value: String): this.type = set(systemPrompt, value)

  val apiType = new Param[String](
    this, "apiType", "The OpenAI API type to use: 'chat_completions' or 'responses'",
    isValid = ParamValidators.inArray(Array("chat_completions", "responses")))

  def getApiType: String = $(apiType)

  def setApiType(value: String): this.type = set(apiType, value)

  val columnTypes = new StringStringMapParam(
    this, "columnTypes", "A map from column names to their types. Supported types are 'text' and 'path'.")
  private def validateColumnType(value: String) = {
    require(value.equalsIgnoreCase("text") || value.equalsIgnoreCase("path"),
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
    set(columnTypes, v.asScala.toMap)

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
    timeout -> 360.0
  )

  override def setCustomServiceName(v: String): this.type = {
    setUrl(s"https://$v.openai.azure.com/" + urlPath.stripPrefix("/"))
  }

  def setAIFoundryCustomServiceName(v: String): this.type = {
    setUrl(s"https://$v.services.ai.azure.com/" + urlPath.stripPrefix("/"))
  }

  private val localParamNames = Seq(
    "promptTemplate", "outputCol", "postProcessing", "postProcessingOptions", "dropPrompt", "dropMessages",
    "systemPrompt", "apiType", "returnUsage")

  private val multiModalTextPrompt = "The name of the file to analyze is %s.\nHere is the content:\n"

  private val textExtensions = Set("md", "csv", "tsv", "json", "xml")
  private val imageExtensions = Set("jpg", "jpeg", "png", "gif", "webp")
  private val audioExtensions = Set("mp3", "wav")

  private def attachmentPlaceholder(columnName: String): String =
    s"[Content for column '$columnName' will be provided later as an attachment.]"

  private[openai] def applyPathPlaceholders(template: String, pathColumns: Seq[String]): String = {
    pathColumns.foldLeft(template) { (current, columnName) =>
      current.replace(s"{$columnName}", attachmentPlaceholder(columnName))
    }
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
    promptCol: Column,
    createMessagesUDF: UserDefinedFunction,
    attachmentsColumn: Column
  ): (DataFrame, String, OpenAIServicesBase with HasTextOutput) = {
    if (isSet(responseFormat)) {
      service match {
        case cc: OpenAIChatCompletion => cc.setResponseFormat(getResponseFormat)
        case resp: OpenAIResponses => resp.setResponseFormat(getResponseFormat)
        case _ => // AIFoundryChatCompletion does not currently expose responseFormat.
      }
    }

    val messageColName = getMessagesCol
    val configuredService = service match {
      case c: HasMessagesInput => c.setMessagesCol(messageColName)
      case other => other
    }

    (
      df.withColumn(messageColName, createMessagesUDF(promptCol, attachmentsColumn)),
      messageColName,
      configuredService
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

  private def withUsageColumn(
      df: DataFrame,
      responseCol: Column,
      parsedCol: Column,
      mappingOpt: Option[UsageFieldMapping]): DataFrame = {
    mappingOpt match {
      case Some(mapping) =>
        val usageCol = normalizeUsageColumn(responseCol.getField("usage"), mapping)
        df.withColumn(
          getOutputCol,
          F.struct(parsedCol.alias("response"), usageCol.alias("usage"))
        )
      case None =>
        df.withColumn(getOutputCol, parsedCol)
    }
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

    val responseCol = F.col(service.getOutputCol)
    val parsedOutputCol = getParser.parse(service.getOutputMessageText(service.getOutputCol))
    val withParsed = withUsageColumn(
      transformed,
      responseCol,
      parsedOutputCol,
      if (getReturnUsage) usageMappingFor(service) else None
    )

    val results = withParsed.drop(service.getOutputCol)

    results.select(results.columns.filter(_ != getErrorCol).map(col) :+ col(getErrorCol): _*)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    transferGlobalParamsToParamMap()
    logTransform[DataFrame]({
      val df = dataset.toDF
      val service = getOpenAIChatService
      val columnTypeMap = if (isSet(columnTypes)) getColumnTypes else Map.empty[String, String]

      columnTypeMap.foreach { case (colName, colType) =>
        val normalized = colType.toLowerCase
        require(normalized == "text" || normalized == "path",
          s"Unsupported column type '$colType' for column '$colName'. Supported types are 'text' and 'path'.")
      }

      val pathColumnNames = columnTypeMap.collect {
        case (colName, colType) if colType.equalsIgnoreCase("path") => colName
        }.toSeq

      val promptTemplateWithPlaceholders = applyPathPlaceholders(getPromptTemplate, pathColumnNames)
      val promptCol = Functions.template(promptTemplateWithPlaceholders)

      pathColumnNames.foreach { colName =>
        require(
          df.columns.contains(colName),
          s"Column '$colName' specified in columnTypes was not found in the DataFrame."
          )
      }

      val attachmentsColumn: Column =
        if (pathColumnNames.nonEmpty) {
          val mapEntries = pathColumnNames.flatMap { columnName =>
            Seq(F.lit(columnName), F.col(columnName).cast(T.StringType))
          }
          F.map(mapEntries: _*)
        } else {
          typedLit(Map.empty[String, String])
        }

      val createMessagesUDF = udf[Seq[OpenAICompositeMessage], String, Map[String, String]] {
        (userMessage, attachmentMap) =>
          if (userMessage == null) {
            null //scalastyle:ignore null
          } else {
            createMessagesForRow(
              userMessage,
              Option(attachmentMap).getOrElse(Map.empty[String, String]),
              pathColumnNames
            )
          }
      }

      val (dfTemplated, inputColName, serviceConfigured) =
        configureService(service, df, promptCol, createMessagesUDF, attachmentsColumn)
      val result = generateText(serviceConfigured, dfTemplated)
      if (getDropPrompt) result.drop(inputColName) else result
    }, dataset.columns.length)
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
    val orderedAttachments = attachmentOrder.flatMap { columnName =>
      attachmentMap.get(columnName).map(_.trim).filter(_.nonEmpty)
    }

    val contentParts = buildContentParts(userMessage, orderedAttachments)
    val messages = getPromptsForMessage(Left(contentParts))
    messages
  }

  private def buildContentParts(promptText: String, attachmentPaths: Seq[String]): Seq[Map[String, String]] = {
    var parts = Seq(stringMessageWrapper(promptText))
    if (!attachmentPaths.isEmpty) {
      parts = parts ++ attachmentPaths.flatMap(wrapFileToMessagesList)
    }
    parts
  }

  private def prepareFile(filePathStr: String): (String, Array[Byte], String, String) = {
    val filePath = new HPath(filePathStr)
    val fileBytes = BinaryFileReader.readSingleFileBytes(filePath)
    val fileName = filePath.getName
    val extension = fileName.lastIndexOf('.') match {
      case idx if idx >= 0 => fileName.substring(idx + 1).toLowerCase
      case _ => ""
    }

    // Use URLConnection to infer MIME type from byte content
    val mimeType = Option(URLConnection.guessContentTypeFromStream(new ByteArrayInputStream(fileBytes))).getOrElse(
      // Fallback to URLConnection.guessContentTypeFromName if content-based detection fails
      Option(URLConnection.guessContentTypeFromName(fileName)).getOrElse("application/octet-stream")
    )

    val fileType = categorizeFileType(mimeType, extension)
    (fileName, fileBytes, fileType, mimeType)
  }

  private def makeResponsesFileMessage(
    fileName: String,
    fileBytes: Array[Byte],
    fileType: String,
    mimeType: String
  ): Map[String, String] = {
    fileType match {
      case "text" =>
        stringMessageWrapper(s"${new String(fileBytes, StandardCharsets.UTF_8)}")
      case "image" =>
        Map(
          "type" -> "input_image",
          "image_url" -> s"data:${mimeType};base64,${Base64.getEncoder.encodeToString(fileBytes)}"
        )
      case "audio" =>
        throw new IllegalArgumentException("Audio input is not supported in the current API version.")
      case _ =>
        Map(
          "type" -> "input_file",
          "filename" -> fileName,
          "file_data" -> s"data:${mimeType};base64,${Base64.getEncoder.encodeToString(fileBytes)}"
        )
    }
  }

  private def makeChatCompletionsFileMessage(
    fileName: String,
    fileBytes: Array[Byte],
    fileType: String,
    mimeType: String
  ): Map[String, String] = {
    fileType match {
      case "text" =>
        stringMessageWrapper(s"Content: ${new String(fileBytes, StandardCharsets.UTF_8)}")
      case _ =>
        throw new IllegalArgumentException(s"Multimodal Input is not supported in Chat Completions API.")
    }
  }

  private def wrapFileToMessagesList(filePathStr: String): Seq[Map[String, String]] = {
    val (fileName, fileBytes, fileType, mimeType) = prepareFile(filePathStr)
    val baseMessage = stringMessageWrapper(multiModalTextPrompt.format(fileName))

    val fileMessage = this.getApiType match {
      case "responses" =>
        makeResponsesFileMessage(fileName, fileBytes, fileType, mimeType)
      case "chat_completions" =>
        makeChatCompletionsFileMessage(fileName, fileBytes, fileType, mimeType)
    }
    Seq(baseMessage, fileMessage)
  }

  private def categorizeFileType(mimeType: String, extension: String): String = {
    if (mimeType == "application/pdf") "file"
    else if (mimeType.startsWith("image/") && imageExtensions.contains(extension)) "image"
    else if (mimeType.startsWith("audio/") && audioExtensions.contains(extension)) "audio"
    else if (mimeType.startsWith("text/") || textExtensions.contains(extension)) "text"
    else "file"
  }

  private[openai] def hasAIFoundryModel: Boolean = this.isDefined(model)

  private def getOpenAIChatService: OpenAIServicesBase with HasTextOutput = {

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

    // apply all parameters
    extractParamMap().toSeq
      .filter(p => !localParamNames.contains(p.param.name) && completion.hasParam(p.param.name))
      .foreach(p => completion.set(completion.getParam(p.param.name), p.value))

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

  private def getParser: OutputParser = {
    val opts = getPostProcessingOptions

    getPostProcessing.toLowerCase match {
      case "csv" => new DelimiterParser(opts.getOrElse("delimiter", ","))
      case "json" => new JsonParser(opts("jsonSchema"), Map.empty)
      case "regex" => new RegexParser(opts("regex"), opts("regexGroup").toInt)
      case "" => new PassThroughParser()
      case _ => throw new IllegalArgumentException(s"Unsupported postProcessing type: '$getPostProcessing'")
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    val service = getOpenAIChatService
    val serviceSchema = service match {
      case chatCompletion: OpenAIResponses =>
        chatCompletion.transformSchema(schema.add(getMessagesCol, StructType(Seq())))
      case chatCompletion: AIFoundryChatCompletion =>
        chatCompletion.transformSchema(schema.add(getMessagesCol, StructType(Seq())))
      case chatCompletion: OpenAIChatCompletion =>
        chatCompletion.transformSchema(schema.add(getMessagesCol, StructType(Seq())))
    }

    val outputDataType: DataType = {
      val parserOutputSchema = getParser.outputSchema
      if (getReturnUsage) {
        T.StructType(Seq(
          StructField("response", parserOutputSchema, nullable = true),
          StructField("usage", usageStructType, nullable = true)
        ))
      } else {
        parserOutputSchema
      }
    }

    val withoutServiceOutput = StructType(serviceSchema.filterNot(_.name == service.getOutputCol))
    val withPromptOutput = withoutServiceOutput.add(getOutputCol, outputDataType)

    val errorFieldOpt: Option[StructField] = withPromptOutput.fields.find(_.name == getErrorCol)
    val fieldsWithoutError: Array[StructField] = withPromptOutput.fields.filterNot(_.name == getErrorCol)
    StructType(fieldsWithoutError ++ errorFieldOpt.toSeq)
  }
}

trait OutputParser {
  def parse(responseCol: Column): Column

  def outputSchema: T.DataType
}

class PassThroughParser extends OutputParser {
  def parse(responseCol: Column): Column = responseCol

  def outputSchema: T.DataType = T.StringType
}

class DelimiterParser(val delimiter: String) extends OutputParser {
  def parse(responseCol: Column): Column = F.split(F.trim(responseCol), delimiter)

  def outputSchema: T.DataType = T.ArrayType(T.StringType)
}

class JsonParser(val schema: String, options: Map[String, String]) extends OutputParser {
  private val cleanJsonString: Column => Column = col =>
    F.regexp_replace(
      // Remove optional leading/trailing code fences and optional 'json' prefix
      F.trim(col),
      """^(```|''')?\s*json\s*|(```|''')$""",
      ""
    )

  def parse(responseCol: Column): Column =
    F.from_json(cleanJsonString(responseCol), schema, options)

  def outputSchema: T.DataType = DataType.fromDDL(schema)
}

class RegexParser(val regex: String, val groupIdx: Int) extends OutputParser {
  def parse(responseCol: Column): Column = F.regexp_extract(responseCol, regex, groupIdx)

  def outputSchema: T.DataType = T.StringType
}
