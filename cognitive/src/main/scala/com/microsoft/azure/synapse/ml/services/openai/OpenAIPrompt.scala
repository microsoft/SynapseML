// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.contracts.HasOutputCol
import com.microsoft.azure.synapse.ml.core.spark.Functions
import com.microsoft.azure.synapse.ml.io.http.{ConcurrencyParams, HasErrorCol, HasURL}
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.{HasGlobalParams, StringStringMapParam}
import com.microsoft.azure.synapse.ml.services._
import org.apache.http.entity.AbstractHttpEntity
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.ml.param.{BooleanParam, Param, ParamMap, ParamValidators}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, functions => F, types => T}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, typedLit, udf}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import com.microsoft.azure.synapse.ml.services.aifoundry.{AIFoundryChatCompletion, HasAIFoundryTextParamsExtended}

import java.io.{ByteArrayInputStream, File}
import java.net.{URI, URL, URLConnection}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.Base64

import scala.collection.JavaConverters._
import scala.util.{Try, Using}

private[openai] case class MessagePayload(
  role: String,
  content: Option[String],
  name: Option[String],
  contentParts: Option[Seq[Map[String, String]]]
)

object OpenAIPrompt extends ComplexParamsReadable[OpenAIPrompt]

class OpenAIPrompt(override val uid: String) extends Transformer
  with HasAIFoundryTextParamsExtended
  with HasOpenAITextParamsExtended with HasMessagesInput
  with HasErrorCol with HasOutputCol
  with HasURL with HasCustomCogServiceDomain with ConcurrencyParams
  with HasSubscriptionKey with HasAADToken with HasCustomAuthHeader
  with HasCognitiveServiceInput
  with ComplexParamsWritable with SynapseMLLogging with HasGlobalParams {

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
    this, "dropPrompt", "whether to drop the column of prompts after templating (when using legacy models)")

  def getDropPrompt: Boolean = $(dropPrompt)

  def setDropPrompt(value: Boolean): this.type = set(dropPrompt, value)

  val systemPrompt = new Param[String](
    this, "systemPrompt", "The initial system prompt to be used.")

  def getSystemPrompt: String = $(systemPrompt)

  def setSystemPrompt(value: String): this.type = set(systemPrompt, value)

  val openAIAPIType = new Param[String](
    this, "openAIAPIType", "The OpenAI API type to use: 'chat_completions' or 'responses'",
    isValid = ParamValidators.inArray(Array("chat_completions", "responses")))

  def getOpenAIAPIType: String = $(openAIAPIType)

  def setOpenAIAPIType(value: String): this.type = set(openAIAPIType, value)

  val columnTypes = new Param[Map[String, String]](
    this, "columnTypes", "A map from column names to their types. Supported types are 'text' and 'path'.")
  
  def getColumnTypes: Map[String, String] = $(columnTypes)
  
  def setColumnTypes(value: Map[String, String]): this.type = {
    for ((colName, colType) <- value) {
      setColumnType(colName, colType)
    }
    this
  }

  def setColumnType(columnName: String, columnType: String): this.type = {
    require(columnType.equalsIgnoreCase("text") || columnType.equalsIgnoreCase("path"),
      s"Unsupported column type: $columnType. Supported types are 'text' and 'path'.")
    val currentTypes = if (isSet(columnTypes)) getColumnTypes else Map.empty[String, String]
    set(columnTypes, currentTypes + (columnName -> columnType))
    this
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
    openAIAPIType -> "chat_completions",
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
    "systemPrompt", "openAIAPIType")

  private val multiModalTextPrompt = "The name of the file to analyze is %s.\nHere is the content:\n"
  private val textExtensions = Set("md", "csv", "tsv", "json", "xml", "html", "htm")
  private val imageExtensions = Set("jpg", "jpeg", "png", "gif", "webp")
  private val audioExtensions = Set("mp3", "wav")

  private def attachmentPlaceholder(columnName: String): String =
    s"[Content for column '$columnName' will be provided later as an attachment.]"

  private[openai] def applyPathPlaceholders(template: String, pathColumns: Seq[String]): String = {
    pathColumns.foldLeft(template) { (current, columnName) =>
      current.replace(s"{$columnName}", attachmentPlaceholder(columnName))
    }
  }

  // Q: What does RAI means?
  private def addChatRAIErrors(df: DataFrame, errorCol: String, outputCol: String): DataFrame = {
    val openAIResultFromRow = ChatModelResponse.makeFromRowConverter
    df.map({ row =>
      val originalOutput = Option(row.getAs[Row](outputCol))
        .map({ row => openAIResultFromRow(row).choices.head })
      val isFiltered = originalOutput.exists(output => Option(output.message.content).isEmpty)

      if (isFiltered) {
        val updatedRowSeq = row.toSeq.updated(
          row.fieldIndex(errorCol),
          Row(originalOutput.get.finish_reason, null) //scalastyle:ignore null
        )
        Row.fromSeq(updatedRowSeq)
      } else {
        row
      }
    })(RowEncoder(df.schema))
  }
  private def addResponsesRAIErrors(df: DataFrame, errorCol: String, outputCol: String): DataFrame = {
    val openAIResultFromRow = ResponsesModelResponse.makeFromRowConverter
    df.map({ row =>
      val originalOutput = Option(row.getAs[Row](outputCol))
        .map({ row => openAIResultFromRow(row).output.head })
      val isFiltered = originalOutput.exists(output => Option(output.content).isEmpty)

      if (isFiltered) {
        val updatedRowSeq = row.toSeq.updated(
          row.fieldIndex(errorCol),
          Row(originalOutput.get.status, null) //Q: what does this line do?
        )
        Row.fromSeq(updatedRowSeq)
      } else {
        row
      }
    })(RowEncoder(df.schema))
  }


  override def transform(dataset: Dataset[_]): DataFrame = {
    import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions._
    transferGlobalParamsToParamMap()
    logTransform[DataFrame]({
      val df = dataset.toDF
      val completion = openAICompletion
      val columnTypeMap = if (isSet(columnTypes)) getColumnTypes else Map.empty[String, String]

      columnTypeMap.foreach { case (colName, colType) =>
        val normalized = colType.toLowerCase
        require(normalized == "text" || normalized == "path",
          s"Unsupported column type '$colType' for column '$colName'. Supported types are 'text' and 'path'.")
      }

      val pathColumnNames = columnTypeMap.collect { case (colName, colType) if colType.equalsIgnoreCase("path") => colName }.toSeq

      val promptTemplateWithPlaceholders = applyPathPlaceholders(getPromptTemplate, pathColumnNames)
      val promptCol = Functions.template(promptTemplateWithPlaceholders)

      pathColumnNames.foreach { colName =>
        require(df.columns.contains(colName), s"Column '$colName' specified in columnTypes was not found in the DataFrame.")
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

      val createMessagesUDF = udf[Seq[MessagePayload], String, Map[String, String]] {
        (userMessage, attachmentMap) =>
          createMessagesForRow(
            userMessage,
            Option(attachmentMap).getOrElse(Map.empty[String, String]),
            pathColumnNames
          )
      }

      completion match {
        case chatCompletion: OpenAIResponses =>
          if (isSet(responseFormat)) {
            chatCompletion.setResponseFormat(getResponseFormat)
          }
          val messageColName = getMessagesCol
          val dfTemplated = df.withColumn(messageColName, createMessagesUDF(promptCol, attachmentsColumn))
          val completionNamed = chatCompletion.setMessagesCol(messageColName)

          val transformed = addResponsesRAIErrors(
            completionNamed.transform(dfTemplated), chatCompletion.getErrorCol, chatCompletion.getOutputCol)
          val results = transformed
            .withColumn(getOutputCol,
              getParser.parse(F.element_at(F.element_at(F.col(completionNamed.getOutputCol).getField("output"), 1)
                .getField("content"), 1).getField("text")))
            .drop(completionNamed.getOutputCol)
          val resultsFinal = results.select(results.columns.filter(_ != getErrorCol).map(col) :+ col(getErrorCol): _*)
          if (getDropPrompt) {
            resultsFinal.drop(messageColName)
          } else {
            resultsFinal
          }
        case chatCompletion: OpenAIChatCompletion =>
          if (isSet(responseFormat)) {
            chatCompletion.setResponseFormat(getResponseFormat)
          }
          val messageColName = getMessagesCol
          val dfTemplated = df.withColumn(messageColName, createMessagesUDF(promptCol, attachmentsColumn))
          val completionNamed = chatCompletion.setMessagesCol(messageColName)

          val transformed = addChatRAIErrors(
            completionNamed.transform(dfTemplated), chatCompletion.getErrorCol, chatCompletion.getOutputCol)
          val results = transformed
            .withColumn(getOutputCol,
              getParser.parse(F.element_at(F.col(completionNamed.getOutputCol).getField("choices"), 1)
                .getField("message").getField("content")))
            .drop(completionNamed.getOutputCol)
          val resultsFinal = results.select(results.columns.filter(_ != getErrorCol).map(col) :+ col(getErrorCol): _*)
          if (getDropPrompt) {
            resultsFinal.drop(messageColName)
          } else {
            resultsFinal
          }
        case completion: OpenAICompletion =>
          if (isSet(responseFormat)) {
            throw new IllegalArgumentException("responseFormat is not supported for completion models")
          }
          val promptColName = df.withDerivativeCol("prompt")
          val dfTemplated = df.withColumn(promptColName, promptCol)
          val completionNamed = completion.setPromptCol(promptColName)
          val results = completionNamed
            .transform(dfTemplated)
            .withColumn(getOutputCol,
              getParser.parse(F.element_at(F.col(completionNamed.getOutputCol).getField("choices"), 1)
                .getField("text")))
            .drop(completionNamed.getOutputCol)
          val resultsFinal = results.select(results.columns.filter(_ != getErrorCol).map(col) :+ col(getErrorCol): _*)
          if (getDropPrompt) {
            resultsFinal.drop(promptColName)
          } else {
            resultsFinal
          }

      }
    }, dataset.columns.length)
  }

  // If the response format is set, add a system prompt to the messages. This is required by the
  // OpenAI api. If the reponseFormat is json and the prompt does not contain string 'JSON' then 400 error is returned
  // For this reason we add a system prompt to the messages.
  // This method is made private[openai] for testing purposes
  private[openai] def getPromptsForMessage(userMessage: String) = {
    val basePrompts = Seq(
      OpenAIMessage("system", getSystemPrompt),
      OpenAIMessage("user", userMessage)
      )

    if (isSet(responseFormat)) {
      val responseFormatPrompt = OpenAIChatCompletionResponseFormat
        .fromResponseFormatString(getResponseFormat("type"))
        .prompt
      basePrompts :+ OpenAIMessage("system", responseFormatPrompt)
    } else {
      basePrompts
    }
  }

  private[openai] def createMessagesForRow(userMessage: String,
                                          attachmentMap: Map[String, String],
                                          attachmentOrder: Seq[String]): Seq[MessagePayload] = {
    val orderedAttachments = attachmentOrder.flatMap { columnName =>
      attachmentMap.get(columnName).map(_.trim).filter(_.nonEmpty)
    }

    val contentParts = buildContentParts(userMessage, orderedAttachments)
    val baseMessages = getPromptsForMessage(userMessage)

    baseMessages.map { message =>
      val hasAttachments = message.role == "user" && contentParts.nonEmpty
      val partsOpt = if (hasAttachments) Some(contentParts) else None
      val contentOpt = if (hasAttachments) None else Option(message.content)
      MessagePayload(message.role, contentOpt, message.name, partsOpt)
    }
  }

  private def buildContentParts(promptText: String, attachmentPaths: Seq[String]): Seq[Map[String, String]] = {
    if (attachmentPaths.isEmpty) {
      Seq.empty
    } else {
      val basePromptPart = Map("type" -> "input_text", "text" -> promptText)
      basePromptPart +: attachmentPaths.flatMap(wrapFileToMessagesList)
    }
  }

  private def wrapFileToMessagesList(filePath: String): Seq[Map[String, String]] = {
    val (fileBytes, fileName) = readFileBytes(filePath)
    val (extension, fileType, mimeType) = inferFileType(filePath, fileBytes)
    val baseMessage = Map("type" -> "input_text", "text" -> multiModalTextPrompt.format(fileName))

    val fileMessage = fileType match {
      case "text" =>
        val textContent = new String(fileBytes, StandardCharsets.UTF_8)
        Map("type" -> "input_text", "text" -> s"File Name: ${fileName}\nContent: ${textContent}")
      case "image" =>
        val encoded = Base64.getEncoder.encodeToString(fileBytes)
        val mime = if (mimeType.nonEmpty) mimeType else s"image/${extension}".stripSuffix("/")
        Map("type" -> "input_image", "image_url" -> s"data:${mime};base64,${encoded}")
      case "audio" =>
        throw new IllegalArgumentException("Audio input is not supported in the current API version.")
      case _ =>
        val encoded = Base64.getEncoder.encodeToString(fileBytes)
        val mime = if (mimeType.nonEmpty) mimeType else s"application/${if (extension.nonEmpty) extension else "octet-stream"}"
        Map(
          "type" -> "input_file",
          "filename" -> fileName,
          "file_data" -> s"data:${mime};base64,${encoded}"
        )
    }

    Seq(baseMessage, fileMessage)
  }

  private def readFileBytes(filePath: String): (Array[Byte], String) = {
    val maybeUri = Try(new URI(filePath)).toOption

    maybeUri match {
      case Some(uri) if Option(uri.getScheme).exists(s => s.equalsIgnoreCase("http") || s.equalsIgnoreCase("https")) =>
        val url = uri.toURL
        val fileName = extractFileName(uri.getPath)
        val bytes = Using.resource(url.openStream()) { stream =>
          stream.readAllBytes()
        }
        (bytes, fileName)
      case Some(uri) if Option(uri.getScheme).contains("file") =>
        val path = Paths.get(uri)
        val fileName = Option(path.getFileName).map(_.toString).getOrElse(extractFileName(filePath))
        (Files.readAllBytes(path), fileName)
      case _ =>
        val path = Paths.get(filePath)
        val fileName = Option(path.getFileName).map(_.toString).getOrElse(extractFileName(filePath))
        (Files.readAllBytes(path), fileName)
    }
  }

  private def extractFileName(path: String): String = {
    val trimmed = path.trim
    val file = new File(trimmed)
    val nameFromFile = file.getName
    if (nameFromFile.nonEmpty && !nameFromFile.endsWith(File.separator)) {
      nameFromFile
    } else {
      val sanitized = trimmed.replaceAll("[\\\\/]+$", "")
      val idx = sanitized.lastIndexOf('/')
      if (idx >= 0 && idx + 1 < sanitized.length) sanitized.substring(idx + 1)
      else if (sanitized.nonEmpty) sanitized
      else "attachment"
    }
  }

  private def inferFileType(filePath: String, fileBytes: Array[Byte]): (String, String, String) = {
    val fileName = extractFileName(filePath).toLowerCase
    val extension =
      if (fileName.contains('.')) fileName.substring(fileName.lastIndexOf('.') + 1)
      else ""

    val mimeFromName = Option(URLConnection.guessContentTypeFromName(fileName)).getOrElse("")
    val mimeFromContent = Option(URLConnection.guessContentTypeFromStream(new ByteArrayInputStream(fileBytes))).getOrElse("")
    val mimeType = if (mimeFromName.nonEmpty) mimeFromName else mimeFromContent

    val fileType =
      if (mimeType == "application/pdf") {
        "file"
      } else if (mimeType.startsWith("image/")) {
        "image"
      } else if (mimeType.startsWith("audio/")) {
        "audio"
      } else if (mimeType.startsWith("text/")) {
        "text"
      } else if (imageExtensions.contains(extension)) {
        "image"
      } else if (audioExtensions.contains(extension)) {
        "audio"
      } else if (textExtensions.contains(extension)) {
        "text"
      } else {
        "file"
      }

    (extension, fileType, mimeType)
  }

  private[openai] def hasAIFoundryModel: Boolean = this.isDefined(model)

  //deployment name can be set by user, it doesn't have to match with model name
  private val legacyModels = Set("ada", "babbage", "curie", "davinci",
    "text-ada-001", "text-babbage-001", "text-curie-001", "text-davinci-002", "text-davinci-003",
    "code-cushman-001", "code-davinci-002")

  private def openAICompletion: OpenAIServicesBase = {

    val completion: OpenAIServicesBase =
      if (hasAIFoundryModel) {
        new AIFoundryChatCompletion()
      }
      else if (legacyModels.contains(getDeploymentName)) {
        new OpenAICompletion()
      }
      else {
        // Use the openAIAPIType parameter to decide which API to use
        getOpenAIAPIType match {
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
      openAICompletion match {
        case chatCompletion: OpenAIResponses =>
          chatCompletion.prepareEntity(r)
        case chatCompletion: AIFoundryChatCompletion =>
          chatCompletion.prepareEntity(r)
        case chatCompletion: OpenAIChatCompletion =>
          chatCompletion.prepareEntity(r)
        case completion: OpenAICompletion =>
          completion.prepareEntity(r)
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
    val transformedSchema = openAICompletion match {
      case chatCompletion: OpenAIResponses =>
        chatCompletion
          .transformSchema(schema.add(getMessagesCol, StructType(Seq())))
          .add(getPostProcessing, getParser.outputSchema)
      case chatCompletion: AIFoundryChatCompletion =>
        chatCompletion
          .transformSchema(schema.add(getMessagesCol, StructType(Seq())))
          .add(getPostProcessing, getParser.outputSchema)
      case chatCompletion: OpenAIChatCompletion =>
        chatCompletion
          .transformSchema(schema.add(getMessagesCol, StructType(Seq())))
          .add(getPostProcessing, getParser.outputSchema)
      case completion: OpenAICompletion =>
        completion
          .transformSchema(schema)
          .add(getPostProcessing, getParser.outputSchema)
    }

   // Move error column to back
    val errorFieldOpt: Option[StructField] = transformedSchema.fields.find(_.name == getErrorCol)
    val fieldsWithoutError: Array[StructField] = transformedSchema.fields.filterNot(_.name == getErrorCol)
    val reorderedFields = Array.concat(fieldsWithoutError, errorFieldOpt.toArray)
    StructType(reorderedFields)
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
