// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.contracts.HasOutputCol
import com.microsoft.azure.synapse.ml.core.spark.Functions
import com.microsoft.azure.synapse.ml.io.http.{ConcurrencyParams, HasErrorCol, HasURL}
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.StringStringMapParam
import com.microsoft.azure.synapse.ml.services._
import org.apache.http.entity.AbstractHttpEntity
import org.apache.spark.ml.param.{BooleanParam, Param, ParamMap, ParamValidators}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, functions => F, types => T}

import scala.collection.JavaConverters._

object OpenAIPrompt extends ComplexParamsReadable[OpenAIPrompt]

class OpenAIPrompt(override val uid: String) extends Transformer
  with HasOpenAITextParamsExtended with HasMessagesInput
  with HasErrorCol with HasOutputCol
  with HasURL with HasCustomCogServiceDomain with ConcurrencyParams
  with HasSubscriptionKey with HasAADToken with HasCustomAuthHeader
  with HasCognitiveServiceInput
  with ComplexParamsWritable with SynapseMLLogging {

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
    // Helper method to validate that regex options contain the required "regexGroup" key
    def validateRegexOptions(options: Map[String, String]): Unit = {
      require(options.contains("regexGroup"), "regexGroup must be specified with regex")
    }

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
        validateRegexOptions(v)
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
    timeout -> 360.0
  )

  override def setCustomServiceName(v: String): this.type = {
    setUrl(s"https://$v.openai.azure.com/" + urlPath.stripPrefix("/"))
  }

  private val localParamNames = Seq(
    "promptTemplate", "outputCol", "postProcessing", "postProcessingOptions", "dropPrompt", "dropMessages",
    "systemPrompt")

  private def addRAIErrors(df: DataFrame, errorCol: String, outputCol: String): DataFrame = {
    val openAIResultFromRow = ChatCompletionResponse.makeFromRowConverter
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

  override def transform(dataset: Dataset[_]): DataFrame = {
    import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions._

    logTransform[DataFrame]({
      val df = dataset.toDF
      val completion = openAICompletion
      val promptCol = Functions.template(getPromptTemplate)
      val createMessagesUDF = udf((userMessage: String) => {
        Seq(
          OpenAIMessage("system", getSystemPrompt),
          OpenAIMessage("user", userMessage)
        )
      })
      completion match {
        case chatCompletion: OpenAIChatCompletion =>
          if (isSet(responseFormat)) {
            chatCompletion.setResponseFormat(getResponseFormat)
          }
          val messageColName = getMessagesCol
          val dfTemplated = df.withColumn(messageColName, createMessagesUDF(promptCol))
          val completionNamed = chatCompletion.setMessagesCol(messageColName)
          val transformed = addRAIErrors(
            completionNamed.transform(dfTemplated), chatCompletion.getErrorCol, chatCompletion.getOutputCol)

          val results = transformed
            .withColumn(getOutputCol,
              getParser.parse(F.element_at(F.col(completionNamed.getOutputCol).getField("choices"), 1)
                .getField("message").getField("content"))).drop(completionNamed.getOutputCol)

          if (getDropPrompt) {
            results.drop(messageColName)
          } else {
            results
          }

        case completion: OpenAICompletion =>
          if (isSet(responseFormat)) {
            throw new IllegalArgumentException("responseFormat is not supported for OpenAICompletion")
          }
          val promptColName = df.withDerivativeCol("prompt")
          val dfTemplated = df.withColumn(promptColName, promptCol)
          val completionNamed = completion.setPromptCol(promptColName)

          // run completion
          val results = completionNamed
            .transform(dfTemplated)
            .withColumn(getOutputCol,
              getParser.parse(F.element_at(F.col(completionNamed.getOutputCol).getField("choices"), 1)
                .getField("text")))
            .drop(completionNamed.getOutputCol)

          if (getDropPrompt) {
            results.drop(promptColName)
          } else {
            results
          }
      }
    }, dataset.columns.length)
  }

  private val legacyModels = Set("ada", "babbage", "curie", "davinci",
    "text-ada-001", "text-babbage-001", "text-curie-001", "text-davinci-002", "text-davinci-003",
    "code-cushman-001", "code-davinci-002")

  private def openAICompletion: OpenAIServicesBase = {

    val completion: OpenAIServicesBase =
      if (legacyModels.contains(getDeploymentName)) {
        new OpenAICompletion()
      }
      else {
        new OpenAIChatCompletion()
      }
    // apply all parameters
    extractParamMap().toSeq
      .filter(p => completion.hasParam(p.param.name))
      .filter(p => !localParamNames.contains(p.param.name))
      .foreach(p => completion.set(completion.getParam(p.param.name), p.value))

    completion
  }

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = {
    r =>
      openAICompletion match {
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
    openAICompletion match {
      case chatCompletion: OpenAIChatCompletion =>
        chatCompletion
          .transformSchema(schema.add(getMessagesCol, StructType(Seq())))
          .add(getPostProcessing, getParser.outputSchema)
      case completion: OpenAICompletion =>
        completion
          .transformSchema(schema)
          .add(getPostProcessing, getParser.outputSchema)
    }
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
  def parse(responseCol: Column): Column = F.from_json(responseCol, schema, options)

  def outputSchema: T.DataType = DataType.fromDDL(schema)
}

class RegexParser(val regex: String, val groupIdx: Int) extends OutputParser {
  def parse(responseCol: Column): Column = F.regexp_extract(responseCol, regex, groupIdx)

  def outputSchema: T.DataType = T.StringType
}
