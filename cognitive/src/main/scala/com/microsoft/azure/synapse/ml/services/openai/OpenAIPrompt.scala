// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.services._
import com.microsoft.azure.synapse.ml.core.contracts.HasOutputCol
import com.microsoft.azure.synapse.ml.core.spark.Functions
import com.microsoft.azure.synapse.ml.io.http.{ConcurrencyParams, HasErrorCol, HasURL}
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.StringStringMapParam
import org.apache.spark.ml.param.{BooleanParam, Param, ParamMap, ParamValidators}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, functions => F, types => T}

import scala.collection.JavaConverters._

object OpenAIPrompt extends ComplexParamsReadable[OpenAIPrompt]

class OpenAIPrompt(override val uid: String) extends Transformer
  with HasOpenAITextParams
  with HasErrorCol with HasOutputCol
  with HasURL with HasCustomCogServiceDomain with ConcurrencyParams
  with HasSubscriptionKey with HasAADToken with HasCustomAuthHeader
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

  def setPostProcessingOptions(value: Map[String, String]): this.type = set(postProcessingOptions, value)

  def setPostProcessingOptions(v: java.util.HashMap[String, String]): this.type =
    set(postProcessingOptions, v.asScala.toMap)

  val dropPrompt = new BooleanParam(
    this, "dropPrompt", "whether to drop the column of prompts after templating (when using legacy models)")

  def getDropPrompt: Boolean = $(dropPrompt)

  def setDropPrompt(value: Boolean): this.type = set(dropPrompt, value)

  val dropMessages = new BooleanParam(
    this, "dropMessages", "whether to drop the column of messages after templating (when using gpt-4 or higher)")

  def getDropMessages: Boolean = $(dropMessages)

  def setDropMessages(value: Boolean): this.type = set(dropMessages, value)

  val systemPrompt = new Param[String](
    this, "systemPrompt", "The initial system prompt to be used.")

  def getSystemPrompt: String = $(systemPrompt)

  def setSystemPrompt(value: String): this.type = set(systemPrompt, value)

  setDefault(
    postProcessing -> "",
    postProcessingOptions -> Map.empty,
    outputCol -> (this.uid + "_output"),
    errorCol -> (this.uid + "_error"),
    dropPrompt -> true,
    dropMessages -> true,
    systemPrompt -> "You are an AI Chatbot. Only respond with a completion.",
    timeout -> 360.0
  )

  override def setCustomServiceName(v: String): this.type = {
    setUrl(s"https://$v.openai.azure.com/" + urlPath.stripPrefix("/"))
  }

  private val localParamNames = Seq(
    "promptTemplate", "outputCol", "postProcessing", "postProcessingOptions", "dropPrompt", "dropMessages",
    "systemPrompt")

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
          val messageColName = df.withDerivativeCol("messages")
          val dfTemplated = df.withColumn(messageColName, createMessagesUDF(promptCol))
          val completionNamed = chatCompletion.setMessagesCol(messageColName)

          val results = completionNamed
            .transform(dfTemplated)
            .withColumn(getOutputCol,
              getParser.parse(F.element_at(F.col(completionNamed.getOutputCol).getField("choices"), 1)
                .getField("message").getField("content")))
            .drop(completionNamed.getOutputCol)

          if (getDropMessages) {
            results.drop(messageColName)
          } else {
            results
          }

        case completion: OpenAICompletion =>
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

  private def openAICompletion: OpenAIServicesBase = {

    val completion: OpenAIServicesBase =
    // use OpenAICompletion
    if (getDeploymentName != "gpt-4") {
      new OpenAICompletion()
    }
    else {
      // use OpenAIChatCompletion
      new OpenAIChatCompletion()
    }
    // apply all parameters
    extractParamMap().toSeq
      .filter(p => !localParamNames.contains(p.param.name))
      .foreach(p => completion.set(completion.getParam(p.param.name), p.value))

    completion
  }

  private def getParser: OutputParser = {
    val opts = getPostProcessingOptions

    getPostProcessing.toLowerCase match {
      case "csv" => new DelimiterParser(opts.getOrElse("delimiter", ","))
      case "json" => new JsonParser(opts.get("jsonSchema").get, Map.empty)
      case "regex" => new RegexParser(opts.get("regex").get, opts.get("regexGroup").get.toInt)
      case "" => new PassThroughParser()
      case _ => throw new IllegalArgumentException(s"Unsupported postProcessing type: '$getPostProcessing'")
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    openAICompletion match {
      case chatCompletion: OpenAIChatCompletion =>
        chatCompletion.setMessagesCol("messages")
        chatCompletion
          .transformSchema(schema)
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
