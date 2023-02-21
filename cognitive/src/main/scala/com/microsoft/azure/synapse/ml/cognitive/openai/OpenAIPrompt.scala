// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive.openai

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts.HasOutputCol
import com.microsoft.azure.synapse.ml.core.spark.Functions
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import com.microsoft.azure.synapse.ml.param.StringStringMapParam
import org.apache.spark.ml.param.{DoubleParam, FloatParam, Param, ParamMap, StringArrayParam}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Dataset, functions => F, types => T}

object OpenAIPrompt extends ComplexParamsReadable[OpenAIPrompt]

class OpenAIPrompt(override val uid: String) extends OpenAICompletion {
  logClass()

  def this() = this(Identifiable.randomUID("OpenAIPrompt"))

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  val promptTemplate = new Param[String](
    this, "promptTemplate", "The prompt. supports string interpolation {col1}: {col2}.")

  def getPromptTemplate: String = $(promptTemplate)

  def setPromptTemplate(value: String): this.type = set(promptTemplate, value)

  val parsedOutputCol = new Param[String](
    this, "parsedOutputCol", "The parsed output column.")

  def getParsedOutputCol: String = $(parsedOutputCol)

  def setParsedOutputCol(value: String): this.type = set(parsedOutputCol, value)

  val postProcessing = new Param[String](
    this, "postProcessing", "Post processing options: csv, json, regex")

  def getPostProcessing: String = $(postProcessing)

  def setPostProcessing(value: String): this.type = set(postProcessing, value)

  val postProcessingOptions = new StringStringMapParam(
    this, "postProcessingOptions", "Options (default): delimiter=',', jsonSchema, regex, regexGroup=0")

  def getPostProcessingOptions: Map[String, String] = $(postProcessingOptions)

  def setPostProcessingOptions(value: Map[String, String]): this.type = set(postProcessingOptions, value)

  setDefault(postProcessing -> "", postProcessingOptions -> Map.empty)

  override def transform(dataset: Dataset[_]): DataFrame = {
    import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions._

    logTransform[DataFrame]({
      val df = dataset.toDF

      val promptColName = df.withDerivativeCol("prompt")
      setPromptCol(promptColName)

      val dfTemplated = df.withColumn(promptColName, Functions.template(getPromptTemplate))

      // apply template
      val promptedDF = super.transform(dfTemplated)

      val opts = getPostProcessingOptions

      val parser = getPostProcessing.toLowerCase match {
        case "csv" => new DelimiterParser(opts.getOrElse("delimiter", ","))
        case "json" => new JsonParser(opts.get("jsonSchema").get, Map.empty)
        case "regex" => new RegexParser(opts.get("regex").get, opts.get("regexGroup").get.toInt)
        case _ => new PassThroughParser()
      }

      promptedDF.withColumn(getParsedOutputCol,
        parser.parse(F.element_at(F.col(getOutputCol).getField("choices"), 1)
          .getField("text")))
    })
  }
}

trait OutputParser {
  def parse(responseCol: Column): Column
}

class PassThroughParser extends OutputParser {
  def parse(responseCol: Column): Column = responseCol
}

class DelimiterParser(val delimiter: String) extends OutputParser {
  def parse(responseCol: Column): Column = F.split(F.trim(responseCol), delimiter)
}

class JsonParser(val schema: String, options: Map[String, String]) extends OutputParser {
  def parse(responseCol: Column): Column = F.from_json(responseCol, schema, options)
}

class RegexParser(val regex: String, val groupIdx: Int) extends OutputParser {
  def parse(responseCol: Column): Column = F.regexp_extract(responseCol, regex, groupIdx)
}
