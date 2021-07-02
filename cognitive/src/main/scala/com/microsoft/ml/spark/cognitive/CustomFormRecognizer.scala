// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import com.microsoft.ml.spark.logging.BasicLogging
import com.microsoft.ml.spark.stages.UDFTransformer
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.param.ServiceParam
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, StringType}
import spray.json.DefaultJsonProtocol._

object AnalyzeCustomModel extends ComplexParamsReadable[AnalyzeCustomModel] {
  def flattenReadResults(inputCol: String, outputCol: String): UDFTransformer = {
    val fromRow = AnalyzeCustomModelResponse.makeFromRowConverter
    def extractText(lines: Array[ReadLine]): String = {
      lines.map(_.text).mkString(" ")
    }
    new UDFTransformer()
      .setUDF(UDFUtils.oldUdf(
        { r: Row =>
          Option(r).map(fromRow).map(
            _.analyzeResult.readResults.map(_.lines.map(extractText).mkString("")).mkString(" ")).mkString("")
        },
        StringType))
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
  }

  def flattenPageResults(inputCol: String, outputCol: String): UDFTransformer = {
    val fromRow = AnalyzeCustomModelResponse.makeFromRowConverter
    def extractTableText(pageResults: Seq[AnalyzeCustomModelPageResult]): String = {
      pageResults.map(_.tables.map(_.cells.map(_.text).mkString(" | ")).mkString("\n")).mkString("\n\n")
    }
    def generateKeyValuePairs(keyValuePair: KeyValuePair): String = {
      "key: " + keyValuePair.key.text + " value: " + keyValuePair.value.text
    }
    def extractKeyValuePairs(pageResults: Seq[AnalyzeCustomModelPageResult]): String = {
      pageResults.map(_.keyValuePairs.map(generateKeyValuePairs).mkString("\n")).mkString("\n\n")
    }
    def extractAllText(pageResults: Seq[AnalyzeCustomModelPageResult]): String = {
      "KeyValuePairs: " + extractKeyValuePairs(pageResults) + "\n\n\n" + "Tables: " + extractTableText(pageResults)
    }
    new UDFTransformer()
      .setUDF(UDFUtils.oldUdf(
        { r: Row =>
          Option(r).map(fromRow).map(
            _.analyzeResult.pageResults.map(extractAllText).mkString(" "))
        },
        StringType))
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
  }

  def flattenDocumentResults(inputCol: String, outputCol: String): UDFTransformer = {
    val fromRow = AnalyzeCustomModelResponse.makeFromRowConverter
    def extractFields(documentResults: Seq[DocumentResult]): String = {
      documentResults.map(_.fields).mkString("\n")
    }
    new UDFTransformer()
      .setUDF(UDFUtils.oldUdf(
        { r: Row =>
          Option(r).map(fromRow).map(
            _.analyzeResult.documentResults.map(extractFields).mkString("")).mkString("")
        },
        StringType))
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
  }
}

class AnalyzeCustomModel(override val uid: String) extends FormRecognizerBase(uid) with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("AnalyzeCustomModel"))

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/formrecognizer/v2.1/custom/models")

  val modelId = new ServiceParam[String](this, "modelId", "Model identifier.", isRequired = true)

  def setModelId(v: String): this.type = setScalarParam(modelId, v)

  def setModelIdCol(v: String): this.type = setVectorParam(modelId, v)

  def getModelId: String = getScalarParam(modelId)

  def getModelIdCol: String = getVectorParam(modelId)

  val includeTextDetails = new ServiceParam[Boolean](this, "includeTextDetails",
    "Include text lines and element references in the result.", isURLParam = true)

  def  setIncludeTextDetails(v: Boolean): this.type = setScalarParam(includeTextDetails, v)

  setDefault(includeTextDetails -> Left(false))

  override protected def prepareUrl: Row => String = {
    val urlParams: Array[ServiceParam[Any]] =
      getUrlParams.asInstanceOf[Array[ServiceParam[Any]]];
    // This semicolon is needed to avoid argument confusion
    { row: Row =>
      val base = getUrl + s"/${getValue(row, modelId)}/analyze"
      val appended = if (!urlParams.isEmpty) {
        "?" + URLEncodingUtils.format(urlParams.flatMap(p =>
          getValueOpt(row, p).map(v => p.name -> p.toValueString(v))
        ).toMap)
      } else {
        ""
      }
      base + appended
    }
  }

  override protected def responseDataType: DataType = AnalyzeCustomModelResponse.schema
}
