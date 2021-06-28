// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import com.microsoft.ml.spark.core.contracts.HasOutputCol
import com.microsoft.ml.spark.core.schema.DatasetExtensions
import com.microsoft.ml.spark.io.http.{HTTPParams, HasErrorCol, SimpleHTTPTransformer}
import com.microsoft.ml.spark.logging.BasicLogging
import com.microsoft.ml.spark.stages.{DropColumns, Lambda, UDFTransformer}
import org.apache.http.entity.{AbstractHttpEntity, ByteArrayEntity, ContentType, StringEntity}
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable,
  Model, NamespaceInjections, PipelineModel}
import org.apache.spark.ml.param.{ParamMap, ServiceParam}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.types.{DataType, StringType, StructType}
import spray.json.DefaultJsonProtocol._
import spray.json._

trait CustomFormRecognizer extends HasImageInput with HTTPParams with HasOutputCol
  with HasCognitiveServiceInput with ComplexParamsWritable with HasErrorCol
  with HasInternalJsonOutputParser with HasAsyncReply
  with HasSetLocation with BasicLogging

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

class AnalyzeCustomModel(override val uid: String) extends Model[AnalyzeCustomModel]
  with CustomFormRecognizer{
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

  setDefault(
    outputCol -> (this.uid + "_output"),
    errorCol -> (this.uid + "_error"))

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

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = {
    r =>
      getValueOpt(r, imageUrl)
        .map(url => new StringEntity(Map("source" -> url).toJson.compactPrint, ContentType.APPLICATION_JSON))
        .orElse(getValueOpt(r, imageBytes)
          .map(bytes => new ByteArrayEntity(bytes, ContentType.APPLICATION_OCTET_STREAM))
        ).orElse(throw new IllegalArgumentException(
        "Payload needs to contain image bytes or url. This code should not run"))
  }

  override def copy(extra: ParamMap): AnalyzeCustomModel = defaultCopy(extra)

  protected def getInternalTransformer(schema: StructType): PipelineModel = {
    val dynamicParamColName = DatasetExtensions.findUnusedColumnName("dynamic", schema)
    val badColumns = getVectorParamMap.values.toSet.diff(schema.fieldNames.toSet)
    assert(badColumns.isEmpty,
      s"Could not find dynamic columns: $badColumns in columns: ${schema.fieldNames.toSet}")

    val dynamicParamCols = getVectorParamMap.values.toList.map(col) match {
      case Nil => Seq(lit(false).alias("placeholder"))
      case l => l
    }

    val stages = Array(
      Lambda(_.withColumn(dynamicParamColName, struct(dynamicParamCols: _*))),
      new SimpleHTTPTransformer()
        .setInputCol(dynamicParamColName)
        .setOutputCol(getOutputCol)
        .setInputParser(getInternalInputParser(schema))
        .setOutputParser(getInternalOutputParser(schema))
        .setHandler(handlingFunc)
        .setConcurrency(getConcurrency)
        .setConcurrentTimeout(get(concurrentTimeout))
        .setErrorCol(getErrorCol),
      new DropColumns().setCol(dynamicParamColName)
    )

    NamespaceInjections.pipelineModel(stages)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame](
      getInternalTransformer(dataset.schema).transform(dataset)
    )
  }

  override def transformSchema(schema: StructType): StructType = {
    getInternalTransformer(schema).transformSchema(schema)
  }

  override protected def responseDataType: DataType = AnalyzeCustomModelResponse.schema
}
