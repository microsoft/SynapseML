// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import com.microsoft.ml.spark.io.http.{HTTPInputParser, HTTPOutputParser, HTTPParams,
  HTTPRequestData, HTTPResponseData, HasErrorCol, SimpleHTTPTransformer}
import com.microsoft.ml.spark.logging.BasicLogging
import com.microsoft.ml.spark.stages.{DropColumns, Lambda, UDFTransformer}
import com.microsoft.ml.spark.core.contracts.HasOutputCol
import com.microsoft.ml.spark.core.schema.DatasetExtensions
import org.apache.http.client.methods.{HttpGet, HttpRequestBase}
import org.apache.http.entity.{AbstractHttpEntity, ByteArrayEntity, ContentType, StringEntity}
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable,
  NamespaceInjections, PipelineModel, Transformer}
import org.apache.spark.ml.param.{ParamMap, ServiceParam}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, StringType, StructType}
import spray.json.DefaultJsonProtocol._
import spray.json._

abstract class FormRecognizerBase(override val uid: String) extends CognitiveServicesBaseNoHandler(uid)
  with HasCognitiveServiceInput  with HasInternalJsonOutputParser with HasAsyncReply
  with HasImageInput with HasSetLocation {

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = {
    r =>
      getValueOpt(r, imageUrl)
        .map(url => new StringEntity(Map("source" -> url).toJson.compactPrint, ContentType.APPLICATION_JSON))
        .orElse(getValueOpt(r, imageBytes)
          .map(bytes => new ByteArrayEntity(bytes, ContentType.APPLICATION_OCTET_STREAM))
        ).orElse(throw new IllegalArgumentException(
        "Payload needs to contain image bytes or url. This code should not run"))
  }

}

trait flattenAnalyzeResult {
  def flattenReadResults(inputCol: String, outputCol: String): UDFTransformer = {
    val fromRow = AnalyzeLayoutResponse.makeFromRowConverter
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
    val fromRow = AnalyzeLayoutResponse.makeFromRowConverter
    def extractText(pageResults: Seq[PageResult]): String = {
      pageResults.map(_.tables.map(_.cells.map(_.text).mkString(" | ")).mkString("\n")).mkString("\n\n")
    }
    new UDFTransformer()
      .setUDF(UDFUtils.oldUdf(
        { r: Row =>
          Option(r).map(fromRow).map(
            _.analyzeResult.pageResults.map(extractText).mkString(" "))
        },
        StringType))
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
  }

  def flattenDocumentResults(inputCol: String, outputCol: String): UDFTransformer = {
    val fromRow = AnalyzeBusinessCardsResponse.makeFromRowConverter
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

object AnalyzeLayout extends ComplexParamsReadable[AnalyzeLayout] with flattenAnalyzeResult

class AnalyzeLayout(override val uid: String) extends FormRecognizerBase(uid) with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("AnalyzeLayout"))

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/formrecognizer/v2.1/layout/analyze")

  val language = new ServiceParam[String](this, "language", "The BCP-47 language code of the " +
    "text in the document. Layout supports auto language identification and multilanguage documents, so only provide" +
    " a language code if you would like to force the documented to be processed as that specific language.",
    isURLParam = true)

  def setLanguage(v: String): this.type = setScalarParam(language, v)

  def setLanguageCol(v: String): this.type = setVectorParam(language, v)

  val pages = new ServiceParam[String](this, "pages", "The page selection only leveraged for" +
    " multi-page PDF and TIFF documents. Accepted input include single pages (e.g.'1, 2' -> pages 1 and 2 will be " +
    "processed), finite (e.g. '2-5' -> pages 2 to 5 will be processed) and open-ended ranges (e.g. '5-' -> all the" +
    " pages from page 5 will be processed & e.g. '-10' -> pages 1 to 10 will be processed). All of these can be mixed" +
    " together and ranges are allowed to overlap (eg. '-5, 1, 3, 5-10' - pages 1 to 10 will be processed). The" +
    " service will accept the request if it can process at least one page of the document (e.g. using '5-100' on a " +
    "5 page document is a valid input where page 5 will be processed). If no page range is provided, the entire" +
    " document will be processed.", isURLParam = true)

  def setPages(v: String): this.type = setScalarParam(pages, v)

  def setPagesCol(v: String): this.type = setVectorParam(pages, v)

  val readingOrder = new ServiceParam[String](this, "readingOrder", "Optional parameter to " +
    "specify which reading order algorithm should be applied when ordering the extract text elements. Can be either" +
    " 'basic' or 'natural'. Will default to basic if not specified", isURLParam = true)

  def setReadingOrder(v: String): this.type = setScalarParam(readingOrder, v)

  def setReadingOrderCol(v: String): this.type = setVectorParam(readingOrder, v)

  setDefault(readingOrder -> Left("basic"))

  override protected def responseDataType: DataType = AnalyzeLayoutResponse.schema

}

object AnalyzeReceipts extends ComplexParamsReadable[AnalyzeReceipts] with flattenAnalyzeResult

class AnalyzeReceipts(override val uid: String) extends FormRecognizerBase(uid) with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("AnalyzeReceipts"))

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/formrecognizer/v2.1/prebuilt/receipt/analyze")

  val includeTextDetails = new ServiceParam[Boolean](this, "includeTextDetails",
  "Include text lines and element references in the result.", isURLParam = true)

  def setIncludeTextDetails(v: Boolean): this.type = setScalarParam(includeTextDetails, v)

  setDefault(includeTextDetails -> Left(false))

  val locale = new ServiceParam[String](this, "locale", "Locale of the receipt. Supported" +
    " locales: en-AU, en-CA, en-GB, en-IN, en-US.", isURLParam = true)

  def setLocale(v: String): this.type = setScalarParam(locale, v)

  def setLocaleCol(v: String): this.type = setVectorParam(locale, v)

  val pages = new ServiceParam[String](this, "pages", "The page selection for multi-page PDF" +
    " and TIFF documents, to extract Receipt information from individual pages and a range of pages (like page 2," +
    " and pages 5-7) by entering the page numbers and ranges separated by commas (e.g. '2, 5-7'). If not set," +
    " all pages will be processed.", isURLParam = true)

  def setPages(v: String): this.type = setScalarParam(pages, v)

  def setPagesCol(v: String): this.type = setVectorParam(pages, v)

  override protected def responseDataType: DataType = AnalyzeReceiptsResponse.schema

}

object AnalyzeBusinessCards extends ComplexParamsReadable[AnalyzeBusinessCards] with flattenAnalyzeResult

class AnalyzeBusinessCards(override val uid: String) extends FormRecognizerBase(uid) with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("AnalyzeBusinessCards"))

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/formrecognizer/v2.1/prebuilt/businessCard/analyze")

  val includeTextDetails = new ServiceParam[Boolean](this, "includeTextDetails",
    "Include text lines and element references in the result.", isURLParam = true)

  def  setIncludeTextDetails(v: Boolean): this.type = setScalarParam(includeTextDetails, v)

  setDefault(includeTextDetails -> Left(false))

  val locale = new ServiceParam[String](this, "locale", "Locale of the receipt. Supported" +
    " locales: en-AU, en-CA, en-GB, en-IN, en-US.", isURLParam = true)

  def setLocale(v: String): this.type = setScalarParam(locale, v)

  def setLocaleCol(v: String): this.type = setVectorParam(locale, v)

  val pages = new ServiceParam[String](this, "pages", "The page selection for multi-page PDF" +
    " and TIFF documents, to extract Receipt information from individual pages and a range of pages (like page 2," +
    " and pages 5-7) by entering the page numbers and ranges separated by commas (e.g. '2, 5-7'). If not set," +
    " all pages will be processed.", isURLParam = true)

  def setPages(v: String): this.type = setScalarParam(pages, v)

  def setPagesCol(v: String): this.type = setVectorParam(pages, v)

  override protected def responseDataType: DataType = AnalyzeBusinessCardsResponse.schema

}

object AnalyzeInvoices extends ComplexParamsReadable[AnalyzeInvoices] with flattenAnalyzeResult

class AnalyzeInvoices(override val uid: String) extends FormRecognizerBase(uid) with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("AnalyzeInvoices"))

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/formrecognizer/v2.1/prebuilt/invoice/analyze")

  val includeTextDetails = new ServiceParam[Boolean](this, "includeTextDetails",
    "Include text lines and element references in the result.", isURLParam = true)

  def  setIncludeTextDetails(v: Boolean): this.type = setScalarParam(includeTextDetails, v)

  setDefault(includeTextDetails -> Left(false))

  val locale = new ServiceParam[String](this, "locale", "Locale of the receipt. Supported" +
    " locales: en-AU, en-CA, en-GB, en-IN, en-US.", isURLParam = true)

  def setLocale(v: String): this.type = setScalarParam(locale, v)

  def setLocaleCol(v: String): this.type = setVectorParam(locale, v)

  val pages = new ServiceParam[String](this, "pages", "The page selection for multi-page PDF" +
    " and TIFF documents, to extract Receipt information from individual pages and a range of pages (like page 2," +
    " and pages 5-7) by entering the page numbers and ranges separated by commas (e.g. '2, 5-7'). If not set," +
    " all pages will be processed.", isURLParam = true)

  def setPages(v: String): this.type = setScalarParam(pages, v)

  def setPagesCol(v: String): this.type = setVectorParam(pages, v)

  override protected def responseDataType: DataType = AnalyzeInvoicesResponse.schema

}

object AnalyzeIDDocuments extends ComplexParamsReadable[AnalyzeIDDocuments] with flattenAnalyzeResult

class AnalyzeIDDocuments(override val uid: String) extends FormRecognizerBase(uid) with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("AnalyzeIDDocuments"))

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/formrecognizer/v2.1/prebuilt/idDocument/analyze")

  val includeTextDetails = new ServiceParam[Boolean](this, "includeTextDetails",
    "Include text lines and element references in the result.", isURLParam = true)

  def  setIncludeTextDetails(v: Boolean): this.type = setScalarParam(includeTextDetails, v)

  setDefault(includeTextDetails -> Left(false))

  val pages = new ServiceParam[String](this, "pages", "The page selection for multi-page PDF" +
    " and TIFF documents, to extract Receipt information from individual pages and a range of pages (like page 2," +
    " and pages 5-7) by entering the page numbers and ranges separated by commas (e.g. '2, 5-7'). If not set," +
    " all pages will be processed.", isURLParam = true)

  def setPages(v: String): this.type = setScalarParam(pages, v)

  def setPagesCol(v: String): this.type = setVectorParam(pages, v)

  override protected def responseDataType: DataType = AnalyzeIDDocumentsResponse.schema

}

object ListCustomModels extends ComplexParamsReadable[ListCustomModels] {
  def flattenModelList(inputCol: String, outputCol: String): UDFTransformer = {
    val fromRow = ListCustomModelsResponse.makeFromRowConverter
    new UDFTransformer()
      .setUDF(UDFUtils.oldUdf(
        { r: Row =>
          Option(r).map(fromRow).map(
            _.modelList.map(_.modelId).mkString(" "))
        },
        StringType))
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
  }
}

class ListCustomModels(override val uid: String) extends CognitiveServicesBase(uid)
  with HasCognitiveServiceInput with HasInternalJsonOutputParser with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("ListCustomModels"))

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/formrecognizer/v2.1/custom/models")

  override protected def prepareMethod(): HttpRequestBase = new HttpGet()

  val op = new ServiceParam[String](this, "op",
    "Specify whether to return summary or full list of models.", isURLParam = true)

  def setOp(v: String): this.type = setScalarParam(op, v)

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = {_ => None}

  override protected def responseDataType: DataType = ListCustomModelsResponse.schema
}

object GetCustomModel extends ComplexParamsReadable[GetCustomModel]

class GetCustomModel(override val uid: String) extends CognitiveServicesBase(uid)
  with HasCognitiveServiceInput with HasInternalJsonOutputParser with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("GetCustomModel"))

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/formrecognizer/v2.1/custom/models")

  val modelId = new ServiceParam[String](this, "modelId", "Model identifier.", isRequired = true)

  def setModelId(v: String): this.type = setScalarParam(modelId, v)

  def setModelIdCol(v: String): this.type = setVectorParam(modelId, v)

  override protected def prepareUrl: Row => String = {
    val urlParams: Array[ServiceParam[Any]] =
      getUrlParams.asInstanceOf[Array[ServiceParam[Any]]];
    // This semicolon is needed to avoid argument confusion
    { row: Row =>
      val base = getUrl + s"/${getValue(row, modelId)}"
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

  override protected def prepareMethod(): HttpRequestBase = new HttpGet()

  val includeKeys = new ServiceParam[Boolean](this, "includeKeys",
    "Include list of extracted keys in model information.", isURLParam = true)

  def setIncludeKeys(v: Boolean): this.type = setScalarParam(includeKeys, v)

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = {_ => None}

  override protected def responseDataType: DataType = GetCustomModelResponse.schema
}
