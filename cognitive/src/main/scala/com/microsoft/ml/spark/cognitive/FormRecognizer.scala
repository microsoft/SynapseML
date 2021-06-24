// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import com.microsoft.ml.spark.io.http.HandlingUtils.{convertAndClose, sendWithRetries}
import com.microsoft.ml.spark.io.http.{HTTPRequestData, HTTPResponseData, HandlingUtils, HeaderValues}
import com.microsoft.ml.spark.logging.BasicLogging
import com.microsoft.ml.spark.stages.UDFTransformer
import com.microsoft.ml.spark.build.BuildInfo
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{HttpGet, HttpRequestBase}
import org.apache.http.entity.{AbstractHttpEntity, ByteArrayEntity, ContentType, StringEntity}
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.param.ServiceParam
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, StringType}
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.net.URI
import java.util.concurrent.TimeoutException
import scala.concurrent.blocking

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

object AnalyzeLayout extends ComplexParamsReadable[AnalyzeLayout] {
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

}

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

object AnalyzeReceipts extends ComplexParamsReadable[AnalyzeReceipts] {
  def flattenReadResults(inputCol: String, outputCol: String): UDFTransformer = {
    val fromRow = AnalyzeReceiptsResponse.makeFromRowConverter
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

  def flattenDocumentResults(inputCol: String, outputCol: String): UDFTransformer = {
    val fromRow = AnalyzeReceiptsResponse.makeFromRowConverter
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

class AnalyzeReceipts(override val uid: String) extends FormRecognizerBase(uid) with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("AnalyzeReceipts"))

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/formrecognizer/v2.1/prebuilt/receipt/analyze")

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

  override protected def responseDataType: DataType = AnalyzeReceiptsResponse.schema

}

object AnalyzeBusinessCards extends ComplexParamsReadable[AnalyzeBusinessCards] {
  def flattenReadResults(inputCol: String, outputCol: String): UDFTransformer = {
    val fromRow = AnalyzeBusinessCardsResponse.makeFromRowConverter
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

object AnalyzeInvoices extends ComplexParamsReadable[AnalyzeInvoices] {
  def flattenReadResults(inputCol: String, outputCol: String): UDFTransformer = {
    val fromRow = AnalyzeBusinessCardsResponse.makeFromRowConverter
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

object AnalyzeIDDocuments extends ComplexParamsReadable[AnalyzeIDDocuments] {
  def flattenReadResults(inputCol: String, outputCol: String): UDFTransformer = {
    val fromRow = AnalyzeIDDocumentsResponse.makeFromRowConverter
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

  def flattenDocumentResults(inputCol: String, outputCol: String): UDFTransformer = {
    val fromRow = AnalyzeIDDocumentsResponse.makeFromRowConverter
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

object TrainCustomModel extends ComplexParamsReadable[TrainCustomModel] {
  def getModelId(inputCol: String, outputCol: String): UDFTransformer = {
    val fromRow = TrainCustomModelResponse.makeFromRowConverter
    new UDFTransformer()
      .setUDF(UDFUtils.oldUdf(
        { r: Row =>
          Option(r).map(fromRow).map(_.modelInfo.modelId).mkString("")
        },
        StringType))
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
  }
}

class TrainCustomModel(override val uid: String) extends CognitiveServicesBaseNoHandler(uid)
  with HasCognitiveServiceInput  with HasInternalJsonOutputParser with HasAsyncReply
  with HasImageInput with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("TrainCustomModel"))

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/formrecognizer/v2.1/custom/models")

  val prefix = new ServiceParam[String](this, "prefix", "A case-sensitive prefix string to filter" +
    " documents in the source path for training. For example, when using a Azure storage blob Uri, use the prefix " +
    "to restrict sub folders for training.")

  def setPrefix(v: String): this.type = setScalarParam(prefix, v)

  val includeSubFolders = new ServiceParam[Boolean](this, "includeSubFolders", "A flag to indicate " +
    "if sub folders within the set of prefix folders will also need to be included when searching for content " +
    "to be preprocessed.")

  def setIncludeSubFolders(v: Boolean): this.type = setScalarParam(includeSubFolders, v)

  setDefault(includeSubFolders -> Left(false))

  val useLabelFile = new ServiceParam[Boolean](this, "useLabelFile", "Use label file for training a model.")

  def setUseLabelFile(v: Boolean): this.type = setScalarParam(useLabelFile, v)

  setDefault(useLabelFile -> Left(false))

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = {
    r => Some(new StringEntity(Map("source" -> getValue(r, imageUrl).toJson,
      "sourceFilter" -> Map("prefix" -> getValue(r, prefix).toJson,
        "includeSubFolders" -> getValue(r, includeSubFolders).toJson).toJson,
      "useLabelFile" -> getValue(r, useLabelFile).toJson).toJson.compactPrint, ContentType.APPLICATION_JSON))
  }

  private def queryForResult(key: Option[String],
                               client: CloseableHttpClient,
                               location: URI): Option[HTTPResponseData] = {
    val get = new HttpGet()
    get.setURI(location)
    key.foreach(get.setHeader("Ocp-Apim-Subscription-Key", _))
    get.setHeader("User-Agent", s"mmlspark/${BuildInfo.version}${HeaderValues.PlatformInfo}")
    val resp = convertAndClose(sendWithRetries(client, get, getBackoffs))
    get.releaseConnection()
    val modelInfo = IOUtils.toString(resp.entity.get.content, "UTF-8").
      parseJson.asJsObject.fields.getOrElse("modelInfo", "")
    var status = ""
    modelInfo match {
      case x: JsObject => x.fields.getOrElse("status", "") match {
        case y: JsString => status = y.value
        case _ => throw new RuntimeException(s"No status found in response/modelInfo: $resp/$modelInfo")
      }
      case _ => throw new RuntimeException(s"No modelInfo found in response: $resp")
    }
    status match {
      case "ready" | "invalid" => Some(resp)
      case "creating" => None
      case s => throw new RuntimeException(s"Received unknown status code: $s")
    }
  }

  override protected def handlingFunc(client: CloseableHttpClient,
                                      request: HTTPRequestData): HTTPResponseData = {
    val response = HandlingUtils.advanced(getBackoffs: _*)(client, request)
    if (response.statusLine.statusCode == 201) {
      val location = new URI(response.headers.filter(_.name == "Location").head.value)
      val maxTries = getMaxPollingRetries
      val key = request.headers.find(_.name == "Ocp-Apim-Subscription-Key").map(_.value)
      val it = (0 to maxTries).toIterator.flatMap { _ =>
        queryForResult(key, client, location).orElse({
          blocking {
            Thread.sleep(getPollingDelay.toLong)
          }
          None
        })
      }
      if (it.hasNext) {
        it.next()
      } else {
        throw new TimeoutException(
          s"Querying for results did not complete within $maxTries tries")
      }
    } else {
      response
    }
  }

  override protected def responseDataType: DataType = TrainCustomModelResponse.schema

}

object AnalyzeCustomForm extends ComplexParamsReadable[AnalyzeCustomForm] {
  def flattenReadResults(inputCol: String, outputCol: String): UDFTransformer = {
    val fromRow = AnalyzeCustomFormResponse.makeFromRowConverter
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
    val fromRow = AnalyzeCustomFormResponse.makeFromRowConverter
    def extractTableText(pageResults: Seq[CustomFormPageResult]): String = {
      pageResults.map(_.tables.map(_.cells.map(_.text).mkString(" | ")).mkString("\n")).mkString("\n\n")
    }
    def generateKeyValuePairs(keyValuePair: KeyValuePair): String = {
      "key: " + keyValuePair.key.text + " value: " + keyValuePair.value.text
    }
    def extractKeyValuePairs(pageResults: Seq[CustomFormPageResult]): String = {
      pageResults.map(_.keyValuePairs.map(generateKeyValuePairs).mkString("\n")).mkString("\n\n")
    }
    def extractAllText(pageResults: Seq[CustomFormPageResult]): String = {
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
    val fromRow = AnalyzeCustomFormResponse.makeFromRowConverter
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

class AnalyzeCustomForm(override val uid: String) extends FormRecognizerBase(uid) with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("AnalyzeCustomForm"))

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

  val includeTextDetails = new ServiceParam[Boolean](this, "includeTextDetails",
    "Include text lines and element references in the result.", isURLParam = true)

  def  setIncludeTextDetails(v: Boolean): this.type = setScalarParam(includeTextDetails, v)

  setDefault(includeTextDetails -> Left(false))

  override protected def responseDataType: DataType = AnalyzeCustomFormResponse.schema

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
