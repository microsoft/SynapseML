// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import com.microsoft.ml.spark.logging.BasicLogging
import com.microsoft.ml.spark.stages.UDFTransformer
import org.apache.http.client.methods.{HttpGet, HttpRequestBase}
import org.apache.http.entity.{AbstractHttpEntity, ByteArrayEntity, ContentType, StringEntity}
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.param.ServiceParam
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, StringType}
import spray.json.DefaultJsonProtocol._
import spray.json._

abstract class FormRecognizerBase(override val uid: String) extends CognitiveServicesBaseNoHandler(uid)
  with HasCognitiveServiceInput with HasInternalJsonOutputParser with HasAsyncReply
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

trait HasPages extends HasServiceParams {

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

}

trait HasTextDetails extends HasServiceParams {
  val includeTextDetails = new ServiceParam[Boolean](this, "includeTextDetails",
    "Include text lines and element references in the result.", isURLParam = true)

  def setIncludeTextDetails(v: Boolean): this.type = setScalarParam(includeTextDetails, v)

  def setIncludeTextDetailsCol(v: String): this.type = setVectorParam(includeTextDetails, v)

}

trait HasModelID extends HasServiceParams {
  val modelId = new ServiceParam[String](this, "modelId", "Model identifier.", isRequired = true)

  def setModelId(v: String): this.type = setScalarParam(modelId, v)

  def setModelIdCol(v: String): this.type = setVectorParam(modelId, v)

}

trait HasLocale extends HasServiceParams {
  val locale = new ServiceParam[String](this, "locale", "Locale of the receipt. Supported" +
    " locales: en-AU, en-CA, en-GB, en-IN, en-US.", {
    case Left(_) => true
    case Right(s) => Set("en-AU", "en-CA", "en-GB", "en-IN", "en-US")(s)
  }, isURLParam = true)

  def setLocale(v: String): this.type = setScalarParam(locale, v)

  def setLocaleCol(v: String): this.type = setVectorParam(locale, v)

}

object FormsFlatteners {
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

object CustomFormsFlatteners {
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

object AnalyzeLayout extends ComplexParamsReadable[AnalyzeLayout]

class AnalyzeLayout(override val uid: String) extends FormRecognizerBase(uid)
  with BasicLogging with HasPages {
  logClass()

  def this() = this(Identifiable.randomUID("AnalyzeLayout"))

  def urlPath: String = "/formrecognizer/v2.1/layout/analyze"

  val language = new ServiceParam[String](this, "language", "The BCP-47 language code of the " +
    "text in the document. Layout supports auto language identification and multilanguage documents, so only provide" +
    " a language code if you would like to force the documented to be processed as that specific language.",
    isURLParam = true)

  def setLanguage(v: String): this.type = setScalarParam(language, v)

  def setLanguageCol(v: String): this.type = setVectorParam(language, v)

  val readingOrder = new ServiceParam[String](this, "readingOrder", "Optional parameter to " +
    "specify which reading order algorithm should be applied when ordering the extract text elements. Can be either" +
    " 'basic' or 'natural'. Will default to basic if not specified", isURLParam = true)

  def setReadingOrder(v: String): this.type = setScalarParam(readingOrder, v)

  def setReadingOrderCol(v: String): this.type = setVectorParam(readingOrder, v)

  setDefault(readingOrder -> Left("basic"))

  override protected def responseDataType: DataType = AnalyzeLayoutResponse.schema

}

object AnalyzeReceipts extends ComplexParamsReadable[AnalyzeReceipts]

class AnalyzeReceipts(override val uid: String) extends FormRecognizerBase(uid)
  with BasicLogging with HasPages with HasTextDetails with HasLocale {
  logClass()

  def this() = this(Identifiable.randomUID("AnalyzeReceipts"))

  def urlPath: String = "/formrecognizer/v2.1/prebuilt/receipt/analyze"

  override protected def responseDataType: DataType = AnalyzeReceiptsResponse.schema

}

object AnalyzeBusinessCards extends ComplexParamsReadable[AnalyzeBusinessCards]

class AnalyzeBusinessCards(override val uid: String) extends FormRecognizerBase(uid)
  with BasicLogging with HasPages with HasTextDetails with HasLocale {
  logClass()

  def this() = this(Identifiable.randomUID("AnalyzeBusinessCards"))

  def urlPath: String = "/formrecognizer/v2.1/prebuilt/businessCard/analyze"

  override protected def responseDataType: DataType = AnalyzeBusinessCardsResponse.schema

}

object AnalyzeInvoices extends ComplexParamsReadable[AnalyzeInvoices]

class AnalyzeInvoices(override val uid: String) extends FormRecognizerBase(uid)
  with BasicLogging with HasPages with HasTextDetails with HasLocale {
  logClass()

  def this() = this(Identifiable.randomUID("AnalyzeInvoices"))

  def urlPath: String = "/formrecognizer/v2.1/prebuilt/invoice/analyze"

  override protected def responseDataType: DataType = AnalyzeInvoicesResponse.schema

}

object AnalyzeIDDocuments extends ComplexParamsReadable[AnalyzeIDDocuments]

class AnalyzeIDDocuments(override val uid: String) extends FormRecognizerBase(uid)
  with BasicLogging with HasPages with HasTextDetails {
  logClass()

  def this() = this(Identifiable.randomUID("AnalyzeIDDocuments"))

  def urlPath: String = "formrecognizer/v2.1/prebuilt/idDocument/analyze"

  override protected def responseDataType: DataType = AnalyzeIDDocumentsResponse.schema

}

object ListCustomModels extends ComplexParamsReadable[ListCustomModels]

class ListCustomModels(override val uid: String) extends CognitiveServicesBase(uid)
  with HasCognitiveServiceInput with HasInternalJsonOutputParser
  with HasSetLocation with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("ListCustomModels"))

  def urlPath: String = "formrecognizer/v2.1/custom/models"

  override protected def prepareMethod(): HttpRequestBase = new HttpGet()

  val op = new ServiceParam[String](this, "op",
    "Specify whether to return summary or full list of models.", isURLParam = true)

  def setOp(v: String): this.type = setScalarParam(op, v)

  def setOpCol(v: String): this.type = setVectorParam(op, v)

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = { _ => None }

  override protected def responseDataType: DataType = ListCustomModelsResponse.schema
}

object GetCustomModel extends ComplexParamsReadable[GetCustomModel]

class GetCustomModel(override val uid: String) extends CognitiveServicesBase(uid)
  with HasCognitiveServiceInput with HasInternalJsonOutputParser
  with HasSetLocation with BasicLogging with HasModelID {
  logClass()

  def this() = this(Identifiable.randomUID("GetCustomModel"))

  def urlPath: String = "formrecognizer/v2.1/custom/models"

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

  def setIncludeKeysCol(v: String): this.type = setVectorParam(includeKeys, v)

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = { _ => None }

  override protected def responseDataType: DataType = GetCustomModelResponse.schema
}

object AnalyzeCustomModel extends ComplexParamsReadable[AnalyzeCustomModel]

class AnalyzeCustomModel(override val uid: String) extends FormRecognizerBase(uid)
  with BasicLogging with HasTextDetails with HasModelID {
  logClass()

  def this() = this(Identifiable.randomUID("AnalyzeCustomModel"))

  def urlPath: String = "formrecognizer/v2.1/custom/models"

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

