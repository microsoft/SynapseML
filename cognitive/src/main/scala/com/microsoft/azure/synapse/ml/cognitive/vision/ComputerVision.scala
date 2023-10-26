// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive.vision

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.cognitive._
import com.microsoft.azure.synapse.ml.cognitive.text.HasLanguage
import com.microsoft.azure.synapse.ml.io.http.HandlingUtils._
import com.microsoft.azure.synapse.ml.io.http._
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.ServiceParam
import com.microsoft.azure.synapse.ml.stages.UDFTransformer
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{HttpEntityEnclosingRequestBase, HttpGet, HttpRequestBase}
import org.apache.http.entity.{AbstractHttpEntity, ByteArrayEntity, ContentType, StringEntity}
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.net.URI
import java.util.concurrent.TimeoutException
import scala.collection.JavaConverters._
import scala.concurrent.blocking
import scala.language.existentials

trait HasImageUrl extends HasServiceParams {
  val imageUrl = new ServiceParam[String](
    this, "imageUrl", "the url of the image to use", isRequired = false)

  def getImageUrl: String = getScalarParam(imageUrl)

  def setImageUrl(v: String): this.type = setScalarParam(imageUrl, v)

  def getImageUrlCol: String = getVectorParam(imageUrl)

  def setImageUrlCol(v: String): this.type = setVectorParam(imageUrl, v)

}

trait HasImageBytes extends HasServiceParams {

  val imageBytes = new ServiceParam[Array[Byte]](
    this, "imageBytes", "bytestream of the image to use", isRequired = false)

  def getImageBytes: Array[Byte] = getScalarParam(imageBytes)

  def setImageBytes(v: Array[Byte]): this.type = setScalarParam(imageBytes, v)

  def getImageBytesCol: String = getVectorParam(imageBytes)

  def setImageBytesCol(v: String): this.type = setVectorParam(imageBytes, v)

}

trait HasImageInput extends HasImageUrl
  with HasImageBytes with HasCognitiveServiceInput {

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = {
    r =>
      getValueOpt(r, imageUrl)
        .map(url => new StringEntity(Map("url" -> url).toJson.compactPrint, ContentType.APPLICATION_JSON))
        .orElse(getValueOpt(r, imageBytes)
          .map(bytes => new ByteArrayEntity(bytes, ContentType.APPLICATION_OCTET_STREAM))
        ).orElse(throw new IllegalArgumentException(
        "Payload needs to contain image bytes or url. This code should not run"))
  }

  override protected def inputFunc(schema: StructType): Row => Option[HttpRequestBase] = {
    val rowToUrl = prepareUrl
    val rowToEntity = prepareEntity;
    if (get(imageUrl).orElse(get(imageBytes)).isEmpty) {
      throw new IllegalArgumentException("Please set one of the" +
        " imageUrl, imageUrlCol, imageBytes, imageBytesCol parameters.")
    }

    { row: Row =>
      if (shouldSkip(row)) {
        None
      } else {
        val req = prepareMethod()
        req.setURI(new URI(rowToUrl(row)))
        addHeaders(req, getValueOpt(row, subscriptionKey), getValueOpt(row, AADToken))
        req match {
          case er: HttpEntityEnclosingRequestBase =>
            rowToEntity(row).foreach(er.setEntity)
          case _ =>
        }
        Some(req)
      }
    }

  }

  override protected def shouldSkip(row: Row): Boolean = {
    val hasUrlInput = emptyParamData(row, imageUrl)
    val hasBytesInput = emptyParamData(row, imageBytes)

    if (hasUrlInput ^ hasBytesInput) {
      super.shouldSkip(row)
    } else {
      true
    }
  }
}

trait HasDetectOrientation extends HasServiceParams {
  val detectOrientation = new ServiceParam[Boolean](
    this, "detectOrientation", "whether to detect image orientation prior to processing", isURLParam = true)

  def getDetectOrientation: Boolean = getScalarParam(detectOrientation)

  def setDetectOrientation(v: Boolean): this.type = setScalarParam(detectOrientation, v)

  def getDetectOrientationCol: String = getVectorParam(detectOrientation)

  def setDetectOrientationCol(v: String): this.type = setVectorParam(detectOrientation, v)

}

trait HasWidth extends HasServiceParams {
  val width = new ServiceParam[Int](
    this, "width", "the desired width of the image", isURLParam = true)

  def getWidth: Int = getScalarParam(width)

  def setWidth(v: Int): this.type = setScalarParam(width, v)

  def getWidthCol: String = getVectorParam(width)

  def setWidthCol(v: String): this.type = setVectorParam(width, v)

}

trait HasHeight extends HasServiceParams {
  val height = new ServiceParam[Int](
    this, "height", "the desired height of the image", isURLParam = true)

  def getHeight: Int = getScalarParam(height)

  def setHeight(v: Int): this.type = setScalarParam(height, v)

  def getHeightCol: String = getVectorParam(height)

  def setHeightCol(v: String): this.type = setVectorParam(height, v)

}

trait HasSmartCropping extends HasServiceParams {
  val smartCropping = new ServiceParam[Boolean](
    this, "smartCropping", "whether to intelligently crop the image", isURLParam = true)

  def getSmartCropping: Boolean = getScalarParam(smartCropping)

  def setSmartCropping(v: Boolean): this.type = setScalarParam(smartCropping, v)

  def getSmartCroppingCol: String = getVectorParam(smartCropping)

  def setSmartCroppingCol(v: String): this.type = setVectorParam(smartCropping, v)

}

object OCR extends ComplexParamsReadable[OCR] with Serializable {

  def flatten(inputCol: String, outputCol: String): UDFTransformer = {
    val fromRow = OCRResponse.makeFromRowConverter
    new UDFTransformer()
      .setUDF(UDFUtils.oldUdf(
        { r: Row =>
          Option(r).map(fromRow).map { resp =>
            resp.regions.map(
              _.lines.map(
                _.words.map(_.text).mkString(" ")
              ).mkString(" ")
            ).mkString(" ")
          }
        },
        StringType))
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
  }
}

class OCR(override val uid: String) extends CognitiveServicesBase(uid)
  with HasLanguage with HasImageInput with HasDetectOrientation
  with HasCognitiveServiceInput with HasInternalJsonOutputParser with HasSetLocation with SynapseMLLogging
  with HasSetLinkedService {
  logClass(FeatureNames.AiServices.Vision)

  def this() = this(Identifiable.randomUID("OCR"))

  def urlPath: String = "/vision/v2.0/ocr"

  override def responseDataType: DataType = OCRResponse.schema
}

object RecognizeText extends ComplexParamsReadable[RecognizeText] {
  def flatten(inputCol: String, outputCol: String): UDFTransformer = {
    val fromRow = RTResponse.makeFromRowConverter
    new UDFTransformer()
      .setUDF(UDFUtils.oldUdf(
        { r: Row =>
          Option(r).map(fromRow).map(
            _.recognitionResult.lines.map(_.text).mkString(" "))
        },
        StringType))
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
  }
}

trait BasicAsyncReply extends HasAsyncReply {

  override protected def queryForResult(key: Option[String],
                                        client: CloseableHttpClient,
                                        location: URI): Option[HTTPResponseData] = {
    val get = new HttpGet()
    get.setURI(location)
    key.foreach(get.setHeader("Ocp-Apim-Subscription-Key", _))
    get.setHeader("User-Agent", s"synapseml/${BuildInfo.version}${HeaderValues.PlatformInfo}")
    val resp = convertAndClose(sendWithRetries(client, get, getBackoffs))
    get.releaseConnection()
    val status = IOUtils.toString(resp.entity.get.content, "UTF-8")
      .parseJson.asJsObject.fields.get("status").map(_.convertTo[String])
    status.map(_.toLowerCase()).flatMap {
      case "succeeded" | "failed" | "partiallycompleted" => Some(resp)
      case "notstarted" | "running" => None
      case s => throw new RuntimeException(s"Received unknown status code: $s")
    }
  }

  private def getRetriesExceededData(maxTries: Int) = {
    val headers = Array[HeaderData]()
    val errorResponseString = "{\"error\": { \"code\": \"418\", \"message\": \"" +
      s"SynapseML: Querying for results did not complete within $maxTries tries" + "\"}}"
    val errorResponse = errorResponseString.getBytes()
    val entityData = EntityData(
      errorResponse,
      None,
      Some(errorResponse.length.toLong),
      Some(HeaderData("Content-Type", "application/json")),
      isChunked = false,
      isRepeatable = false,
      isStreaming = false)
    //scalastyle:off magic.number
    val statusLineData = StatusLineData(
      ProtocolVersionData("1.1", 1, 1),
      418,
      s"SynapseML: Querying for results did not complete within $maxTries tries")
    //scalastyle:on magic.number
    new HTTPResponseData(headers, Some(entityData), statusLineData, "en-us")

  }

  protected def handlingFunc(client: CloseableHttpClient,
                             request: HTTPRequestData): HTTPResponseData = {
    val response = HandlingUtils.advanced(getBackoffs: _*)(client, request)
    if (response != null && response.statusLine.statusCode == 202) {
      val originalLocation = new URI(response.headers.filter(_.name.toLowerCase() == "operation-location").head.value)
      val location = modifyPollingURI(originalLocation)
      val maxTries = getMaxPollingRetries
      val key = request.headers.find(_.name == "Ocp-Apim-Subscription-Key").map(_.value)
      blocking {
        Thread.sleep(getInitialPollingDelay.toLong)
      }
      val it = (0 to maxTries).toIterator.flatMap { i =>
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
        if (getSuppressMaxRetriesException) {
          getRetriesExceededData(maxTries)
        } else {
          throw new TimeoutException(
            s"Querying for results did not complete within $maxTries tries")
        }
      }
    } else {
      response
    }
  }

  protected def modifyPollingURI(originalURI: URI): URI = {
    originalURI
  }
}

trait HasAsyncReply extends Params {
  val backoffs: IntArrayParam = new IntArrayParam(
    this, "backoffs", "array of backoffs to use in the handler")

  /** @group getParam */
  def getBackoffs: Array[Int] = $(backoffs)

  /** @group setParam */
  def setBackoffs(value: Array[Int]): this.type = set(backoffs, value)

  val maxPollingRetries: IntParam = new IntParam(
    this, "maxPollingRetries", "number of times to poll")

  /** @group getParam */
  def getMaxPollingRetries: Int = $(maxPollingRetries)

  /** @group setParam */
  def setMaxPollingRetries(value: Int): this.type = set(maxPollingRetries, value)

  val pollingDelay: IntParam = new IntParam(
    this, "pollingDelay", "number of milliseconds to wait between polling")

  /** @group getParam */
  def getPollingDelay: Int = $(pollingDelay)

  /** @group setParam */
  def setPollingDelay(value: Int): this.type = set(pollingDelay, value)

  val initialPollingDelay: IntParam = new IntParam(
    this, "initialPollingDelay", "number of milliseconds to wait before first poll for result")

  /** @group getParam */
  def getInitialPollingDelay: Int = $(initialPollingDelay)

  /** @group setParam */
  def setInitialPollingDelay(value: Int): this.type = set(initialPollingDelay, value)

  val suppressMaxRetriesException: BooleanParam = new BooleanParam(
    this,
    "suppressMaxRetriesException",
    "set true to suppress the maxumimum retries exception and report in the error column")

  /** @group getParam */
  def getSuppressMaxRetriesException: Boolean = $(suppressMaxRetriesException)

  /** @group setParam */
  def setSuppressMaxRetriesException(value: Boolean): this.type =
    set(suppressMaxRetriesException, value)


  //scalastyle:off magic.number
  setDefault(
    backoffs -> Array(100, 500, 1000),
    maxPollingRetries -> 1000,
    pollingDelay -> 300,
    initialPollingDelay -> 300,
    suppressMaxRetriesException -> false)
  //scalastyle:on magic.number

  protected def queryForResult(key: Option[String],
                               client: CloseableHttpClient,
                               location: URI): Option[HTTPResponseData]

  protected def handlingFunc(client: CloseableHttpClient,
                             request: HTTPRequestData): HTTPResponseData
}


class RecognizeText(override val uid: String)
  extends CognitiveServicesBaseNoHandler(uid)
    with BasicAsyncReply
    with HasImageInput with HasCognitiveServiceInput
    with HasInternalJsonOutputParser with HasSetLocation with SynapseMLLogging with HasSetLinkedService {
  logClass(FeatureNames.AiServices.Vision)

  def this() = this(Identifiable.randomUID("RecognizeText"))

  val mode = new ServiceParam[String](this, "mode",
    "If this parameter is set to 'Printed', " +
      "printed text recognition is performed. If 'Handwritten' is specified," +
      " handwriting recognition is performed",
    {
      case Left(s) => Set("Printed", "Handwritten")(s)
      case Right(_) => true
    }, isURLParam = true)

  def getMode: String = getScalarParam(mode)

  def setMode(v: String): this.type = setScalarParam(mode, v)

  def getModeCol: String = getVectorParam(mode)

  def setModeCol(v: String): this.type = setVectorParam(mode, v)

  def urlPath: String = "/vision/v2.0/recognizeText"

  override protected def responseDataType: DataType = RTResponse.schema
}

object ReadImage extends ComplexParamsReadable[ReadImage] {
  def flatten(inputCol: String, outputCol: String): UDFTransformer = {
    val fromRow = ReadResponse.makeFromRowConverter
    new UDFTransformer()
      .setUDF(UDFUtils.oldUdf(
        { r: Row =>
          Option(r).map(fromRow).map(
            _.analyzeResult.readResults.map(_.lines.map(_.text).mkString(" ")).mkString(" "))
        },
        StringType))
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
  }
}

class ReadImage(override val uid: String)
  extends CognitiveServicesBaseNoHandler(uid)
    with BasicAsyncReply
    with HasImageInput with HasCognitiveServiceInput
    with HasInternalJsonOutputParser with HasSetLocation with SynapseMLLogging with HasSetLinkedService {
  logClass(FeatureNames.AiServices.Vision)

  def this() = this(Identifiable.randomUID("ReadImage"))

  val language = new ServiceParam[String](this, "language",
    "IThe BCP-47 language code of the text in the document. Currently," +
      " only English (en), Dutch (nl), French (fr), German (de), Italian (it), Portuguese (pt)," +
      " and Spanish (es) are supported." +
      " Read supports auto language identification and multilanguage documents," +
      " so only provide a language code if you would like to force the documented" +
      " to be processed as that specific language.",
    {
      case Left(s) => Set("en", "nl", "fr", "de", "it", "pt", "es")(s)
      case Right(_) => true
    }, isURLParam = true)

  def setLanguage(v: String): this.type = setScalarParam(language, v)

  def setLanguageCol(v: String): this.type = setVectorParam(language, v)

  def urlPath: String = "/vision/v3.1/read/analyze"

  override protected def responseDataType: DataType = ReadResponse.schema
}


object GenerateThumbnails extends ComplexParamsReadable[GenerateThumbnails] with Serializable

class GenerateThumbnails(override val uid: String)
  extends CognitiveServicesBase(uid) with HasImageInput
    with HasWidth with HasHeight with HasSmartCropping
    with HasCognitiveServiceInput with HasSetLocation with SynapseMLLogging
    with HasSetLinkedService {
  logClass(FeatureNames.AiServices.Vision)

  def this() = this(Identifiable.randomUID("GenerateThumbnails"))

  override protected def getInternalOutputParser(schema: StructType): HTTPOutputParser = {
    new CustomOutputParser().setUDF({ r: HTTPResponseData => r.entity.map(_.content).orNull })
  }

  def urlPath: String = "/vision/v2.0/generateThumbnail"
}

object AnalyzeImage extends ComplexParamsReadable[AnalyzeImage]

class AnalyzeImage(override val uid: String)
  extends CognitiveServicesBase(uid) with HasImageInput
    with HasInternalJsonOutputParser with HasCognitiveServiceInput with HasSetLocation with SynapseMLLogging
    with HasSetLinkedService {
  logClass(FeatureNames.AiServices.Vision)

  val visualFeatures = new ServiceParam[Seq[String]](
    this, "visualFeatures", "what visual feature types to return",
    {
      case Left(seq) => seq.forall(Set(
        "Categories", "Tags", "Description", "Faces", "ImageType", "Color", "Adult", "Brands", "Objects"
      ))
      case _ => true

    },
    isURLParam = true,
    toValueString = { seq => seq.mkString(",") }
  )

  def getVisualFeatures: Seq[String] = getScalarParam(visualFeatures)

  def getVisualFeaturesCol: String = getVectorParam(visualFeatures)

  def setVisualFeatures(v: Seq[String]): this.type = setScalarParam(visualFeatures, v)

  def setVisualFeatures(v: java.util.ArrayList[String]): this.type = setVisualFeatures(v.asScala)

  def setVisualFeaturesCol(v: String): this.type = setVectorParam(visualFeatures, v)

  val details = new ServiceParam[Seq[String]](
    this, "details", "what visual feature types to return",
    { _ => true },
    isURLParam = true,
    toValueString = { seq => seq.mkString(",") }
  )

  def getDetails: Seq[String] = getScalarParam(details)

  def getDetailsCol: String = getVectorParam(details)

  def setDetails(v: Seq[String]): this.type = setScalarParam(details, v)

  def setDetailsCol(v: String): this.type = setVectorParam(details, v)

  val descriptionExclude = new ServiceParam[Seq[String]](
    this, "descriptionExclude", "Whether to exclude certain parts of the model in the description",
    { _ => true },
    isURLParam = true,
    toValueString = { seq => seq.mkString(",") }
  )

  def getDescriptionExclude: Seq[String] = getScalarParam(descriptionExclude)

  def getDescriptionExcludeCol: String = getVectorParam(descriptionExclude)

  def setDescriptionExclude(v: Seq[String]): this.type = setScalarParam(descriptionExclude, v)

  def setDescriptionExcludeCol(v: String): this.type = setVectorParam(descriptionExclude, v)

  val language = new ServiceParam[String](
    this, "language", "the language of the response (en if none given)", isURLParam = true
  )

  def getLanguage: String = getScalarParam(language)

  def getLanguageCol: String = getVectorParam(language)

  def setLanguage(v: String): this.type = setScalarParam(language, v)

  def setLanguageCol(v: String): this.type = setVectorParam(language, v)

  def this() = this(Identifiable.randomUID("AnalyzeImage"))

  override def responseDataType: DataType = AIResponse.schema

  def urlPath: String = "/vision/v2.0/analyze"
}

object RecognizeDomainSpecificContent
  extends ComplexParamsReadable[RecognizeDomainSpecificContent] with Serializable {

  def getMostProbableCeleb(inputCol: String, outputCol: String): UDFTransformer = {
    val fromRow = DSIRResponse.makeFromRowConverter
    new UDFTransformer()
      .setUDF(UDFUtils.oldUdf(
        { r: Row =>
          Option(r).map { r =>
            fromRow(r).result.celebrities.flatMap {
              case Seq() => None
              case celebs => Some(celebs.maxBy(_.confidence).name)
            }
          }.orNull
        },
        StringType))
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
  }
}

class RecognizeDomainSpecificContent(override val uid: String)
  extends CognitiveServicesBase(uid) with HasImageInput
    with HasServiceParams with HasCognitiveServiceInput
    with HasInternalJsonOutputParser with HasSetLocation with SynapseMLLogging with HasSetLinkedService {
  logClass(FeatureNames.AiServices.Vision)

  def this() = this(Identifiable.randomUID("RecognizeDomainSpecificContent"))

  val model = new ServiceParam[String](this, "model",
    "the domain specific model: celebrities, landmarks")

  def setModel(v: String): this.type = setScalarParam(model, v)

  def setModelCol(v: String): this.type = setVectorParam(model, v)

  override def responseDataType: DataType = DSIRResponse.schema

  def urlPath: String = "/vision/v2.0"

  override protected def prepareUrl: Row => String = { r => getUrl + s"/models/${getValue(r, model)}/analyze" }

}

object TagImage extends ComplexParamsReadable[TagImage]

class TagImage(override val uid: String)
  extends CognitiveServicesBase(uid) with HasImageInput
    with HasCognitiveServiceInput with HasInternalJsonOutputParser with HasSetLocation with SynapseMLLogging
    with HasSetLinkedService {
  logClass(FeatureNames.AiServices.Vision)

  def this() = this(Identifiable.randomUID("TagImage"))

  def urlPath: String = "/vision/v2.0/tag"

  override def responseDataType: DataType = TagImagesResponse.schema

  val language = new ServiceParam[String](this, "language",
    "The desired language for output generation.",
    isRequired = false, isURLParam = true,
    isValid = {
      case Left(lang) => Set("en", "es", "ja", "pt", "zh")(lang)
      case _ => true
    })

  def setLanguage(v: String): this.type = setScalarParam(language, v)

  def setLanguageCol(v: String): this.type = setVectorParam(language, v)

}

object DescribeImage extends ComplexParamsReadable[DescribeImage]

class DescribeImage(override val uid: String)
  extends CognitiveServicesBase(uid) with HasCognitiveServiceInput
    with HasImageInput with HasInternalJsonOutputParser with HasSetLocation with SynapseMLLogging
    with HasSetLinkedService {
  logClass(FeatureNames.AiServices.Vision)

  def this() = this(Identifiable.randomUID("DescribeImage"))

  override def responseDataType: DataType = DescribeImageResponse.schema

  def urlPath: String = "/vision/v2.0/describe"

  val maxCandidates = new ServiceParam[Int](this, "maxCandidates", "Maximum candidate descriptions to return",
    isURLParam = true
  )

  def setMaxCandidates(v: Int): this.type = setScalarParam(maxCandidates, v)

  def setMaxCandidatesCol(v: String): this.type = setVectorParam(maxCandidates, v)


  val language = new ServiceParam[String](this, "language", "Language of image description",
    isValid = {
      case Left(lang) => Set("en", "ja", "pt", "zh")(lang)
      case _ => true
    },
    isURLParam = true
  )

  def setLanguage(v: String): this.type = setScalarParam(language, v)

  def setLanguageCol(v: String): this.type = setVectorParam(language, v)

}
