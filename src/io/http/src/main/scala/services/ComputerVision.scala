// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.net.URI
import java.util.concurrent.TimeoutException

import com.microsoft.ml.spark.HandlingUtils._
import com.microsoft.ml.spark.cognitive._
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{HttpEntityEnclosingRequestBase, HttpGet, HttpRequestBase}
import org.apache.http.entity.{AbstractHttpEntity, ByteArrayEntity, StringEntity}
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import spray.json.DefaultJsonProtocol._
import spray.json._
import org.apache.http.entity.ContentType

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
    { row: Row =>
      if (shouldSkip(row)) {
        None
      } else {
        val req = prepareMethod()
        req.setURI(new URI(rowToUrl(row)))
        getValueOpt(row, subscriptionKey).foreach(
          req.setHeader(subscriptionKeyHeaderName, _))
        CognitiveServiceUtils.setUA(req)
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
      .setUDF(udf(
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
  with HasCognitiveServiceInput with HasInternalJsonOutputParser {

  def this() = this(Identifiable.randomUID("OCR"))

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/vision/v2.0/ocr")

  def setDefaultLanguage(v: String): this.type = setDefaultValue(language, v)

  override def responseDataType: DataType = OCRResponse.schema
}

object RecognizeText extends ComplexParamsReadable[RecognizeText] {
  def flatten(inputCol: String, outputCol: String): UDFTransformer = {
    val fromRow = RTResponse.makeFromRowConverter
    new UDFTransformer()
      .setUDF(udf(
        { r: Row =>
          Option(r).map(fromRow).map(
            _.recognitionResult.lines.map(_.text).mkString(" "))
        },
        StringType))
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
  }
}

class RecognizeText(override val uid: String)
  extends CognitiveServicesBaseWithoutHandler(uid)
    with HasImageInput with HasCognitiveServiceInput
    with HasInternalJsonOutputParser {

  def this() = this(Identifiable.randomUID("RecognizeText"))

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

  setDefault(backoffs -> Array(100, 500, 1000), maxPollingRetries -> 1000)

  val mode = new ServiceParam[String](this, "mode",
    "If this parameter is set to 'Printed', " +
      "printed text recognition is performed. If 'Handwritten' is specified," +
      " handwriting recognition is performed",
    { spd: ServiceParamData[String] =>
      spd.data.get match {
        case Left(_) => true
        case Right(s) => Set("Printed", "Handwritten")(s)
      }
    }, isURLParam = true)

  def getMode: String = getScalarParam(mode)

  def setMode(v: String): this.type = setScalarParam(mode, v)

  def getModeCol: String = getVectorParam(mode)

  def setModeCol(v: String): this.type = setVectorParam(mode, v)

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/vision/v2.0/recognizeText")

  private def queryForResult(key: Option[String],
                             client: CloseableHttpClient,
                             location: URI): Option[HTTPResponseData] = {
    val get = new HttpGet()
    get.setURI(location)
    key.foreach(get.setHeader("Ocp-Apim-Subscription-Key", _))
    CognitiveServiceUtils.setUA(get)
    val resp = convertAndClose(sendWithRetries(client, get, getBackoffs))
    get.releaseConnection()
    val status = IOUtils.toString(resp.entity.get.content, "UTF-8")
      .parseJson.asJsObject.fields.get("status").map(_.convertTo[String])
    status.flatMap {
      case "Succeeded" | "Failed" => Some(resp)
      case "NotStarted" | "Running" => None
      case s => throw new RuntimeException(s"Received unknown status code: $s")
    }
  }

  override protected def handlingFunc(client: CloseableHttpClient,
                                      request: HTTPRequestData): HTTPResponseData = {
    val response = HandlingUtils.advanced(getBackoffs: _*)(client, request)
    if (response.statusLine.statusCode == 202) {
      val location = new URI(response.headers.filter(_.name == "Operation-Location").head.value)
      val maxTries = getMaxPollingRetries
      val delay = 100
      val key = request.headers.find(_.name == "Ocp-Apim-Subscription-Key").map(_.value)
      val it = (0 to maxTries).toIterator.flatMap { _ =>
        queryForResult(key, client, location).orElse({
          Thread.sleep(delay.toLong)
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

  override protected def responseDataType: DataType = RTResponse.schema
}

object GenerateThumbnails extends ComplexParamsReadable[GenerateThumbnails] with Serializable

class GenerateThumbnails(override val uid: String)
  extends CognitiveServicesBase(uid) with HasImageInput
    with HasWidth with HasHeight with HasSmartCropping
    with HasInternalJsonOutputParser with HasCognitiveServiceInput {

  def this() = this(Identifiable.randomUID("GenerateThumbnails"))

  override protected def getInternalOutputParser(schema: StructType): HTTPOutputParser = {
    new CustomOutputParser().setUDF({ r: HTTPResponseData => r.entity.map(_.content).orNull })
  }

  override def responseDataType: DataType = BinaryType

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/vision/v2.0/generateThumbnail")

}

object AnalyzeImage extends ComplexParamsReadable[AnalyzeImage]

class AnalyzeImage(override val uid: String)
  extends CognitiveServicesBase(uid) with HasImageInput
    with HasInternalJsonOutputParser with HasCognitiveServiceInput {

  val visualFeatures = new ServiceParam[Seq[String]](
    this, "visualFeatures", "what visual feature types to return",
    { spd: ServiceParamData[Seq[String]] =>
      spd.data.get match {
        case Left(seq) => seq.forall(Set(
          "Categories", "Tags", "Description", "Faces", "ImageType", "Color", "Adult"
        ))
        case _ => true
      }
    },
    isURLParam = true,
    toValueString = { seq => seq.mkString(",") }
  )

  def getVisualFeatures: Seq[String] = getScalarParam(visualFeatures)

  def getVisualFeaturesCol: String = getVectorParam(visualFeatures)

  def setVisualFeatures(v: Seq[String]): this.type = setScalarParam(visualFeatures, v)

  def setVisualFeaturesCol(v: String): this.type = setVectorParam(visualFeatures, v)

  val details = new ServiceParam[Seq[String]](
    this, "details", "what visual feature types to return",
    { spd: ServiceParamData[Seq[String]] =>
      spd.data.get match {
        case Left(seq) => seq.forall(Set("Celebrities", "Landmarks"))
        case _ => true
      }
    },
    isURLParam = true,
    toValueString = { seq => seq.mkString(",") }
  )

  def getDetails: Seq[String] = getScalarParam(details)

  def getDetailsCol: String = getVectorParam(details)

  def setDetails(v: Seq[String]): this.type = setScalarParam(details, v)

  def setDetailsCol(v: String): this.type = setVectorParam(details, v)

  val language = new ServiceParam[String](
    this, "language", "the language of the response (en if none given)", isURLParam = true
  )

  def getLanguage: String = getScalarParam(language)

  def getLanguageCol: String = getVectorParam(language)

  def setLanguage(v: String): this.type = setScalarParam(language, v)

  def setLanguageCol(v: String): this.type = setVectorParam(language, v)

  def setDefaultLanguage(v: String): this.type = setDefaultValue(language, v)

  setDefault(language, ServiceParamData(None, Some("en")))

  def this() = this(Identifiable.randomUID("AnalyzeImage"))

  override def responseDataType: DataType = AIResponse.schema

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/vision/v2.0/analyze")

}

object RecognizeDomainSpecificContent
  extends ComplexParamsReadable[RecognizeDomainSpecificContent] with Serializable {

  def getMostProbableCeleb(inputCol: String, outputCol: String): UDFTransformer = {
    val fromRow = DSIRResponse.makeFromRowConverter
    new UDFTransformer()
      .setUDF(udf(
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
    with HasInternalJsonOutputParser {

  def this() = this(Identifiable.randomUID("RecognizeDomainSpecificContent"))

  val model = new ServiceParam[String](this, "model",
    "the domain specific model: celebrities, landmarks")

  def setModel(v: String): this.type = setScalarParam(model, v)

  def setModelCol(v: String): this.type = setVectorParam(model, v)

  override def responseDataType: DataType = DSIRResponse.schema

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/vision/v2.0")

  override protected def prepareUrl: Row => String = { r => getUrl + s"/models/${getValue(r, model)}/analyze" }

}

object TagImage extends ComplexParamsReadable[TagImage]

class TagImage(override val uid: String)
  extends CognitiveServicesBase(uid) with HasImageInput
    with HasCognitiveServiceInput with HasInternalJsonOutputParser {

  def this() = this(Identifiable.randomUID("TagImage"))

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/vision/v2.0/tag")

  override def responseDataType: DataType = TagImagesResponse.schema

  private def validateLanguage(spd: ServiceParamData[String]): Boolean = {
    spd.data.map {
      case Left(lang) => Set("en", "es", "ja", "pt", "zh")(lang)
      case _ => true
    }.getOrElse(true)
  }

  val language = new ServiceParam[String](this, "language",
    "The desired language for output generation.",
    isRequired = false, isURLParam = true, isValid = validateLanguage)

  def setLanguage(v: String): this.type = setScalarParam(language, v)

  def setLanguageCol(v: String): this.type = setVectorParam(language, v)

  setDefault(language -> ServiceParamData(None, Some("en")))
}

object DescribeImage extends ComplexParamsReadable[DescribeImage]

class DescribeImage(override val uid: String)
  extends CognitiveServicesBase(uid) with HasCognitiveServiceInput
    with HasImageInput with HasInternalJsonOutputParser {

  def this() = this(Identifiable.randomUID("DescribeImage"))

  override def responseDataType: DataType = DescribeImageResponse.schema

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/vision/v2.0/describe")

  val maxCandidates = new ServiceParam[Int](this, "maxCandidates", "Maximum candidate descriptions to return",
    isURLParam = true
  )

  def setMaxCandidates(v: Int): this.type = setScalarParam(maxCandidates, v)

  def setMaxCandidatesCol(v: String): this.type = setVectorParam(maxCandidates, v)

  setDefault(maxCandidates, ServiceParamData(None, Some(1)))

  val language = new ServiceParam[String](this, "language", "Language of image description",
    isValid = { spd: ServiceParamData[String] =>
      spd.data.map {
        case Left(lang) => Set("en", "ja", "pt", "zh")(lang)
        case _ => true
      }.getOrElse(true)
    },
    isURLParam = true
  )

  def setLanguage(v: String): this.type = setScalarParam(language, v)

  def setLanguageCol(v: String): this.type = setVectorParam(language, v)

  setDefault(language, ServiceParamData(None, Some("en")))

}
