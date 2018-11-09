// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.net.URI
import java.util.concurrent.TimeoutException

import com.microsoft.ml.spark.HandlingUtils._
import com.microsoft.ml.spark.cognitive._
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.HttpGet
import org.apache.http.entity.{AbstractHttpEntity, StringEntity}
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.spark.ml.param.{ServiceParam, ServiceParamData}
import org.apache.spark.ml.util._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import spray.json.DefaultJsonProtocol._
import spray.json._

trait HasImageUrl extends HasServiceParams {
  val imageUrl = new ServiceParam[String](
    this, "imageUrl", "the url of the image to use", isRequired = true)

  def getImageUrl: String = getScalarParam(imageUrl)

  def setImageUrl(v: String): this.type = setScalarParam(imageUrl, v)

  def getImageUrlCol: String = getVectorParam(imageUrl)

  def setImageUrlCol(v: String): this.type = setVectorParam(imageUrl, v)

}

trait HasDetectOrientation extends HasServiceParams {
  val detectOrientation = new ServiceParam[Boolean](
    this, "detectOrientation", "whether to detect image orientation prior to processing")

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
  with HasLanguage with HasImageUrl with HasDetectOrientation
  with HasCognitiveServiceInput with HasInternalJsonOutputParser {

  def this() = this(Identifiable.randomUID("OCR"))

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/vision/v2.0/ocr")

  def setDefaultLanguage(v: String): this.type = setDefaultValue(language, v)

  override def prepareEntity: Row => Option[AbstractHttpEntity] = {row =>
    val body: Map[String, String] = List(
      getValueOpt(row, detectOrientation).map(v => "detectOrientation" -> v.toString),
      Some("url" -> getValue(row, imageUrl)),
      getValueOpt(row, language).map(lang => "language" -> lang)
    ).flatten.toMap
    Some(new StringEntity(body.toJson.compactPrint))
  }

  override def responseDataType: DataType = OCRResponse.schema
}

class RecognizeText(override val uid: String)
  extends CognitiveServicesBaseWithoutHandler(uid)
    with HasImageUrl with HasCognitiveServiceInput
    with HasInternalJsonOutputParser {

  def this() = this(Identifiable.randomUID("RecognizeText"))

  val mode = new ServiceParam[String](this, "mode",
    "If this parameter is set to 'Printed', " +
      "printed text recognition is performed. If 'Handwritten' is specified," +
      " handwriting recognition is performed",
    {spd: ServiceParamData[String] => spd.data.get match {
      case Left(_) => true
      case Right(s) => Set("Printed", "Handwritten")(s)
    }}, isURLParam = true)

  def getMode: String = getScalarParam(mode)

  def setMode(v: String): this.type = setScalarParam(mode, v)

  def getModeCol: String = getVectorParam(mode)

  def setModeCol(v: String): this.type = setVectorParam(mode, v)

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/vision/v2.0/recognizeText")

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] =
    { r => Some(new StringEntity(Map("url" -> getValue(r, imageUrl)).toJson.compactPrint))}

  private def queryForResult(key: Option[String],
                             client: CloseableHttpClient,
                             location: URI): Option[HTTPResponseData] = {
    val get = new HttpGet()
    get.setURI(location)
    key.foreach(get.setHeader("Ocp-Apim-Subscription-Key", _))
    CognitiveServiceUtils.setUA(get)
    val resp = convertAndClose(sendWithRetries(client, get, Array(100)))
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
    val response = HandlingUtils.advanced(100)(client, request)
    if (response.statusLine.statusCode == 202) {
      val location = new URI(response.headers.filter(_.name == "Operation-Location").head.value)
      val maxTries = 1000
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

object GenerateThumbnails extends ComplexParamsReadable[GenerateThumbnails] with Serializable

class GenerateThumbnails(override val uid: String)
  extends CognitiveServicesBase(uid) with HasImageUrl
    with HasWidth with HasHeight with HasSmartCropping
    with HasInternalJsonOutputParser with HasCognitiveServiceInput {

  def this() = this(Identifiable.randomUID("GenerateThumbnails"))

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] =
    { r => Some(new StringEntity(Map("url" -> getValue(r, imageUrl)).toJson.compactPrint))}

  override protected def getInternalOutputParser(schema: StructType): HTTPOutputParser = {
    new CustomOutputParser().setUDF({ r: HTTPResponseData => r.entity.map(_.content).orNull })
  }

  override def responseDataType: DataType = BinaryType

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/vision/v2.0/generateThumbnail")

}

class AnalyzeImage(override val uid: String)
  extends CognitiveServicesBase(uid) with HasImageUrl
    with HasInternalJsonOutputParser with HasCognitiveServiceInput {

  val visualFeatures = new ServiceParam[Seq[String]](
    this, "visualFeatures", "what visual feature types to return",
    {spd:ServiceParamData[Seq[String]] => spd.data.get match {
      case Left(seq) => seq.forall(Set(
        "Categories", "Tags", "Description", "Faces", "ImageType", "Color", "Adult"
      ))
      case _ => true
    }},
    isURLParam = true,
    toValueString = {seq => seq.mkString(",")}
  )

  def getVisualFeatures: Seq[String] = getScalarParam(visualFeatures)

  def getVisualFeaturesCol: String = getVectorParam(visualFeatures)

  def setVisualFeatures(v: Seq[String]): this.type = setScalarParam(visualFeatures, v)

  def setVisualFeaturesCol(v: String): this.type = setVectorParam(visualFeatures, v)

  val details = new ServiceParam[Seq[String]](
    this, "details", "what visual feature types to return",
    {spd: ServiceParamData[Seq[String]] => spd.data.get match {
      case Left(seq) => seq.forall(Set("Celebrities", "Landmarks"))
      case _ => true
    }},
    isURLParam = true,
    toValueString = {seq => seq.mkString(",")}
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

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] =
    { r => Some(new StringEntity(Map("url" -> getValue(r, imageUrl)).toJson.compactPrint))}
}

object AnalyzeImage extends ComplexParamsReadable[AnalyzeImage]

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
  extends CognitiveServicesBase(uid) with HasImageUrl with HasServiceParams
    with HasCognitiveServiceInput with HasInternalJsonOutputParser {

  def this() = this(Identifiable.randomUID("RecognizeDomainSpecificContent"))

  val model = new ServiceParam[String](this, "model",
    "the domain specific model: celebrities, landmarks")

  def setModel(v: String): this.type = setScalarParam(model, v)

  def setModelCol(v: String): this.type = setVectorParam(model, v)

  override def responseDataType: DataType = DSIRResponse.schema

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/vision/v2.0")

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] =
    { r => Some(new StringEntity(Map("url" -> getValue(r, imageUrl)).toJson.compactPrint))}

  override protected def prepareUrl: Row => String =
  {r => getUrl + s"/models/${getValue(r, model)}/analyze"}

}

object DescribeImage extends ComplexParamsReadable[DescribeImage]

class DescribeImage(override val uid: String)
  extends CognitiveServicesBase(uid) with HasCognitiveServiceInput
    with HasImageUrl with HasInternalJsonOutputParser {

  def this() = this(Identifiable.randomUID("DescribeImage"))

  override def responseDataType: DataType = DescribeImageResponse.schema

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/vision/v2.0/describe")

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] =
  { r => Some(new StringEntity(Map("url" -> getValue(r, imageUrl)).toJson.compactPrint))}

  val maxCandidates = new ServiceParam[String](this, "maxCandidates", "Maximum candidate descriptions to return",
    isURLParam = true
    )

  def setmaxCandidates(v: String): this.type = setScalarParam(maxCandidates, v)

  def setmaxCandidatesCol(v: String): this.type = setVectorParam(maxCandidates, v)

  setDefault(maxCandidates, ServiceParamData(None, Some("1")))

  val language = new ServiceParam[String](this, "language", "Language of image description",
    isValid = {spd:ServiceParamData[String] => spd.data.map {
      case Left(lang) => Set("en", "ja", "pt", "zh")(lang)
      case _ => true
    }.getOrElse(true)},
    isURLParam = true
    )

  def setLanguage(v: String): this.type = setScalarParam(language, v)

  def setLanguageCol(v: String): this.type = setVectorParam(language, v)

  setDefault(language, ServiceParamData(None, Some("en")))

}
