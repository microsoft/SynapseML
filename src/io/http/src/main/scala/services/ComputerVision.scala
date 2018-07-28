// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.net.URI
import java.util.concurrent.TimeoutException

import com.microsoft.ml.spark.HandlingUtils._
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{HttpGet, HttpPost, HttpRequestBase}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.spark.ml.param.VectorizableParam
import org.apache.spark.ml.util._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import spray.json.DefaultJsonProtocol._
import spray.json._

trait HasImageUrl extends HasVectorizableParams {
  val imageUrl = new VectorizableParam[String](
    this, "imageUrl", "the url of the image to use")

  def getImageUrl: String = getScalarParam(imageUrl)

  def setImageUrl(v: String): this.type = setScalarParam(imageUrl, v)

  def getImageUrlCol: String = getVectorParam(imageUrl)

  def setImageUrlCol(v: String): this.type = setVectorParam(imageUrl, v)

}

trait HasDetectOrientation extends HasVectorizableParams {
  val detectOrientation = new VectorizableParam[Boolean](
    this, "detectOrientation", "whether to detect image orientation prior to processing")

  def getDetectOrientation: Boolean = getScalarParam(detectOrientation)

  def setDetectOrientation(v: Boolean): this.type = setScalarParam(detectOrientation, v)

  def getDetectOrientationCol: String = getVectorParam(detectOrientation)

  def setDetectOrientationCol(v: String): this.type = setVectorParam(detectOrientation, v)

}

trait HasWidth extends HasVectorizableParams {
  val width = new VectorizableParam[Int](
    this, "width", "the desired width of the image")

  def getWidth: Int = getScalarParam(width)

  def setWidth(v: Int): this.type = setScalarParam(width, v)

  def getWidthCol: String = getVectorParam(width)

  def setWidthCol(v: String): this.type = setVectorParam(width, v)

}

trait HasHeight extends HasVectorizableParams {
  val height = new VectorizableParam[Int](
    this, "height", "the desired height of the image")

  def getHeight: Int = getScalarParam(height)

  def setHeight(v: Int): this.type = setScalarParam(height, v)

  def getHeightCol: String = getVectorParam(height)

  def setHeightCol(v: String): this.type = setVectorParam(height, v)

}

trait HasSmartCropping extends HasVectorizableParams {
  val smartCropping = new VectorizableParam[Boolean](
    this, "smartCropping", "whether to intelligently crop the image")

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
  with HasInternalCustomInputParser with HasInternalJsonOutputParser {

  def this() = this(Identifiable.randomUID("OCR"))

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/vision/v2.0/ocr")

  def inputFunc(schema: StructType): Row => HttpPost = { row: Row =>
    val post = new HttpPost(getUrl)
    getValueOpt(row, subscriptionKey).foreach(post.setHeader("Ocp-Apim-Subscription-Key", _))
    post.setHeader("Content-Type", "application/json")
    val body: Map[String, String] = List(
      getValueOpt(row, detectOrientation).map(v => "detectOrientation" -> v.toString),
      Some("url" -> getValue(row, imageUrl)),
      getValueOpt(row, language).map(v => "language" -> v)
    ).flatten.toMap
    post.setEntity(new StringEntity(body.toJson.compactPrint))
    post
  }

  override def responseDataType: DataType = OCRResponse.schema
}

class RecognizeText(override val uid: String)
  extends CognitiveServicesBaseWithoutHandler(uid)
    with HasImageUrl with HasInternalCustomInputParser
    with HasInternalJsonOutputParser {

  def this() = this(Identifiable.randomUID("RecognizeText"))

  val mode = new VectorizableParam[String](this, "mode",
    "If this parameter is set to \"Printed\", " +
      "printed text recognition is performed. If \"Handwritten\" is specified," +
      " handwriting recognition is performed",
    {
      case Left(_) => true
      case Right(s) => Set("Printed", "Handwritten")(s)
    })

  def getMode: String = getScalarParam(mode)

  def setMode(v: String): this.type = setScalarParam(mode, v)

  def getModeCol: String = getVectorParam(mode)

  def setModeCol(v: String): this.type = setVectorParam(mode, v)

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/vision/v2.0/recognizeText")

  def inputFunc(schema: StructType): Row => HttpPost = { row: Row =>
    val post = new HttpPost(getUrl + "?mode=" + getValue(row, mode))
    getValueOpt(row, subscriptionKey).foreach(post.setHeader("Ocp-Apim-Subscription-Key", _))
    post.setHeader("Content-Type", "application/json")
    val body: Map[String, String] = Map(
      "url" -> getValue(row, imageUrl)
    )
    post.setEntity(new StringEntity(body.toJson.compactPrint))
    post
  }

  private def queryForResult(key: Option[String],
                             client: CloseableHttpClient,
                             location: URI): Option[HTTPResponseData] = {
    val get = new HttpGet()
    get.setURI(location)
    key.foreach(get.setHeader("Ocp-Apim-Subscription-Key", _))
    val resp = convertAndClose(sendWithRetries(client, get, Array(100)))
    get.releaseConnection()
    val status = IOUtils.toString(resp.entity.content, "UTF-8")
      .parseJson.asJsObject.fields("status").convertTo[String]
    status match {
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
          Thread.sleep(delay.toLong);
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
    with HasInternalJsonOutputParser with HasInternalCustomInputParser {

  def this() = this(Identifiable.randomUID("GenerateThumbnails"))

  def inputFunc(schema: StructType): Row => HttpPost = { row: Row =>
    val fullURL = getUrl + "?" + URLEncodingUtils
      .format(List(
        Some("width" -> getValue(row, width).toString),
        Some("height" -> getValue(row, height).toString),
        getValueOpt(row, smartCropping).map("smartCropping" -> _.toString)
      ).flatten.toMap)
    val post = new HttpPost(fullURL)
    getValueOpt(row, subscriptionKey).foreach(post.setHeader("Ocp-Apim-Subscription-Key", _))
    post.setHeader("Content-Type", "application/json")
    val body = Map("url" -> getValue(row, imageUrl))
    post.setEntity(new StringEntity(body.toJson.compactPrint))
    post
  }

  override protected def getInternalOutputParser(schema: StructType): HTTPOutputParser = {
    new CustomOutputParser().setUDF({ r: HTTPResponseData => r.entity.content })
  }

  override def responseDataType: DataType = BinaryType

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/vision/v2.0/generateThumbnail")

}

class AnalyzeImage(override val uid: String)
  extends CognitiveServicesBase(uid) with HasImageUrl
    with HasInternalJsonOutputParser with HasInternalCustomInputParser {

  val visualFeatures = new VectorizableParam[Seq[String]](
    this, "visualFeatures", "what visual feature types to return",
    {
      case Left(seq) => seq.forall(Set(
        "Categories", "Tags", "Description", "Faces", "ImageType", "Color", "Adult"
      ))
      case _ => true
    }
  )

  def getVisualFeatures: Seq[String] = getScalarParam(visualFeatures)

  def getVisualFeaturesCol: String = getVectorParam(visualFeatures)

  def setVisualFeatures(v: Seq[String]): this.type = setScalarParam(visualFeatures, v)

  def setVisualFeaturesCol(v: String): this.type = setVectorParam(visualFeatures, v)

  val details = new VectorizableParam[Seq[String]](
    this, "details", "what visual feature types to return",
    {
      case Left(seq) => seq.forall(Set("Celebrities", "Landmarks"))
      case _ => true
    }
  )

  def getDetails: Seq[String] = getScalarParam(details)

  def getDetailsCol: String = getVectorParam(details)

  def setDetails(v: Seq[String]): this.type = setScalarParam(details, v)

  def setDetailsCol(v: String): this.type = setVectorParam(details, v)

  val language = new VectorizableParam[String](
    this, "language", "the language of the response (en if none given)"
  )

  def getLanguage: String = getScalarParam(language)

  def getLanguageCol: String = getVectorParam(language)

  def setLanguage(v: String): this.type = setScalarParam(language, v)

  def setLanguageCol(v: String): this.type = setVectorParam(language, v)

  def this() = this(Identifiable.randomUID("AnalyzeImage"))

  def inputFunc(schema: StructType): Row => HttpPost = { row: Row =>
    val fullURL = getUrl + "?" + URLEncodingUtils
      .format(List(
        getValueOpt(row, visualFeatures).map("visualFeatures" -> _.mkString(",")),
        getValueOpt(row, details).map("details" -> _.mkString(",")),
        getValueOpt(row, language).map("language" -> _)
      ).flatten.toMap)
    val post = new HttpPost(fullURL)
    getValueOpt(row, subscriptionKey).foreach(post.setHeader("Ocp-Apim-Subscription-Key", _))
    post.setHeader("Content-Type", "application/json")
    val body = Map("url" -> getValue(row, imageUrl))
    post.setEntity(new StringEntity(body.toJson.compactPrint))
    post
  }

  override def responseDataType: DataType = AIResponse.schema

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/vision/v2.0/analyze")
}

object AnalyzeImage extends ComplexParamsReadable[AnalyzeImage]

object RecognizeDomainSpecificContent
  extends ComplexParamsReadable[RecognizeDomainSpecificContent] with Serializable {
  def getInputFunc(url: String,
                   subscriptionKey: Option[String],
                   staticParams: Map[String, String]): Row => HttpPost = {
    { dynamicParamRow: Row =>
      val allParams = staticParams ++ dynamicParamRow.getValuesMap(dynamicParamRow.schema.fieldNames)
      import spray.json.DefaultJsonProtocol._
      val post = new HttpPost(url + s"/models/${allParams("model")}/analyze")
      subscriptionKey.foreach(post.setHeader("Ocp-Apim-Subscription-Key", _))
      post.setHeader("Content-Type", "application/json")
      post.setEntity(new StringEntity(Map("url" -> allParams("url")).toJson.compactPrint))
      post
    }
  }

  def getProbableCeleb(inputCol: String, outputCol: String): UDFTransformer = {
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
  extends CognitiveServicesBase(uid) with HasImageUrl with HasVectorizableParams
    with HasInternalCustomInputParser with HasInternalJsonOutputParser {

  def this() = this(Identifiable.randomUID("RecognizeDomainSpecificContent"))

  val model = new VectorizableParam[String](this, "model",
    "the domain specific model: celebrities, landmarks")

  def setModel(v: String): this.type = setScalarParam(model, v)

  def setModelCol(v: String): this.type = setVectorParam(model, v)

  override def inputFunc(schema: StructType): Row => HttpRequestBase = { row: Row =>
    val post = new HttpPost(getUrl + s"/models/${getValue(row, model)}/analyze")
    getValueOpt(row, subscriptionKey).foreach(post.setHeader("Ocp-Apim-Subscription-Key", _))
    post.setHeader("Content-Type", "application/json")
    post.setEntity(new StringEntity(Map("url" -> getValue(row, imageUrl)).toJson.compactPrint))
    post
  }

  override def responseDataType: DataType = DSIRResponse.schema

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/vision/v2.0")
}
