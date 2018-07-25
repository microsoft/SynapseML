// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.http.client.methods.{HttpPost, HttpRequestBase}
import org.apache.http.entity.StringEntity
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
    this, "detectOrientation", "the API key to use")

  def getDetectOrientation: Boolean = getScalarParam(detectOrientation)

  def setDetectOrientation(v: Boolean): this.type = setScalarParam(detectOrientation, v)

  def getDetectOrientationCol: String = getVectorParam(detectOrientation)

  def setDetectOrientationCol(v: String): this.type = setVectorParam(detectOrientation, v)

}

trait HasWidth extends HasVectorizableParams {
  val width = new VectorizableParam[Int](
    this, "width", "the API key to use")

  def getWidth: Int = getScalarParam(width)

  def setWidth(v: Int): this.type = setScalarParam(width, v)

  def getWidthCol: String = getVectorParam(width)

  def setWidthCol(v: String): this.type = setVectorParam(width, v)

}

trait HasHeight extends HasVectorizableParams {
  val height = new VectorizableParam[Int](
    this, "height", "the API key to use")

  def getHeight: Int = getScalarParam(height)

  def setHeight(v: Int): this.type = setScalarParam(height, v)

  def getHeightCol: String = getVectorParam(height)

  def setHeightCol(v: String): this.type = setVectorParam(height, v)

}

trait HasSmartCropping extends HasVectorizableParams {
  val smartCropping = new VectorizableParam[Boolean](
    this, "detectOrientation", "the API key to use")

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

  def this() = this(Identifiable.randomUID("OCRTransformer"))

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

object RecognizeDomainSpecificContent extends ComplexParamsReadable[RecognizeDomainSpecificContent] with Serializable {
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
