// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.http.client.methods.{HttpPost, HttpRequestBase}
import org.apache.http.entity.StringEntity
import org.apache.spark.ml.util._
import org.apache.spark.ml.{NamespaceInjections, PipelineModel}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import spray.json._

trait HasImageUrl extends CognitiveServicesBase {
  def setImageUrl(v: String): this.type = updateStatic("url", v)

  def setImageUrlCol(v: String): this.type = updateDynamicCols("url", v)
}

object OCR extends ComplexParamsReadable[OCR] with Serializable {
  def getInputFunc(url: String,
                   subscriptionKey: Option[String],
                   staticParams: Map[String, String]): Map[String, String] => HttpPost = {
    { dynamicParams: Map[String, String] =>
      import spray.json.DefaultJsonProtocol._
      val post = new HttpPost(url)
      subscriptionKey.foreach(post.setHeader("Ocp-Apim-Subscription-Key", _))
      post.setHeader("Content-Type", "application/json")
      post.setEntity(new StringEntity((staticParams ++ dynamicParams).toJson.compactPrint))
      post
    }
  }

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
  with HasLanguage with HasImageUrl {

  def this() = this(Identifiable.randomUID("OCRTransformer"))

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/vision/v2.0/ocr")

  def setDetectOrientation(v: Boolean): this.type = updateStatic("detectOrientation", v.toString)

  def setDetectOrientationCol(v: Boolean): this.type = updateDynamicCols("detectOrientation", v.toString)

  def inputFunc: Map[String, String] => HttpPost =
    OCR.getInputFunc(getUrl, get(subscriptionKey), getStaticParams)

  override def responseDataType: DataType = OCRResponse.schema
}

object GenerateThumbnails extends ComplexParamsReadable[GenerateThumbnails] with Serializable {
  def getInputFunc(url: String,
                   subscriptionKey: Option[String],
                   staticParams: Map[String, String]): Map[String, String] => HttpPost = {
    { dynamicParams: Map[String, String] =>
      val allParams = staticParams ++ dynamicParams
      val imageUrl = allParams("url")
      import spray.json.DefaultJsonProtocol._
      val fullURL = url + "?" + URLEncodingUtils
        .format(allParams.filterKeys { k => !(k == "url") })
      val post = new HttpPost(fullURL)
      subscriptionKey.foreach(post.setHeader("Ocp-Apim-Subscription-Key", _))
      post.setHeader("Content-Type", "application/json")
      post.setEntity(new StringEntity(Map("url" -> imageUrl).toJson.compactPrint))
      post
    }
  }
}

class GenerateThumbnails(override val uid: String)
  extends CognitiveServicesBase(uid) with HasImageUrl {

  def this() = this(Identifiable.randomUID("GenerateThumbnails"))

  def inputFunc: Map[String, String] => HttpPost =
    GenerateThumbnails.getInputFunc(getUrl, get(subscriptionKey), getStaticParams)

  override private[ml] def getInternalTransformer(dynamicParamCol: String, schema: StructType): PipelineModel = {
    NamespaceInjections.pipelineModel(Array(
      new SimpleHTTPTransformer()
        .setInputCol(dynamicParamCol)
        .setOutputCol(getOutputCol)
        .setInputParser(new CustomInputParser().setUDF(inputFunc))
        .setOutputParser(new CustomOutputParser().setUDF({ r: HTTPResponseData => r.entity.content }))
        .setHandlingStrategy(getHandlingStrategy)
        .setConcurrency(getConcurrency)
        .setConcurrentTimeout(getConcurrentTimeout)
        .setErrorCol(getErrorCol),
      new DropColumns().setCol(dynamicParamCol)
    ))
  }

  override def responseDataType: DataType = BinaryType

  def setWidth(v: Int): this.type = {
    updateStatic("width", v.toString)
  }

  def setWidthCol(v: String): this.type = {
    updateDynamicCols("width", v)
  }

  def setHeight(v: Int): this.type = {
    updateStatic("height", v.toString)
  }

  def setHeightCol(v: String): this.type = {
    updateDynamicCols("height", v)
  }

  def setSmartCropping(v: Boolean): this.type = {
    updateStatic("smartCropping", v.toString)
  }

  def setSmartCroppingCol(v: String): this.type = {
    updateDynamicCols("smartCropping", v)
  }

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/vision/v2.0/generateThumbnail")

}

object RecognizeDomainSpecificContent extends ComplexParamsReadable[RecognizeDomainSpecificContent] with Serializable {
  def getInputFunc(url: String,
                   subscriptionKey: Option[String],
                   staticParams: Map[String, String]): Map[String, String] => HttpPost = {
    { dynamicParams: Map[String, String] =>
      import spray.json.DefaultJsonProtocol._
      val allParams = dynamicParams ++ staticParams
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
  extends CognitiveServicesBase(uid) with HasImageUrl {

  def this() = this(Identifiable.randomUID("RecognizeDomainSpecificContent"))

  def setModel(v: String): this.type = updateStatic("model", v)

  override def inputFunc: Map[String, String] => HttpRequestBase =
    RecognizeDomainSpecificContent.getInputFunc(getUrl, get(subscriptionKey), getStaticParams)

  override def responseDataType: DataType = DSIRResponse.schema

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/vision/v2.0")
}
