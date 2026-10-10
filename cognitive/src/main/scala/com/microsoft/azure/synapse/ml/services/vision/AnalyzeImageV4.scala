// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.vision

import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.io.http.{HandlingUtils, HTTPRequestData, HTTPResponseData}
import com.microsoft.azure.synapse.ml.param.ServiceParam
import com.microsoft.azure.synapse.ml.services._
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.param.IntArrayParam
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataType
import spray.json.DefaultJsonProtocol._

import java.net.URI
import scala.collection.JavaConverters._

object AnalyzeImageV4 extends ComplexParamsReadable[AnalyzeImageV4] {
  private[vision] val SupportedFeatures =
    Set("tags", "objects", "caption", "denseCaptions", "read", "smartCrops", "people")

  private[vision] def validFeatures(value: Seq[String]): Boolean =
    value != null && value.nonEmpty && value.forall(SupportedFeatures)

  private[vision] def validRatios(value: Seq[_]): Boolean =
    value != null && value.nonEmpty && value.forall {
      case number: java.lang.Number =>
        val ratio = number.doubleValue()
        ratio >= 0.75 && ratio <= 1.8
      case _ => false
    }

  private def encodeRatios(value: Any): String = value match {
    // Spark array columns can contain boxed integral or float values rather than Doubles.
    case ratios: Seq[_] if validRatios(ratios) =>
      ratios.collect { case number: java.lang.Number => number.doubleValue() }.mkString(",")
    case _ => throw new IllegalArgumentException(
      "Invalid smart crop aspect ratios: expected a non-empty numeric array with values between 0.75 and 1.8")
  }
}

/** Analyzes images with the synchronous Image Analysis 4.0 GA API, version 2024-02-01.
  *
  * This is a separate stage because its features and response schema are not compatible with
  * [[AnalyzeImage]]. Image input can be a URL or bytes; null input rows produce null output without
  * an HTTP request. Unrequested result fields are null. HTTP failures populate errorCol.
  * Caption and denseCaptions require a region that supports those features.
  */
class AnalyzeImageV4(override val uid: String)
  extends CognitiveServicesBaseNoHandler(uid) with HasImageInput with HasCognitiveServiceInput
    with HasInternalJsonOutputParser with HasSetLocation with HasSetLinkedService with SynapseMLLogging {
  logClass(FeatureNames.AiServices.Vision)

  override protected lazy val pyInternalWrapper = true

  def this() = this(Identifiable.randomUID("AnalyzeImageV4"))

  val backoffs = new IntArrayParam(this, "backoffs", "Retry backoff delays in milliseconds",
    values => values != null && values.forall(_ >= 0))

  def getBackoffs: Array[Int] = $(backoffs)

  def setBackoffs(value: Array[Int]): this.type = set(backoffs, value)

  // Persist plain retry settings, not a serialized executable handler UDF.
  setDefault(backoffs -> Array(100, 500, 1000)) //scalastyle:ignore magic.number

  override protected def handlingFunc(client: CloseableHttpClient, request: HTTPRequestData): HTTPResponseData =
    HandlingUtils.advanced(getBackoffs: _*)(client, request)

  val features = new ServiceParam[Seq[String]](
    this, "features", "Image Analysis 4.0 features; defaults to tags",
    {
      case Left(value) => AnalyzeImageV4.validFeatures(value)
      case Right(_) => true
    }, isRequired = true, isURLParam = true, toValueString = _.mkString(","))

  def getFeatures: Seq[String] = getScalarParam(features)

  def getFeaturesCol: String = getVectorParam(features)

  /** Selects one or more GA features. Null, empty and legacy feature names are rejected. */
  def setFeatures(value: Seq[String]): this.type = setScalarParam(features, value)

  def setFeatures(value: java.util.ArrayList[String]): this.type = {
    require(value != null, "features must not be null")
    setFeatures(value.asScala.toSeq)
  }

  def setFeaturesCol(value: String): this.type = setVectorParam(features, value)

  val language = new ServiceParam[String](
    this, "language", "Language of the results; the service defaults to en", isURLParam = true)

  def getLanguage: String = getScalarParam(language)

  def getLanguageCol: String = getVectorParam(language)

  def setLanguage(value: String): this.type = setScalarParam(language, value)

  def setLanguageCol(value: String): this.type = setVectorParam(language, value)

  val genderNeutralCaption = new ServiceParam[Boolean](
    this, "genderNeutralCaption", "Use gender-neutral captions", isURLParam = true) {
    override val payloadName: String = "gender-neutral-caption"
  }

  def getGenderNeutralCaption: Boolean = getScalarParam(genderNeutralCaption)

  def getGenderNeutralCaptionCol: String = getVectorParam(genderNeutralCaption)

  def setGenderNeutralCaption(value: Boolean): this.type = setScalarParam(genderNeutralCaption, value)

  def setGenderNeutralCaptionCol(value: String): this.type = setVectorParam(genderNeutralCaption, value)

  val smartCropsAspectRatios = new ServiceParam[Seq[Double]](
    this, "smartCropsAspectRatios", "Requested crop aspect ratios, each between 0.75 and 1.8 inclusive",
    {
      case Left(value) => AnalyzeImageV4.validRatios(value)
      case Right(_) => true
    }, isURLParam = true, toValueString = _.mkString(",")) {
    override val payloadName: String = "smartcrops-aspect-ratios"
  }

  def getSmartCropsAspectRatios: Seq[Double] = getScalarParam(smartCropsAspectRatios)

  def getSmartCropsAspectRatiosCol: String = getVectorParam(smartCropsAspectRatios)

  def setSmartCropsAspectRatios(value: Seq[Double]): this.type = setScalarParam(smartCropsAspectRatios, value)

  def setSmartCropsAspectRatios(value: java.util.ArrayList[Double]): this.type = {
    require(value != null, "smartCropsAspectRatios must not be null")
    setSmartCropsAspectRatios(value.asScala.toSeq)
  }

  def setSmartCropsAspectRatiosCol(value: String): this.type = setVectorParam(smartCropsAspectRatios, value)

  setDefault(features -> Left(Seq("tags")))

  override def urlPath: String = "/computervision/imageanalysis:analyze"

  override protected def responseDataType: DataType = ImageAnalysisV4Response.schema

  /** Sets a resource endpoint with or without a trailing slash, replacing any earlier URL override.
    * Query parameters belong in setUrl or the individual stage parameters, not in this endpoint.
    */
  override def setEndpoint(value: String): this.type = {
    require(value != null && value.trim.nonEmpty, "endpoint must not be blank")
    val endpoint = new URI(value)
    require(Set("http", "https")(endpoint.getScheme) && endpoint.getHost != null &&
      endpoint.getRawQuery == null && endpoint.getRawFragment == null && endpoint.getUserInfo == null,
      "endpoint must be an absolute HTTP(S) resource URL without query, fragment or user information")
    setUrl(value.stripSuffix("/") + urlPath)
  }

  override protected def prepareUrl: Row => String = { row =>
    require(AnalyzeImageV4.validFeatures(getValue(row, features)), "Invalid Image Analysis 4.0 features")
    val builder = new URIBuilder(get(customUrlRoot).getOrElse(prepareUrlRoot(row)))
    // Replace reserved query keys rather than appending a second '?' or retaining a preview version.
    builder.setParameter("api-version", "2024-02-01")
    getUrlParams.foreach { param =>
      val typed = param.asInstanceOf[ServiceParam[Any]]
      getValueAnyOpt(row, typed).foreach { value =>
        val encoded = if (param == smartCropsAspectRatios) AnalyzeImageV4.encodeRatios(value)
          else typed.toValueString(value)
        builder.setParameter(typed.payloadName, encoded)
      }
    }
    builder.build().toString
  }
}
