// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.util.UUID

import org.apache.commons.lang3.StringEscapeUtils
import org.apache.http.client.methods.{HttpPost, HttpRequestBase}
import org.apache.http.entity.StringEntity
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}
import spray.json._

object TextAnalyticsUtils extends Serializable {
  def getInputFunc(url: String,
                   subscriptionKey: Option[String],
                   staticParams: Map[String, String]): Map[String, String] => HttpPost = {
    { dynamicParams: Map[String, String] =>
      import TAJSONFormat._
      val allParams = dynamicParams ++ staticParams
      val post = new HttpPost(url)
      subscriptionKey.foreach(post.setHeader("Ocp-Apim-Subscription-Key", _))
      post.setHeader("Content-Type", "application/json")
      val json = TARequest(Array(TADocument(
        allParams.get("language"), allParams("id"),
        Option(allParams("text")).getOrElse("")
      ))).toJson.compactPrint
      post.setEntity(new StringEntity(json, "UTF-8"))
      post
    }
  }
}

abstract class TextAnalyticsBase(override val uid: String)
  extends CognitiveServicesBase(uid) {

  def setTextCol(v: String): this.type = updateDynamicCols("text", v)

  def setText(v: String): this.type = updateStatic("text", v)

  def setIdCol(v: String): this.type = updateDynamicCols("id", v)

  def setId(v: String): this.type = updateStatic("id", v)

  override def inputFunc: Map[String, String] => HttpRequestBase =
    TextAnalyticsUtils.getInputFunc(getUrl, get(subscriptionKey), getStaticParams)

  override def transform(dataset: Dataset[_]): DataFrame = {
    if (!getStaticParams.keySet("id") && !getDynamicParamCols.keySet("id")) {
      val idCol = this.uid + "_id"
      val generateUUID = udf(() => UUID.randomUUID().toString)
      setIdCol(idCol)
      super.transform(dataset.toDF.withColumn(idCol, generateUUID()))
        .drop(idCol)
    } else {
      super.transform(dataset)
    }
  }

}

trait HasLanguage extends CognitiveServicesBase {
  def setLanguageCol(v: String): this.type = updateDynamicCols("language", v)

  def setLanguage(v: String): this.type = updateStatic("language", v)
}

object TextSentiment extends ComplexParamsReadable[TextSentiment]

class TextSentiment(override val uid: String)
  extends TextAnalyticsBase(uid) with HasLanguage {

  def this() = this(Identifiable.randomUID("TextSentiment"))

  override def responseDataType: DataType = SentimentResponse.schema

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/text/analytics/v2.0/sentiment")

}

object LanguageDetector extends ComplexParamsReadable[LanguageDetector]

class LanguageDetector(override val uid: String)
  extends TextAnalyticsBase(uid) {

  def this() = this(Identifiable.randomUID("LanguageDetector"))

  override def responseDataType: DataType = DetectLanguageResponse.schema

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/text/analytics/v2.0/languages")

}

object EntityDetector extends ComplexParamsReadable[EntityDetector]

class EntityDetector(override val uid: String)
  extends TextAnalyticsBase(uid) with HasLanguage {

  def this() = this(Identifiable.randomUID("EntityDetector"))

  override def responseDataType: DataType = DetectEntitiesResponse.schema

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/text/analytics/v2.0/entities")

}

object NER extends ComplexParamsReadable[NER]

class NER(override val uid: String)
  extends TextAnalyticsBase(uid) with HasLanguage {

  def this() = this(Identifiable.randomUID("NER"))

  override def responseDataType: DataType = NERResponse.schema
}

object KeyPhraseExtractor extends ComplexParamsReadable[EntityDetector]

class KeyPhraseExtractor(override val uid: String)
  extends TextAnalyticsBase(uid) with HasLanguage {

  def this() = this(Identifiable.randomUID("KeyPhraseExtractor"))

  override def responseDataType: DataType = KeyPhraseResponse.schema

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/text/analytics/v2.0/keyPhrases")

}
