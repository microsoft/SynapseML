// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.util.UUID

import com.microsoft.ml.spark.schema.DatasetExtensions
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.http.client.methods.{HttpPost, HttpRequestBase}
import org.apache.http.entity.StringEntity
import org.apache.spark.ml.{NamespaceInjections, PipelineModel, Transformer}
import org.apache.spark.ml.param.{BooleanParam, VectorizableParam}
import org.apache.spark.ml.util._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, _}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import spray.json._
import spray.json.DefaultJsonProtocol._

object TextAnalyticsUtils extends Serializable {

  def makeDocumentsCol(idCol: String, textCol: String, languageCol: Option[String] = None): Column = {
    array(struct(
      languageCol.map(col(_).alias("language"))
        .getOrElse(typedLit[Option[String]](None).alias("language")),
      col(idCol).cast("string").alias("id"),
      col(textCol)
    ))
  }

}

abstract class TextAnalyticsBase(override val uid: String) extends CognitiveServicesBase(uid)
  with HasInternalCustomInputParser with HasInternalJsonOutputParser {

  val text = new VectorizableParam[Seq[String]](this, "text", "the text in the request body")

  def setTextCol(v: String): this.type = setVectorParam(text, v)

  def setText(v: Seq[String]): this.type = setScalarParam(text, v)

  def setText(v: String): this.type = setScalarParam(text, Seq(v))

  setDefault(text -> Right("text"))

  val language = new VectorizableParam[Seq[String]](this, "language",
    "the language code of the text (optional for some services)")

  def setLanguageCol(v: String): this.type = setVectorParam(language, v)

  def setLanguage(v: Seq[String]): this.type = setScalarParam(language, v)

  def setLanguage(v: String): this.type = setScalarParam(language, Seq(v))

  protected def innerResponseDataType: StructType =
    responseDataType("documents").dataType match {
      case ArrayType(idt: StructType, _) => idt
      case _ =>
        throw new IllegalArgumentException("response data types should have a inner type")
    }

  override protected def responseDataType: StructType = {
    new StructType()
      .add("documents", ArrayType(innerResponseDataType))
      .add("errors", ArrayType(TAError.schema))
  }

  override protected def inputFunc(schema: StructType): Row => HttpRequestBase = {
    { row: Row =>
      import TAJSONFormat._
      val post = new HttpPost(getUrl)
      getValueOpt(row, subscriptionKey).foreach(post.setHeader("Ocp-Apim-Subscription-Key", _))
      post.setHeader("Content-Type", "application/json")
      val texts = getValue(row, text)
      val languages = getValueOpt(row, language).map { seq =>
        if (seq.lengthCompare(1) == 0) Array.fill(texts.length)(seq.head)
        else seq.toArray
      }
      val documents = texts.zipWithIndex.map { case (t, i) => TADocument(
        Option(languages.map(ls => ls(i)).orNull),
        i.toString,
        t
      )
      }
      val json = TARequest(documents).toJson.compactPrint
      post.setEntity(new StringEntity(json, "UTF-8"))
      post
    }
  }

  override protected def getInternalTransformer(schema: StructType): PipelineModel = {
    val dynamicParamColName = DatasetExtensions.findUnusedColumnName("dynamic", schema)

    def reshapeToArray(parameterName: String): Option[(Transformer, String, String)] = {
      val reshapedColName = DatasetExtensions.findUnusedColumnName(parameterName, schema)
      getVectorParamMap.get(parameterName).flatMap {
        case c if schema(c).dataType == StringType =>
          Some((Lambda(_.withColumn(reshapedColName, array(col(getVectorParam(parameterName))))),
            getVectorParam(parameterName),
            reshapedColName))
        case _ => None
      }
    }

    val reshapeCols = Seq(reshapeToArray("text"), reshapeToArray("language")).flatten

    val newColumnMapping = reshapeCols.map {
      case (lambda, oldCol, newCol) => (oldCol, newCol)
    }.toMap

    val columnsToGroup = getVectorParamMap.map { case (paramName, oldCol) =>
      val newCol = newColumnMapping.getOrElse(oldCol, oldCol)
      col(newCol).alias(oldCol)
    }.toSeq

    val unpackBatchUDF = udf({ row: Row =>
      val documents = row.getSeq[Row](0).map(doc => (doc.getString(0).toInt, doc)).toMap
      val errors = row.getSeq[Row](1).map(err => (err.getString(0).toInt, err)).toMap
      val rows: Seq[Row] = (0 until (documents.size + errors.size)).map(i =>
        documents.get(i)
          .map(doc => Row(doc.get(1), None))
          .getOrElse(Row(None, errors(i).getString(1)))
      )
      rows
    }, ArrayType(
      new StructType()
        .add(
          innerResponseDataType.fields(1).name,
          innerResponseDataType.fields(1).dataType)
        .add("error-message", StringType)
    ))

    val stages = reshapeCols.map(_._1).toArray ++ Array(
      Lambda(_.withColumn(
        dynamicParamColName,
        struct(columnsToGroup: _*))),
      new SimpleHTTPTransformer()
        .setInputCol(dynamicParamColName)
        .setOutputCol(getOutputCol)
        .setInputParser(getInternalInputParser(schema))
        .setOutputParser(getInternalOutputParser(schema))
        .setHandlingStrategy(getHandlingStrategy)
        .setConcurrency(getConcurrency)
        .setConcurrentTimeout(getConcurrentTimeout)
        .setErrorCol(getErrorCol),
      new UDFTransformer()
        .setInputCol(getOutputCol)
        .setOutputCol(getOutputCol)
        .setUDF(unpackBatchUDF),
      new DropColumns().setCols(Array(
        dynamicParamColName) ++ newColumnMapping.values.toArray.asInstanceOf[Array[String]])
    )

    NamespaceInjections.pipelineModel(stages)
  }

}

trait HasLanguage extends HasVectorizableParams {
  val language = new VectorizableParam[String](this, "language", "the language to use")

  def setLanguageCol(v: String): this.type = setVectorParam(language, v)

  def setLanguage(v: String): this.type = setScalarParam(language, v)
}

object TextSentiment extends ComplexParamsReadable[TextSentiment]

class TextSentiment(override val uid: String)
  extends TextAnalyticsBase(uid) {

  def this() = this(Identifiable.randomUID("TextSentiment"))

  override def responseDataType: StructType = SentimentResponse.schema

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/text/analytics/v2.0/sentiment")

}

object LanguageDetector extends ComplexParamsReadable[LanguageDetector]

class LanguageDetector(override val uid: String)
  extends TextAnalyticsBase(uid) {

  def this() = this(Identifiable.randomUID("LanguageDetector"))

  override def responseDataType: StructType = DetectLanguageResponse.schema

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/text/analytics/v2.0/languages")

}

object EntityDetector extends ComplexParamsReadable[EntityDetector]

class EntityDetector(override val uid: String)
  extends TextAnalyticsBase(uid) {

  def this() = this(Identifiable.randomUID("EntityDetector"))

  override def responseDataType: StructType = DetectEntitiesResponse.schema

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/text/analytics/v2.0/entities")

}

object NER extends ComplexParamsReadable[NER]

class NER(override val uid: String)
  extends TextAnalyticsBase(uid) {

  def this() = this(Identifiable.randomUID("NER"))

  override def responseDataType: StructType = NERResponse.schema
}

object KeyPhraseExtractor extends ComplexParamsReadable[EntityDetector]

class KeyPhraseExtractor(override val uid: String)
  extends TextAnalyticsBase(uid) {

  def this() = this(Identifiable.randomUID("KeyPhraseExtractor"))

  override def responseDataType: StructType = KeyPhraseResponse.schema

  def setLocation(v: String): this.type =
    setUrl(s"https://$v.api.cognitive.microsoft.com/text/analytics/v2.0/keyPhrases")

}
