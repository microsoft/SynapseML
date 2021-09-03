// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import com.microsoft.ml.spark.core.schema.DatasetExtensions
import com.microsoft.ml.spark.io.http.SimpleHTTPTransformer
import com.microsoft.ml.spark.logging.BasicLogging
import com.microsoft.ml.spark.stages.{DropColumns, Lambda, UDFTransformer}
import org.apache.http.client.methods.{HttpEntityEnclosingRequestBase, HttpPost, HttpRequestBase}
import org.apache.http.entity.{AbstractHttpEntity, ContentType, StringEntity}
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.{ComplexParamsReadable, NamespaceInjections, PipelineModel, Transformer}
import org.apache.spark.ml.param.ServiceParam
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{array, col, lit, struct}
import org.apache.spark.sql.types.{ArrayType, DataType, StringType, StructType}
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.net.URI

trait HasSubscriptionRegion extends HasServiceParams {
  val subscriptionRegion = new ServiceParam[String](
    this, "subscriptionRegion", "the API region to use")

  def getSubscriptionRegion: String = getScalarParam(subscriptionRegion)

  def setSubscriptionRegion(v: String): this.type = setScalarParam(subscriptionRegion, v)

  def getSubscriptionRegionCol: String = getVectorParam(subscriptionRegion)

  def setSubscriptionRegionCol(v: String): this.type = setVectorParam(subscriptionRegion, v)

}

trait HasTextInput extends HasServiceParams {
  val text = new ServiceParam[Seq[String]](
    this, "text", "the string to translate")

  def getText: Seq[String] = getScalarParam(text)

  def setText(v: Seq[String]): this.type = setScalarParam(text, v)

  def setText(v: String): this.type = setScalarParam(text, Seq(v))

  def getTextCol: String = getVectorParam(text)

  def setTextCol(v: String): this.type = setVectorParam(text, v)

}

trait HasFromLanguage extends HasServiceParams {
  val fromLanguage = new ServiceParam[String](this, "fromLanguage", "Specifies the language of the input text." +
    " The source language must be one of the supported languages included in the dictionary scope.",
    isRequired = true, isURLParam = true)

  def setFromLanguage(v: String): this.type = setScalarParam(fromLanguage, v)

  def setFromLanguageCol(v: String): this.type = setVectorParam(fromLanguage, v)

  def getFromLanguage: String = getScalarParam(fromLanguage)

  def getFromLanguageCol: String = getVectorParam(fromLanguage)
}

trait HasToLanguage extends HasServiceParams {
  val toLanguage = new ServiceParam[String](this, "toLanguage", "Specifies the language of the output text." +
    " The target language must be one of the supported languages included in the dictionary scope.",
    isRequired = true, isURLParam = true)

  def setToLanguage(v: String): this.type = setScalarParam(toLanguage, v)

  def setToLanguageCol(v: String): this.type = setVectorParam(toLanguage, v)

  def getToLanguage: String = getScalarParam(toLanguage)

  def getToLanguageCol: String = getVectorParam(toLanguage)
}

trait TextAsOnlyEntity extends HasTextInput with HasCognitiveServiceInput with HasSubscriptionRegion {

  override protected def inputFunc(schema: StructType): Row => Option[HttpRequestBase] = {
    { row: Row =>
      if (shouldSkip(row)) {
        None
      } else if (getValue(row, text).forall(Option(_).isEmpty)) {
        None
      } else {
        val urlParams: Array[ServiceParam[Any]] =
          getUrlParams.asInstanceOf[Array[ServiceParam[Any]]]

        val texts = getValue(row, text)

        val base = getUrl + "?api-version=3.0"
        val appended = if (!urlParams.isEmpty) {
          "&" + URLEncodingUtils.format(urlParams.flatMap(p =>
            getValueOpt(row, p).map {
              val pName = p.name match {
                case "fromLanguage" => "from"
                case "toLanguage" => "to"
                case s => s
              }
              v => pName -> p.toValueString(v)
            }
          ).toMap)
        } else {
          ""
        }

        val post = new HttpPost(base + appended)
        getValueOpt(row, subscriptionKey).foreach(post.setHeader("Ocp-Apim-Subscription-Key", _))
        getValueOpt(row, subscriptionRegion).foreach(post.setHeader("Ocp-Apim-Subscription-Region", _))
        post.setHeader("Content-Type", "application/json; charset=UTF-8")

        val json = texts.map(s => Map("Text" -> s)).toJson.compactPrint
        post.setEntity(new StringEntity(json, "UTF-8"))
        Some(post)
      }
    }
  }

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = { _ => None }
}

abstract class TextTranslatorBase(override val uid: String) extends CognitiveServicesBase(uid)
  with HasInternalJsonOutputParser with HasSubscriptionRegion
  with HasSetLocation with HasSetLinkedService {


  protected def reshapeColumns(schema: StructType, parameterNames: Seq[String])
  : Seq[(Transformer, String, String)] = {

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

    parameterNames.flatMap(x => reshapeToArray(x))
  }

  // noinspection ScalaStyle
  protected def customGetInternalTransformer(schema: StructType,
                                             parameterNames: Seq[String]): PipelineModel = {
    val dynamicParamColName = DatasetExtensions.findUnusedColumnName("dynamic", schema)

    val reshapeCols = reshapeColumns(schema, parameterNames)

    val newColumnMapping = reshapeCols.map {
      case (_, oldCol, newCol) => (oldCol, newCol)
    }.toMap

    val columnsToGroup = getVectorParamMap.values.size match {
      case 0 => getVectorParamMap.values.toList.map(col) match {
        case Nil => Seq(lit(false).alias("placeholder"))
        case l => l
      }
      case _ => getVectorParamMap.map { case (_, oldCol) =>
        val newCol = newColumnMapping.getOrElse(oldCol, oldCol)
        col(newCol).alias(oldCol)
      }.toSeq
    }

    val stages = reshapeCols.map(_._1).toArray ++ Array(
      Lambda(_.withColumn(
        dynamicParamColName,
        struct(columnsToGroup: _*))),
      new SimpleHTTPTransformer()
        .setInputCol(dynamicParamColName)
        .setOutputCol(getOutputCol)
        .setInputParser(getInternalInputParser(schema))
        .setOutputParser(getInternalOutputParser(schema))
        .setHandler(getHandler)
        .setConcurrency(getConcurrency)
        .setConcurrentTimeout(get(concurrentTimeout))
        .setErrorCol(getErrorCol),
      new DropColumns().setCols(Array(
        dynamicParamColName) ++ newColumnMapping.values.toArray.asInstanceOf[Array[String]])
    )

    NamespaceInjections.pipelineModel(stages)
  }

  override protected def getInternalTransformer(schema: StructType): PipelineModel =
    customGetInternalTransformer(schema, Seq("text"))

  override def setLocation(v: String): this.type = {
    setSubscriptionRegion(v)
    setUrl("https://api.cognitive.microsofttranslator.com/" + urlPath)
  }

}

object Translate extends ComplexParamsReadable[Translate]

class Translate(override val uid: String) extends TextTranslatorBase(uid)
  with TextAsOnlyEntity with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("Translate"))

  def urlPath: String = "translate"

  override protected def inputFunc(schema: StructType): Row => Option[HttpRequestBase] = {
    { row: Row =>
      if (shouldSkip(row)) {
        None
      } else if (getValue(row, text).forall(Option(_).isEmpty)) {
        None
      } else if (getValue(row, toLanguage).forall(Option(_).isEmpty)) {
        None
      } else {
        val urlParams: Array[ServiceParam[Any]] =
          getUrlParams.asInstanceOf[Array[ServiceParam[Any]]]

        val texts = getValue(row, text)

        val base = getUrl + "?api-version=3.0"
        val appended = if (!urlParams.isEmpty) {
          "&" + URLEncodingUtils.format(urlParams.flatMap(p =>
            getValueOpt(row, p).map {
              val pName = p.name match {
                case "fromLanguage" => "from"
                case "toLanguage" => "to"
                case s => s
              }
              v => pName -> p.toValueString(v)
            }
          ).toMap)
        } else {
          ""
        }

        val post = new HttpPost(base + appended)
        getValueOpt(row, subscriptionKey).foreach(post.setHeader("Ocp-Apim-Subscription-Key", _))
        getValueOpt(row, subscriptionRegion).foreach(post.setHeader("Ocp-Apim-Subscription-Region", _))
        post.setHeader("Content-Type", "application/json; charset=UTF-8")

        val json = texts.map(s => Map("Text" -> s)).toJson.compactPrint
        post.setEntity(new StringEntity(json, "UTF-8"))
        Some(post)
      }
    }
  }

  override protected def getInternalTransformer(schema: StructType): PipelineModel =
    customGetInternalTransformer(schema, Seq("text", "toLanguage"))

  val toLanguage = new ServiceParam[Seq[String]](this, "toLanguage", "Specifies the language of the output" +
    " text. The target language must be one of the supported languages included in the translation scope." +
    " For example, use to=de to translate to German. It's possible to translate to multiple languages simultaneously" +
    " by repeating the parameter in the query string. For example, use to=de&to=it to translate to German and Italian.",
    isRequired = true, isURLParam = true,
    toValueString = { seq => seq.mkString(",") })

  def setToLanguage(v: Seq[String]): this.type = setScalarParam(toLanguage, v)

  def setToLanguage(v: String): this.type = setScalarParam(toLanguage, Seq(v))

  def setToLanguageCol(v: String): this.type = setVectorParam(toLanguage, v)

  val fromLanguage = new ServiceParam[String](this, "fromLanguage", "Specifies the language of the input" +
    " text. Find which languages are available to translate from by looking up supported languages using the" +
    " translation scope. If the from parameter is not specified, automatic language detection is applied to" +
    " determine the source language. You must use the from parameter rather than autodetection when using the" +
    " dynamic dictionary feature.", isURLParam = true)

  def setFromLanguage(v: String): this.type = setScalarParam(fromLanguage, v)

  def setFromLanguageCol(v: String): this.type = setVectorParam(fromLanguage, v)

  val textType = new ServiceParam[String](this, "textType", "Defines whether the text being" +
    " translated is plain text or HTML text. Any HTML needs to be a well-formed, complete element. Possible values" +
    " are: plain (default) or html.", {
    case Left(s) => Set("plain", "html")(s)
    case Right(_) => true
  }, isURLParam = true)

  def setTextType(v: String): this.type = setScalarParam(textType, v)

  def setTextTypeCol(v: String): this.type = setVectorParam(textType, v)

  val category = new ServiceParam[String](this, "category", "A string specifying the category" +
    " (domain) of the translation. This parameter is used to get translations from a customized system built with" +
    " Custom Translator. Add the Category ID from your Custom Translator project details to this parameter to use" +
    " your deployed customized system. Default value is: general.", isURLParam = true)

  def setCategory(v: String): this.type = setScalarParam(category, v)

  def setCategoryCol(v: String): this.type = setVectorParam(category, v)

  val profanityAction = new ServiceParam[String](this, "profanityAction", "Specifies how" +
    " profanities should be treated in translations. Possible values are: NoAction (default), Marked or Deleted. ",
    {
      case Left(s) => Set("NoAction", "Marked", "Deleted")(s)
      case Right(_) => true
    }, isURLParam = true)

  def setProfanityAction(v: String): this.type = setScalarParam(profanityAction, v)

  def setProfanityActionCol(v: String): this.type = setVectorParam(profanityAction, v)

  val profanityMarker = new ServiceParam[String](this, "profanityMarker", "Specifies how" +
    " profanities should be marked in translations. Possible values are: Asterisk (default) or Tag.", {
    case Left(s) => Set("Asterisk", "Tag")(s)
    case Right(_) => true
  }, isURLParam = true)

  def setProfanityMarker(v: String): this.type = setScalarParam(profanityMarker, v)

  def setProfanityMarkerCol(v: String): this.type = setVectorParam(profanityMarker, v)

  val includeAlignment = new ServiceParam[Boolean](this, "includeAlignment", "Specifies whether" +
    " to include alignment projection from source text to translated text.", isURLParam = true)

  def setIncludeAlignment(v: Boolean): this.type = setScalarParam(includeAlignment, v)

  def setIncludeAlignmentCol(v: String): this.type = setVectorParam(includeAlignment, v)

  val includeSentenceLength = new ServiceParam[Boolean](this, "includeSentenceLength", "Specifies" +
    " whether to include sentence boundaries for the input text and the translated text. ", isURLParam = true)

  def setIncludeSentenceLength(v: Boolean): this.type = setScalarParam(includeSentenceLength, v)

  def setIncludeSentenceLengthCol(v: String): this.type = setVectorParam(includeSentenceLength, v)

  val suggestedFrom = new ServiceParam[String](this, "suggestedFrom", "Specifies a fallback" +
    " language if the language of the input text can't be identified. Language autodetection is applied when the" +
    " from parameter is omitted. If detection fails, the suggestedFrom language will be assumed.", isURLParam = true)

  def setSuggestedFrom(v: String): this.type = setScalarParam(suggestedFrom, v)

  def setSuggestedFromCol(v: String): this.type = setVectorParam(suggestedFrom, v)

  val fromScript = new ServiceParam[String](this, "fromScript", "Specifies the script of the" +
    " input text.", isURLParam = true)

  def setFromScript(v: String): this.type = setScalarParam(fromScript, v)

  def setFromScriptCol(v: String): this.type = setVectorParam(fromScript, v)

  val toScript = new ServiceParam[String](this, "toScript", "Specifies the script of the" +
    " translated text.", isURLParam = true)

  def setToScript(v: String): this.type = setScalarParam(toScript, v)

  def setToScriptCol(v: String): this.type = setVectorParam(toScript, v)

  val allowFallback = new ServiceParam[Boolean](this, "allowFallback", "Specifies that the service" +
    " is allowed to fall back to a general system when a custom system does not exist. ", isURLParam = true)

  def setAllowFallback(v: Boolean): this.type = setScalarParam(allowFallback, v)

  def setAllowFallbackCol(v: String): this.type = setVectorParam(allowFallback, v)

  setDefault(textType -> Left("plain"),
    category -> Left("general"),
    profanityAction -> Left("NoAction"),
    profanityMarker -> Left("Asterisk"),
    includeAlignment -> Left(false),
    includeSentenceLength -> Left(false),
    allowFallback -> Left(true))

  override def responseDataType: DataType = ArrayType(TranslateResponse.schema)
}

object Transliterate extends ComplexParamsReadable[Transliterate]

class Transliterate(override val uid: String) extends TextTranslatorBase(uid)
  with TextAsOnlyEntity with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("Transliterate"))

  def urlPath: String = "transliterate"

  val language = new ServiceParam[String](this, "language", "Language tag identifying the" +
    " language of the input text. If a code is not specified, automatic language detection will be applied.",
    isRequired = true, isURLParam = true)

  def setLanguage(v: String): this.type = setScalarParam(language, v)

  def setLanguageCol(v: String): this.type = setVectorParam(language, v)

  val fromScript = new ServiceParam[String](this, "fromScript", "Specifies the script of the" +
    " input text.", isRequired = true, isURLParam = true)

  def setFromScript(v: String): this.type = setScalarParam(fromScript, v)

  def setFromScriptCol(v: String): this.type = setVectorParam(fromScript, v)

  val toScript = new ServiceParam[String](this, "toScript", "Specifies the script of the" +
    " translated text.", isRequired = true, isURLParam = true)

  def setToScript(v: String): this.type = setScalarParam(toScript, v)

  def setToScriptCol(v: String): this.type = setVectorParam(toScript, v)

  override def responseDataType: DataType = ArrayType(TransliterateResponse.schema)
}

object Detect extends ComplexParamsReadable[Detect]

class Detect(override val uid: String) extends TextTranslatorBase(uid)
  with TextAsOnlyEntity with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("Detect"))

  def urlPath: String = "detect"

  override def responseDataType: DataType = ArrayType(DetectResponse.schema)
}

object BreakSentence extends ComplexParamsReadable[BreakSentence]

class BreakSentence(override val uid: String) extends TextTranslatorBase(uid)
  with TextAsOnlyEntity with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("BreakSentence"))

  def urlPath: String = "breaksentence"

  val language = new ServiceParam[String](this, "language", "Language tag identifying the" +
    " language of the input text. If a code is not specified, automatic language detection will be applied.",
    isURLParam = true)

  def setLanguage(v: String): this.type = setScalarParam(language, v)

  def setLanguageCol(v: String): this.type = setVectorParam(language, v)

  val script = new ServiceParam[String](this, "script", "Script tag identifying the script" +
    " used by the input text. If a script is not specified, the default script of the language will be assumed.",
    isURLParam = true)

  def setScript(v: String): this.type = setScalarParam(script, v)

  def setScriptCol(v: String): this.type = setVectorParam(script, v)

  override def responseDataType: DataType = ArrayType(BreakSentenceResponse.schema)
}

object DictionaryLookup extends ComplexParamsReadable[DictionaryLookup]

class DictionaryLookup(override val uid: String) extends TextTranslatorBase(uid)
  with TextAsOnlyEntity with HasFromLanguage with HasToLanguage with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("DictionaryLookup"))

  def urlPath: String = "dictionary/lookup"

  override def responseDataType: DataType = ArrayType(DictionaryLookupResponse.schema)
}

trait HasTextAndTranslationInput extends HasServiceParams {

  val textAndTranslation = new ServiceParam[Seq[(String, String)]](
    this, "textAndTranslation", " A string specifying the translated text" +
      " previously returned by the Dictionary lookup operation.")

  def getTextAndTranslation: Seq[(String, String)] = getScalarParam(textAndTranslation)

  def setTextAndTranslation(v: Seq[(String, String)]): this.type = setScalarParam(textAndTranslation, v)

  def setTextAndTranslation(v: (String, String)): this.type = setScalarParam(textAndTranslation, Seq(v))

  def getTextAndTranslationCol: String = getVectorParam(textAndTranslation)

  def setTextAndTranslationCol(v: String): this.type = setVectorParam(textAndTranslation, v)

}

object DictionaryExamples extends ComplexParamsReadable[DictionaryExamples]

class DictionaryExamples(override val uid: String) extends TextTranslatorBase(uid)
  with HasTextAndTranslationInput with HasFromLanguage with HasToLanguage
  with HasCognitiveServiceInput with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("DictionaryExamples"))

  def urlPath: String = "dictionary/examples"

  override protected def inputFunc(schema: StructType): Row => Option[HttpRequestBase] = {
    { row: Row =>
      if (shouldSkip(row)) {
        None
      } else {
        val urlParams: Array[ServiceParam[Any]] =
          getUrlParams.asInstanceOf[Array[ServiceParam[Any]]]

        val textAndTranslations = getValue(row, textAndTranslation)
        if (textAndTranslations.isEmpty)
          None
        else {

          val base = getUrl + "?api-version=3.0"
          val appended = if (!urlParams.isEmpty) {
            "&" + URLEncodingUtils.format(urlParams.flatMap(p =>
              getValueOpt(row, p).map {
                val pName = p.name match {
                  case "fromLanguage" => "from"
                  case "toLanguage" => "to"
                  case s => s
                }
                v => pName -> p.toValueString(v)
              }
            ).toMap)
          } else {
            ""
          }

          val post = new HttpPost(base + appended)
          getValueOpt(row, subscriptionKey).foreach(post.setHeader("Ocp-Apim-Subscription-Key", _))
          getValueOpt(row, subscriptionRegion).foreach(post.setHeader("Ocp-Apim-Subscription-Region", _))
          post.setHeader("Content-Type", "application/json; charset=UTF-8")

          val json = textAndTranslations.head.getClass.getTypeName match {
            case "scala.Tuple2" => textAndTranslations.map(
              t => Map("Text" -> t._1, "Translation" -> t._2)).toJson.compactPrint
            case _ => textAndTranslations.asInstanceOf[Seq[Row]].map(
              s => Map("Text" -> s.getString(0), "Translation" -> s.getString(1))).toJson.compactPrint
          }

          post.setEntity(new StringEntity(json, "UTF-8"))
          Some(post)
        }
      }
    }
  }

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = { _ => None }

  override protected def getInternalTransformer(schema: StructType): PipelineModel =
    customGetInternalTransformer(schema, Seq("textAndTranslation"))

  override def responseDataType: DataType = ArrayType(DictionaryExamplesResponse.schema)
}
