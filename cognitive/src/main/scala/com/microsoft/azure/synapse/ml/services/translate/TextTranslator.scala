// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.translate

import com.microsoft.azure.synapse.ml.services._
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.io.http.SimpleHTTPTransformer
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.ServiceParam
import com.microsoft.azure.synapse.ml.stages.{DropColumns, Lambda}
import org.apache.http.client.methods.{HttpGet, HttpPost, HttpRequestBase}
import org.apache.http.entity.{AbstractHttpEntity, StringEntity}
import org.apache.spark.ml.param.{Param, StringArrayParam}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsReadable, NamespaceInjections, PipelineModel, Transformer}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{array, col, lit, struct}
import org.apache.spark.sql.types.{ArrayType, DataType, StringType, StructType}
import spray.json.DefaultJsonProtocol._
import spray.json._

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

private[translate] object TranslatorApiVersion {
  val V3: String = "3.0"
  val V2026: String = "2026-06-06"
  val Supported: Set[String] = Set(V3, V2026)
}

trait TextAsOnlyEntity extends HasTextInput with HasCognitiveServiceInput with HasSubscriptionRegion {
  this: TextTranslatorBase =>

  override protected def contentType: Row => String = { _ => "application/json; charset=UTF-8" }

  override protected def inputFunc(schema: StructType): Row => Option[HttpRequestBase] = {
    { row: Row =>
      if (shouldSkip(row)) {
        None
      } else {
        val urlParams: Array[ServiceParam[Any]] =
          getUrlParams.asInstanceOf[Array[ServiceParam[Any]]]

        val version = getApiVersion
        validateApiVersion(version)
        val texts = getValue(row, text)
          .flatMap(Option(_))
          .filter(value => version == TranslatorApiVersion.V3 || value.trim.nonEmpty)
        if (texts.isEmpty) {
          None
        } else {
          val base = getUrl + s"?api-version=$version"
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
          addHeaders(post, row)
          getValueOpt(row, subscriptionRegion).foreach(post.setHeader("Ocp-Apim-Subscription-Region", _))

          val json = version match {
            case TranslatorApiVersion.V3 => texts.map(s => Map("Text" -> s)).toJson.compactPrint
            case TranslatorApiVersion.V2026 =>
              val inputs = texts.map(s => JsObject("text" -> JsString(s)))
              JsObject("inputs" -> JsArray(inputs.toVector)).compactPrint
          }
          post.setEntity(new StringEntity(json, "UTF-8"))
          Some(post)
        }
      }
    }
  }

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = { _ => None }
}

abstract class TextTranslatorBase(override val uid: String) extends CognitiveServicesBase(uid)
  with HasInternalJsonOutputParser with HasSubscriptionRegion
  with HasSetLocation with HasSetLinkedServiceUsingLocation {

  val apiVersion = new Param[String](
    this,
    "apiVersion",
    "Translator Text API version.",
    isValid = TranslatorApiVersion.Supported)

  setDefault(apiVersion -> TranslatorApiVersion.V3)

  def getApiVersion: String = $(apiVersion)

  def setApiVersion(v: String): this.type = {
    require(TranslatorApiVersion.Supported(v),
      s"Unsupported Translator API version '$v'. Supported versions: $supportedApiVersions")
    set(apiVersion, v)
  }

  private def supportedApiVersions: String = TranslatorApiVersion.Supported.toSeq.sorted.mkString(", ")

  protected def supportsApiVersion(version: String): Boolean = version == TranslatorApiVersion.V3

  protected def validateApiVersion(version: String): Unit = {
    require(TranslatorApiVersion.Supported(version),
      s"Unsupported Translator API version '$version'. Supported versions: $supportedApiVersions")
    require(supportsApiVersion(version),
      s"${getClass.getSimpleName} is not available in Translator API $version. Use API 3.0 for this operation.")
  }

  override private[ml] def internalServiceType: String = "texttranslation"

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

    val missingRequiredParams = this.getRequiredParams.filter {
      p => this.get(p).isEmpty && this.getDefault(p).isEmpty
    }
    assert(missingRequiredParams.isEmpty,
      s"Missing required params: ${missingRequiredParams.map(s => s.name).mkString("(", ", ", ")")}")

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
    {
      validateApiVersion(getApiVersion)
      customGetInternalTransformer(schema, Seq("text"))
    }

  override def setLocation(v: String): this.type = {
    setSubscriptionRegion(v)
    val domain = getLocationDomain(v)
    setUrl(s"https://api.cognitive.microsofttranslator.$domain/" + urlPath)
  }

}

object Translate extends ComplexParamsReadable[Translate]

class Translate(override val uid: String) extends TextTranslatorBase(uid)
  with TextAsOnlyEntity with SynapseMLLogging {
  logClass(FeatureNames.AiServices.Translate)

  def this() = this(Identifiable.randomUID("Translate"))

  def urlPath: String = "translate"

  override protected def supportsApiVersion(version: String): Boolean =
    TranslatorApiVersion.Supported(version)

  override protected def contentType: Row => String = { _ => "application/json; charset=UTF-8" }

  private def v3Query(row: Row): String = {
    val urlParams = getUrlParams
      .asInstanceOf[Array[ServiceParam[Any]]]
    if (urlParams.isEmpty) {
      ""
    } else {
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
    }
  }

  private def v2026Payload(row: Row, texts: Seq[String]): String = {
    require(!isSet(includeAlignment) && !isSet(includeSentenceLength) && !isSet(suggestedFrom),
      "includeAlignment, includeSentenceLength, and suggestedFrom are only supported by Translator API 3.0.")
    val sourceLanguage = getValueOpt(row, fromLanguage).map(JsString(_))
    val sourceScript = getValueOpt(row, fromScript).map(JsString(_))
    val sourceTextType = getValueOpt(row, textType).map { value =>
      value match {
        case "plain" => JsString("Plain")
        case "html" => JsString("Html")
        case _ => throw new IllegalArgumentException(
          s"Invalid textType '$value'. Supported values are plain and html.")
      }
    }
    val targetScript = getValueOpt(row, toScript).map(JsString(_))
    val deploymentName = getValueOpt(row, category).filterNot(_ == "general").map(JsString(_))
    val fallback = getValueOpt(row, allowFallback).filterNot(identity).map(JsBoolean(_))
    val action = getValueOpt(row, profanityAction).filterNot(_ == "NoAction").map(JsString(_))
    val marker = getValueOpt(row, profanityMarker).filterNot(_ == "Asterisk").map(JsString(_))
    val targetLanguages = getValue(row, toLanguage)
      .flatMap(Option(_))
      .filter(_.trim.nonEmpty)
    require(targetLanguages.nonEmpty,
      "Translator API 2026-06-06 requires at least one non-blank target language.")
    val targets = targetLanguages.map { language =>
      JsObject(Map("language" -> JsString(language)) ++
        targetScript.map("script" -> _) ++
        deploymentName.map("deploymentName" -> _) ++
        fallback.map("allowFallback" -> _) ++
        action.map("profanityAction" -> _) ++
        marker.map("profanityMarker" -> _))
    }
    val inputs = texts.map { textValue =>
      JsObject(Map("text" -> JsString(textValue), "targets" -> JsArray(targets.toVector)) ++
        sourceLanguage.map("language" -> _) ++
        sourceScript.map("script" -> _) ++
        sourceTextType.filterNot(_ == JsString("Plain")).map("textType" -> _))
    }
    JsObject("inputs" -> JsArray(inputs.toVector)).compactPrint
  }

  override protected def inputFunc(schema: StructType): Row => Option[HttpRequestBase] = {
    { row: Row =>
      if (shouldSkip(row)) {
        None
      } else {
        val version = getApiVersion
        validateApiVersion(version)
        val texts = getValue(row, text)
          .flatMap(Option(_))
          .filter(value => version == TranslatorApiVersion.V3 || value.trim.nonEmpty)
        val targetsMissing = version == TranslatorApiVersion.V3 &&
          getValue(row, toLanguage).forall(Option(_).isEmpty)
        if (texts.isEmpty || targetsMissing) {
          None
        } else {
          val base = getUrl + s"?api-version=$version"
          val appended = if (version == TranslatorApiVersion.V3) v3Query(row) else ""

          val post = new HttpPost(base + appended)
          addHeaders(post, row)
          getValueOpt(row, subscriptionRegion).foreach(post.setHeader("Ocp-Apim-Subscription-Region", _))

          val json = version match {
            case TranslatorApiVersion.V3 =>
              texts.map(s => Map("Text" -> s)).toJson.compactPrint
            case TranslatorApiVersion.V2026 =>
              v2026Payload(row, texts)
          }
          post.setEntity(new StringEntity(json, "UTF-8"))
          Some(post)
        }
      }
    }
  }

  override protected def getInternalTransformer(schema: StructType): PipelineModel =
    {
      validateApiVersion(getApiVersion)
      customGetInternalTransformer(schema, Seq("text", "toLanguage"))
    }

  val toLanguage = new ServiceParam[Seq[String]](this, "toLanguage",
    "Specifies the language of the output text. The target language must be one of the supported languages" +
      " included in the translation scope. For example, use to=de to translate to German. It's possible to translate" +
      " to multiple languages simultaneously by repeating the parameter in the query string. For example, use " +
      "to=de and to=it to translate to German and Italian.",
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

  override def responseDataType: DataType = getApiVersion match {
    case TranslatorApiVersion.V3 => ArrayType(TranslateResponse.schema)
    case TranslatorApiVersion.V2026 => TranslateResponseV2026.schema
  }
}

object Transliterate extends ComplexParamsReadable[Transliterate]

class Transliterate(override val uid: String) extends TextTranslatorBase(uid)
  with TextAsOnlyEntity with SynapseMLLogging {
  logClass(FeatureNames.AiServices.Translate)

  def this() = this(Identifiable.randomUID("Transliterate"))

  def urlPath: String = "transliterate"

  override protected def supportsApiVersion(version: String): Boolean =
    TranslatorApiVersion.Supported(version)

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

  override def responseDataType: DataType = getApiVersion match {
    case TranslatorApiVersion.V3 => ArrayType(TransliterateResponse.schema)
    case TranslatorApiVersion.V2026 => TransliterateResponseV2026.schema
  }
}

object Detect extends ComplexParamsReadable[Detect]

class Detect(override val uid: String) extends TextTranslatorBase(uid)
  with TextAsOnlyEntity with SynapseMLLogging {
  logClass(FeatureNames.AiServices.Translate)

  def this() = this(Identifiable.randomUID("Detect"))

  def urlPath: String = "detect"

  override def responseDataType: DataType = ArrayType(DetectResponse.schema)
}

object BreakSentence extends ComplexParamsReadable[BreakSentence]

class BreakSentence(override val uid: String) extends TextTranslatorBase(uid)
  with TextAsOnlyEntity with SynapseMLLogging {
  logClass(FeatureNames.AiServices.Translate)

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
  with TextAsOnlyEntity with HasFromLanguage with HasToLanguage with SynapseMLLogging {
  logClass(FeatureNames.AiServices.Translate)

  def this() = this(Identifiable.randomUID("DictionaryLookup"))

  def urlPath: String = "dictionary/lookup"

  override def responseDataType: DataType = ArrayType(DictionaryLookupResponse.schema)
}

trait HasTextAndTranslationInput extends HasServiceParams {

  import TranslatorJsonProtocol._

  val textAndTranslation = new ServiceParam[Seq[TextAndTranslation]](
    this, "textAndTranslation", " A string specifying the translated text" +
      " previously returned by the Dictionary lookup operation.")

  def getTextAndTranslation: Seq[TextAndTranslation] = getScalarParam(textAndTranslation)

  def setTextAndTranslation(v: Seq[TextAndTranslation]): this.type = setScalarParam(textAndTranslation, v)

  def setTextAndTranslation(v: TextAndTranslation): this.type = setScalarParam(textAndTranslation, Seq(v))

  def getTextAndTranslationCol: String = getVectorParam(textAndTranslation)

  def setTextAndTranslationCol(v: String): this.type = setVectorParam(textAndTranslation, v)

}

object DictionaryExamples extends ComplexParamsReadable[DictionaryExamples]

class DictionaryExamples(override val uid: String) extends TextTranslatorBase(uid)
  with HasTextAndTranslationInput with HasFromLanguage with HasToLanguage
  with HasCognitiveServiceInput with SynapseMLLogging {
  logClass(FeatureNames.AiServices.Translate)

  def this() = this(Identifiable.randomUID("DictionaryExamples"))

  def urlPath: String = "dictionary/examples"

  override protected def contentType: Row => String = { _ => "application/json; charset=UTF-8" }

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

          val version = getApiVersion
          validateApiVersion(version)
          val base = getUrl + s"?api-version=$version"
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
          addHeaders(post, row)
          getValueOpt(row, subscriptionRegion).foreach(post.setHeader("Ocp-Apim-Subscription-Region", _))

          val json = textAndTranslations.head.getClass.getTypeName match {
            case "com.microsoft.azure.synapse.ml.services.translate.TextAndTranslation" => textAndTranslations.map(
              t => Map("Text" -> t.text, "Translation" -> t.translation)).toJson.compactPrint
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
    {
      validateApiVersion(getApiVersion)
      customGetInternalTransformer(schema, Seq("textAndTranslation"))
    }

  override def responseDataType: DataType = ArrayType(DictionaryExamplesResponse.schema)
}

object Languages extends ComplexParamsReadable[Languages]

class Languages(override val uid: String) extends TextTranslatorBase(uid)
  with HasCognitiveServiceInput with SynapseMLLogging {
  logClass(FeatureNames.AiServices.Translate)

  def this() = this(Identifiable.randomUID("Languages"))

  override def urlPath: String = "languages"

  override protected def supportsApiVersion(version: String): Boolean =
    TranslatorApiVersion.Supported(version)

  val scope = new StringArrayParam(
    this,
    "scope",
    "Translator language groups to return. API 3.0 supports translation, transliteration, and dictionary; " +
      "API 2026-06-06 supports translation, transliteration, and models.",
    values => values.nonEmpty &&
      values.forall(Set("translation", "transliteration", "dictionary", "models")))

  def getScope: Array[String] = $(scope)

  def setScope(v: Array[String]): this.type = set(scope, v)

  def setScope(v: Seq[String]): this.type = set(scope, v.toArray)

  def setScope(v: String): this.type = set(scope, Array(v))

  private def validateScope(version: String, values: Seq[String]): Unit = {
    val allowed = version match {
      case TranslatorApiVersion.V3 => Set("translation", "transliteration", "dictionary")
      case TranslatorApiVersion.V2026 => Set("translation", "transliteration", "models")
    }
    require(values.forall(allowed),
      s"Translator API $version supports these language scopes: ${allowed.toSeq.sorted.mkString(", ")}")
  }

  override protected def inputFunc(schema: StructType): Row => Option[HttpRequestBase] = {
    { row: Row =>
      val version = getApiVersion
      validateApiVersion(version)
      val selectedScope = this.get(scope).map(_.toSeq)
      selectedScope.foreach(validateScope(version, _))
      val query = Seq(Some("api-version" -> version), selectedScope.map("scope" -> _.mkString(","))).flatten
      val request = new HttpGet(getUrl + "?" + URLEncodingUtils.format(query.toMap))
      addHeaders(request, row)
      getValueOpt(row, subscriptionRegion).foreach(request.setHeader("Ocp-Apim-Subscription-Region", _))
      Some(request)
    }
  }

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = { _ => None }

  override protected def getInternalTransformer(schema: StructType): PipelineModel = {
    val version = getApiVersion
    validateApiVersion(version)
    get(scope).foreach(values => validateScope(version, values))
    customGetInternalTransformer(schema, Seq.empty)
  }

  override def responseDataType: DataType = TranslatorLanguagesResponse.schema
}
