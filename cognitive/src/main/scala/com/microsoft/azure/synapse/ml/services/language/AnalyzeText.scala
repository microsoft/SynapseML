// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.language

import com.microsoft.azure.synapse.ml.logging.{ FeatureNames, SynapseMLLogging }
import com.microsoft.azure.synapse.ml.param.ServiceParam
import com.microsoft.azure.synapse.ml.services._
import com.microsoft.azure.synapse.ml.services.text.{ TADocument, TextAnalyticsAutoBatch }
import com.microsoft.azure.synapse.ml.stages.{ FixedMiniBatchTransformer, FlattenBatch, HasBatchSize, UDFTransformer }
import org.apache.http.entity.{ AbstractHttpEntity, StringEntity }
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.{ ComplexParamsReadable, NamespaceInjections, PipelineModel }
import org.apache.spark.ml.param.{ Param, ParamValidators }
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{ ArrayType, DataType, StructType }
import spray.json._
import spray.json.DefaultJsonProtocol._

trait HasAnalyzeTextServiceBaseParams extends HasServiceParams {
  val modelVersion = new ServiceParam[String](
    this, name = "modelVersion", "Version of the model")

  def getModelVersion: String = getScalarParam(modelVersion)
  def setModelVersion(v: String): this.type = setScalarParam(modelVersion, v)
  def getModelVersionCol: String = getVectorParam(modelVersion)
  def setModelVersionCol(v: String): this.type = setVectorParam(modelVersion, v)



  val loggingOptOut = new ServiceParam[Boolean](
    this, "loggingOptOut", "loggingOptOut for task"
  )

  def setLoggingOptOut(v: Boolean): this.type = setScalarParam(loggingOptOut, v)

  def getLoggingOptOut: Boolean = getScalarParam(loggingOptOut)

  def setLoggingOptOutCol(v: String): this.type = setVectorParam(loggingOptOut, v)

  def getLoggingOptOutCol: String = getVectorParam(loggingOptOut)

  val stringIndexType = new ServiceParam[String](this, "stringIndexType",
                                                 "Specifies the method used to interpret string offsets. " +
                                                   "Defaults to Text Elements(Graphemes) according to Unicode v8.0.0." +
                                                   "For more information see https://aka.ms/text-analytics-offsets",
                                                 isValid = {
                                                   case Left(s) => Set("TextElements_v8",
                                                                       "UnicodeCodePoint",
                                                                       "Utf16CodeUnit")(s)
                                                   case _ => true
                                                 })

  def setStringIndexType(v: String): this.type = setScalarParam(stringIndexType, v)

  def getStringIndexType: String = getScalarParam(stringIndexType)

  def setStringIndexTypeCol(v: String): this.type = setVectorParam(stringIndexType, v)

  def getStringIndexTypeCol: String = getVectorParam(stringIndexType)

  val showStats = new ServiceParam[Boolean](
    this, name = "showStats", "Whether to include detailed statistics in the response",
    isURLParam = true)

  def setShowStats(v: Boolean): this.type = setScalarParam(showStats, v)

  def getShowStats: Boolean = getScalarParam(showStats)

  // We don't support setKindCol here because output schemas for different kind are different
  val kind = new Param[String](
    this, "kind", "Enumeration of supported Text Analysis tasks",
    isValid = validKinds.contains(_)
    )

  protected def validKinds: Set[String]

  def setKind(v: String): this.type = set(kind, v)

  def getKind: String = $(kind)

  setDefault(
    showStats -> Left(false),
    modelVersion -> Left("latest"),
    loggingOptOut -> Left(false),
    stringIndexType -> Left("TextElements_v8")
  )
}


trait AnalyzeTextTaskParameters extends HasAnalyzeTextServiceBaseParams {
  val opinionMining = new ServiceParam[Boolean](
    this, name = "opinionMining", "opinionMining option for SentimentAnalysisTask")

  def setOpinionMining(v: Boolean): this.type = setScalarParam(opinionMining, v)

  def getOpinionMining: Boolean = getScalarParam(opinionMining)

  def setOpinionMiningCol(v: String): this.type = setVectorParam(opinionMining, v)

  def getOpinionMiningCol: String = getVectorParam(opinionMining)

  val domain = new ServiceParam[String](this, "domain",
    "if specified, will set the PII domain to include only a subset of the entity categories. " +
      "Possible values include: 'PHI', 'none'.", isValid = {
      case Left(s) => Set("none", "phi")(s)
      case _ => true
    })

  def setDomain(v: String): this.type = setScalarParam(domain, v)

  def getDomain: String = getScalarParam(domain)

  def setDomainCol(v: String): this.type = setVectorParam(domain, v)

  def getDomainCol: String = getVectorParam(domain)

  val piiCategories = new ServiceParam[Seq[String]](this, "piiCategories",
    "describes the PII categories to return")

  def setPiiCategories(v: Seq[String]): this.type = setScalarParam(piiCategories, v)

  def getPiiCategories: Seq[String] = getScalarParam(piiCategories)

  def setPiiCategoriesCol(v: String): this.type = setVectorParam(piiCategories, v)

  def getPiiCategoriesCol: String = getVectorParam(piiCategories)

  setDefault(
    opinionMining -> Left(false),
    domain -> Left("none")
  )
}

trait HasCountryHint extends HasServiceParams {
  val countryHint = new ServiceParam[Seq[String]](this, "countryHint",
    "the countryHint for language detection")

  def setCountryHint(v: Seq[String]): this.type = setScalarParam(countryHint, v)

  def setCountryHint(v: String): this.type = setScalarParam(countryHint, Seq(v))

  def setCountryHintCol(v: String): this.type = setVectorParam(countryHint, v)

  def getCountryHint: Seq[String] = getScalarParam(countryHint)

  def getCountryHintCol: String = getVectorParam(countryHint)
}

object AnalyzeText extends ComplexParamsReadable[AnalyzeText] with Serializable

class AnalyzeText(override val uid: String) extends CognitiveServicesBase(uid)
  with HasCognitiveServiceInput with HasInternalJsonOutputParser with HasSetLocation
  with HasAPIVersion with HasCountryHint with TextAnalyticsAutoBatch with HasBatchSize
  with AnalyzeTextTaskParameters with SynapseMLLogging {
  logClass(FeatureNames.AiServices.Language)

  def this() = this(Identifiable.randomUID("AnalyzeText"))

  override protected def validKinds: Set[String] = Set("EntityLinking",
                                                       "EntityRecognition",
                                                       "KeyPhraseExtraction",
                                                       "LanguageDetection",
                                                       "PiiEntityRecognition",
                                                       "SentimentAnalysis")

  setDefault(
    apiVersion -> Left("2022-05-01")
  )

  override def urlPath: String = "/language/:analyze-text"

  override private[ml] def internalServiceType: String = "textanalytics"


  override protected def shouldSkip(row: Row): Boolean = if (emptyParamData(row, text)) {
    true
  } else {
    super.shouldSkip(row)
  }

  protected def makeAnalysisInput(row: Row): String = {
    import ATJSONFormat._

    val validText = getValue(row, text)
    getKind match {
      case "LanguageDetection" =>
        val countryHints = getValueOpt(row, countryHint).getOrElse(Seq.fill(validText.length)(""))
        val validCountryHints = (if (countryHints.length == 1) {
          Seq.fill(validText.length)(countryHints.head)
        } else {
          countryHints
        }).map(ch => Option(ch).getOrElse(""))
        assert(validCountryHints.length == validText.length)
        LanguageDetectionAnalysisInput(validText.zipWithIndex.map { case (t, i) =>
          LanguageInput(Some(validCountryHints(i)), i.toString, Option(t).getOrElse(""))
        }).toJson.compactPrint
      case _ =>
        val langs = getValueOpt(row, language).getOrElse(Seq.fill(validText.length)(""))
        val validLanguages = (if (langs.length == 1) {
          Seq.fill(validText.length)(langs.head)
        } else {
          langs
        }).map(lang => Option(lang).getOrElse(""))
        assert(validLanguages.length == validText.length)
        MultiLanguageAnalysisInput(validText.zipWithIndex.map { case (t, i) =>
          TADocument(Some(validLanguages(i)), i.toString, Option(t).getOrElse(""))
        }).toJson.compactPrint
    }
  }

  protected def makeTaskParameters(row: Row): String = {
    import ATJSONFormat._

    getKind match {
      case "EntityLinking" | "EntityRecognition" => EntityTaskParameters(
        getValue(row, loggingOptOut), getValue(row, modelVersion),
        getValue(row, stringIndexType)).toJson.compactPrint
      case "KeyPhraseExtraction" | "LanguageDetection" => KPnLDTaskParameters(
        getValue(row, loggingOptOut), getValue(row, modelVersion)).toJson.compactPrint
      case "PiiEntityRecognition" => PiiTaskParameters(
        getValue(row, domain), getValue(row, loggingOptOut), getValue(row, modelVersion),
        getValueOpt(row, piiCategories), getValue(row, stringIndexType)).toJson.compactPrint
      case "SentimentAnalysis" => SentimentAnalysisTaskParameters(
        getValue(row, loggingOptOut), getValue(row, modelVersion),
        getValue(row, opinionMining), getValue(row, stringIndexType)).toJson.compactPrint
    }
  }

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = row => {
    val analysisInput = makeAnalysisInput(row)
    val parameters = makeTaskParameters(row)
    val body = s"""{"kind": "$getKind", "analysisInput": $analysisInput, "parameters": $parameters}""".stripMargin
    Some(new StringEntity(body, "UTF-8"))
  }

  setDefault(batchSize -> 10)

  protected def postprocessResponse(responseOpt: Row): Option[Seq[Row]] = {
    Option(responseOpt).map { response =>
      val results = response.getAs[Row]("results")
      val stats = results.getAs[Row]("statistics")
      val docs = results.getAs[Seq[Row]]("documents").map(
        doc => (doc.getAs[String]("id"), doc)).toMap
      val errors = results.getAs[Seq[Row]]("errors").map(
        error => (error.getAs[String]("id"), error)).toMap
      val modelVersion = results.getAs[String]("modelVersion")
      (0 until (docs.size + errors.size)).map { i =>
        Row.fromSeq(Seq(
          stats,
          docs.get(i.toString),
          errors.get(i.toString),
          modelVersion
        ))
      }
    }
  }

  protected def postprocessResponseUdf: UserDefinedFunction = {
    val responseType = responseDataType.asInstanceOf[StructType]
    val results = responseType("results").dataType.asInstanceOf[StructType]
    val outputType = ArrayType(
      new StructType()
        .add("statistics", results("statistics").dataType)
        .add("documents", results("documents").dataType.asInstanceOf[ArrayType].elementType)
        .add("errors", results("errors").dataType.asInstanceOf[ArrayType].elementType)
        .add("modelVersion", results("modelVersion").dataType)
    )
    UDFUtils.oldUdf(postprocessResponse _, outputType)
  }

  override protected def getInternalTransformer(schema: StructType): PipelineModel = {

    val batcher = if (shouldAutoBatch(schema)) {
      Some(new FixedMiniBatchTransformer().setBatchSize(getBatchSize))
    } else {
      None
    }
    val newSchema = batcher.map(_.transformSchema(schema)).getOrElse(schema)

    val pipe = super.getInternalTransformer(newSchema)

    val postprocess = new UDFTransformer()
      .setInputCol(getOutputCol)
      .setOutputCol(getOutputCol)
      .setUDF(postprocessResponseUdf)

    val flatten = if (shouldAutoBatch(schema)) {
      Some(new FlattenBatch())
    } else {
      None
    }

    NamespaceInjections.pipelineModel(
      Array(batcher, Some(pipe), Some(postprocess), flatten).flatten
    )
  }

  override protected def responseDataType: DataType = getKind match {
    case "EntityRecognition" => EntityRecognitionResponse.schema
    case "LanguageDetection" => LanguageDetectionResponse.schema
    case "EntityLinking" => EntityLinkingResponse.schema
    case "KeyPhraseExtraction" => KeyPhraseExtractionResponse.schema
    case "PiiEntityRecognition" => PIIResponse.schema
    case "SentimentAnalysis" => SentimentResponse.schema
  }

}
