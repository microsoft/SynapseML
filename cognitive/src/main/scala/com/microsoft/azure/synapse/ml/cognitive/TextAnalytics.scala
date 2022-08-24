// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive

import com.microsoft.azure.synapse.ml.core.schema.SparkBindings
import com.microsoft.azure.synapse.ml.io.http.HasHandler
import com.microsoft.azure.synapse.ml.param.{ServiceParam, StringStringMapParam}
import com.microsoft.azure.synapse.ml.stages.{FixedMiniBatchTransformer, FlattenBatch, HasBatchSize, UDFTransformer}
import org.apache.http.client.methods.{HttpPost, HttpRequestBase}
import org.apache.http.entity.{AbstractHttpEntity, StringEntity}
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{ComplexParamsReadable, NamespaceInjections, PipelineModel, Transformer}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import spray.json.DefaultJsonProtocol._
import spray.json._
import scala.collection.JavaConverters._
import java.net.URI

trait HasOpinionMining extends HasServiceParams {
  val includeOpinionMining = new ServiceParam[Boolean](
    this, name = "includeOpinionMining", "includeOpinionMining option")

  def getIncludeOpinionMining: Boolean = $(includeOpinionMining).left.get

  def setIncludeOpinionMining(v: Boolean): this.type = setScalarParam(includeOpinionMining, v)

  def setIncludeOpinionMiningCol(v: String): this.type = setVectorParam(includeOpinionMining, v)

  setDefault(
    includeOpinionMining -> Left(false)
  )
}

trait TextAnalyticsInputParams extends HasServiceParams {
  val text = new ServiceParam[Seq[String]](this, "text", "the text in the request body", isRequired = true)

  def setTextCol(v: String): this.type = setVectorParam(text, v)

  def setText(v: Seq[String]): this.type = setScalarParam(text, v)

  def setText(v: String): this.type = setScalarParam(text, Seq(v))

  setDefault(text -> Right("text"))

  val language = new ServiceParam[Seq[String]](this, "language",
    "the language code of the text (optional for some services)")

  def setLanguageCol(v: String): this.type = setVectorParam(language, v)

  def setLanguage(v: Seq[String]): this.type = setScalarParam(language, v)

  def setLanguage(v: String): this.type = setScalarParam(language, Seq(v))

  //setDefault(language -> Left(Seq("en")))

}

trait HasModelVersion extends HasServiceParams {
  val modelVersion = new ServiceParam[String](
    this, name = "modelVersion", "Version of the model")

  def getModelVersion: String = $(modelVersion).left.get

  def setModelVersion(v: String): this.type = setScalarParam(modelVersion, v)

  def setModelVersionCol(v: String): this.type = setVectorParam(modelVersion, v)

  setDefault(
    modelVersion -> Left("latest")
  )
}

trait TextAnalyticsBaseParams extends HasServiceParams with HasModelVersion {

  val showStats = new ServiceParam[Boolean](
    this, name = "showStats", "Whether to include detailed statistics in the response")

  def getShowStats: Boolean = $(showStats).left.get

  def setShowStats(v: Boolean): this.type = setScalarParam(showStats, v)

  def setShowStatsCol(v: String): this.type = setVectorParam(showStats, v)

  val disableServiceLogs = new ServiceParam[Boolean](
    this, name = "disableServiceLogs", "disableServiceLogs option")

  def getDisableServiceLogs: Boolean = $(disableServiceLogs).left.get

  def setDisableServiceLogs(v: Boolean): this.type = setScalarParam(disableServiceLogs, v)

  def setDisableServiceLogsCol(v: String): this.type = setVectorParam(disableServiceLogs, v)

  setDefault(
    showStats -> Left(false),
    disableServiceLogs -> Left(true)
  )

}

private[ml] abstract class TextAnalyticsBaseNoBinding(uid: String)
  extends CognitiveServicesBaseNoHandler(uid)
    with HasCognitiveServiceInput with HasInternalJsonOutputParser
    with HasSetLocation with HasBatchSize with TextAnalyticsBaseParams
    with TextAnalyticsInputParams {

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = {
    throw new NotImplementedError("Text Analytics models use the " +
      " inputFunc method directly, this method should not be called")
  }

  protected def makeDocuments(row: Row): Seq[TADocument] = {
    val validText = getValue(row, text)
    val langs = getValueOpt(row, language).getOrElse(Seq.fill(validText.length)(""))
    val validLanguages = (if (langs.length == 1) {
      Seq.fill(validText.length)(langs.head)
    } else {
      langs
    }).map(lang => Option(lang).getOrElse(""))
    assert(validLanguages.length == validText.length)
    validText.zipWithIndex.map { case (t, i) =>
      TADocument(Some(validLanguages(i)), i.toString, Option(t).getOrElse(""))
    }
  }

  override protected def inputFunc(schema: StructType): Row => Option[HttpRequestBase] = {
    { row: Row =>
      if (shouldSkip(row)) {
        None
      } else if (getValue(row, text).forall(Option(_).isEmpty)) {
        None
      } else {
        import TAJSONFormat._
        val post = new HttpPost(getUrl)
        getValueOpt(row, subscriptionKey).foreach(post.setHeader("Ocp-Apim-Subscription-Key", _))
        post.setHeader("Content-Type", "application/json")
        val json = TARequest(makeDocuments(row)).toJson.compactPrint
        post.setEntity(new StringEntity(json, "UTF-8"))
        Some(post)
      }
    }
  }

  setDefault(batchSize -> 10)

  protected def shouldAutoBatch(schema: StructType): Boolean = {
    ($(text), get(language)) match {
      case (Left(_), Some(Right(b))) =>
        schema(b).dataType.isInstanceOf[StringType]
      case (Left(_), None) =>
        true
      case (Right(a), Some(Right(b))) =>
        (schema(a).dataType, schema(b).dataType) match {
          case (_: StringType, _: StringType) => true
          case (_: ArrayType, _: ArrayType) => false
          case (_: StringType, _: ArrayType) | (_: ArrayType, _: StringType) =>
            throw new IllegalArgumentException(s"Mismatched column types. " +
              s"Both columns $a and $b need to be StringType (for auto batching)" +
              s" or ArrayType(StringType) (for user batching)")
          case _ =>
            throw new IllegalArgumentException(s"Unknown column types. " +
              s"Both columns $a and $b need to be StringType (for auto batching)" +
              s" or ArrayType(StringType) (for user batching)")
        }
      case (Right(a), _) =>
        schema(a).dataType.isInstanceOf[StringType]
      case _ => false
    }
  }

  protected def postprocessResponse(responseOpt: Row): Option[Seq[Row]] = {
    Option(responseOpt).map { response =>
      val stats = response.getAs[Row]("statistics")
      val docs = response.getAs[Seq[Row]]("documents").map(doc => (doc.getString(0), doc)).toMap
      val errors = response.getAs[Seq[Row]]("errors").map(error => (error.getString(0), error)).toMap
      val modelVersion = response.getAs[String]("modelVersion")
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
    val outputType = ArrayType(
      new StructType()
        .add("statistics", responseType("statistics").dataType)
        .add("document", responseType("documents").dataType.asInstanceOf[ArrayType].elementType)
        .add("error", responseType("errors").dataType.asInstanceOf[ArrayType].elementType)
        .add("modelVersion", responseType("modelVersion").dataType)
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

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}


private[ml] trait HasUnpackedBinding {
  type T <: HasDocId

  def unpackedResponseBinding: SparkBindings[UnpackedTAResponse[T]]

}

private[ml] abstract class TextAnalyticsBase(uid: String)
  extends TextAnalyticsBaseNoBinding(uid) with HasUnpackedBinding {

    protected def responseBinding: SparkBindings[TAResponse[T]]

  override protected def responseDataType: DataType = responseBinding.schema

}

trait HasLanguage extends HasServiceParams {
  val language = new ServiceParam[String](this, "language", "the language to use", isURLParam = true)

  def setLanguageCol(v: String): this.type = setVectorParam(language, v)

  def setLanguage(v: String): this.type = setScalarParam(language, v)
}

trait HasStringIndexType extends HasServiceParams {
  val stringIndexType = new ServiceParam[String](this, "stringIndexType",
    "Specifies the method used to interpret string offsets. " +
      "Defaults to Text Elements (Graphemes) according to Unicode v8.0.0. " +
      "For additional information see https://aka.ms/text-analytics-offsets", isURLParam = true)

  def setStringIndexType(v: String): this.type = setScalarParam(stringIndexType, v)
}

object TextSentiment extends ComplexParamsReadable[TextSentiment]

class TextSentiment(override val uid: String)
  extends TextAnalyticsBase(uid) with HasStringIndexType with HasHandler {
  logClass()

  type T = TextSentimentScoredDoc

  def this() = this(Identifiable.randomUID("TextSentiment"))

  val opinionMining = new ServiceParam[Boolean](this, "opinionMining",
    "if set to true, response will contain not only sentiment prediction but also opinion mining " +
      "(aspect-based sentiment analysis) results.", isURLParam = true)

  def setOpinionMining(v: Boolean): this.type = setScalarParam(opinionMining, v)

  override protected def responseBinding: TextSentimentResponse.type = TextSentimentResponse

  override def unpackedResponseBinding: UnpackedTextSentimentResponse.type = UnpackedTextSentimentResponse

  override def urlPath: String = "/text/analytics/v3.1/sentiment"

  override def inputFunc(schema: StructType): Row => Option[HttpRequestBase] = { r: Row =>
    super.inputFunc(schema)(r).map { request =>
      request.setURI(new URI(prepareUrl(r)))
      request
    }
  }


}

object KeyPhraseExtractor extends ComplexParamsReadable[KeyPhraseExtractor]

class KeyPhraseExtractor(override val uid: String)
  extends TextAnalyticsBase(uid) with HasStringIndexType with HasHandler {
  logClass()

  type T = KeyPhraseScoredDoc

  def this() = this(Identifiable.randomUID("KeyPhraseExtractor"))

  override protected def responseBinding: KeyPhraseExtractorResponse.type = KeyPhraseExtractorResponse

  override def unpackedResponseBinding: UnpackedKPEResponse.type = UnpackedKPEResponse

  override def urlPath: String = "/text/analytics/v3.1/keyPhrases"
}

object NER extends ComplexParamsReadable[NER]

class NER(override val uid: String)
  extends TextAnalyticsBase(uid) with HasStringIndexType with HasHandler {
  logClass()

  type T = NERScoredDoc

  def this() = this(Identifiable.randomUID("NER"))

  override protected def responseBinding: NERResponse.type = NERResponse

  override def unpackedResponseBinding: UnpackedNERResponse.type = UnpackedNERResponse

  override def urlPath: String = "/text/analytics/v3.1/entities/recognition/general"
}

object PII extends ComplexParamsReadable[PII]

class PII(override val uid: String)
  extends TextAnalyticsBase(uid) with HasStringIndexType with HasHandler {
  logClass()

  type T = PIIScoredDoc

  def this() = this(Identifiable.randomUID("PII"))

  val domain = new ServiceParam[String](this, "domain",
    "if specified, will set the PII domain to include only a subset of the entity categories. " +
      "Possible values include: 'PHI', 'none'.", isURLParam = true)
  val piiCategories = new ServiceParam[Seq[String]](this, "piiCategories",
    "describes the PII categories to return", isURLParam = true)

  def setDomain(v: String): this.type = setScalarParam(domain, v)

  def setPiiCategories(v: Seq[String]): this.type = setScalarParam(piiCategories, v)

  override protected def responseBinding: PIIResponse.type = PIIResponse

  override def unpackedResponseBinding: UnpackedPIIResponse.type = UnpackedPIIResponse

  override def urlPath: String = "/text/analytics/v3.1/entities/recognition/pii"
}

object LanguageDetector extends ComplexParamsReadable[LanguageDetector]

class LanguageDetector(override val uid: String)
  extends TextAnalyticsBase(uid) with HasStringIndexType with HasHandler {
  logClass()

  type T = LanguageDetectorScoredDoc

  def this() = this(Identifiable.randomUID("LanguageDetector"))

  override protected def responseBinding: LanguageDetectorResponse.type = LanguageDetectorResponse

  override def unpackedResponseBinding: UnpackedLanguageDetectorResponse.type = UnpackedLanguageDetectorResponse

  override def urlPath: String = "/text/analytics/v3.1/languages"
}

object EntityDetector extends ComplexParamsReadable[EntityDetector]

class EntityDetector(override val uid: String)
  extends TextAnalyticsBase(uid) with HasStringIndexType with HasHandler {
  logClass()

  type T = EntityDetectorScoredDoc

  def this() = this(Identifiable.randomUID("EntityDetector"))

  override protected def responseBinding: EntityDetectorResponse.type = EntityDetectorResponse

  override def unpackedResponseBinding: UnpackedEntityDetectorResponse.type = UnpackedEntityDetectorResponse

  override def urlPath: String = "/text/analytics/v3.1/entities/linking"
}

object AnalyzeHealthText extends ComplexParamsReadable[AnalyzeHealthText]

class AnalyzeHealthText(override val uid: String)
  extends TextAnalyticsBaseNoBinding(uid)
    with HasUnpackedBinding
    with HasStringIndexType with BasicAsyncReply {
  logClass()

  type T = AnalyzeHealthTextScoredDoc

  def this() = this(Identifiable.randomUID("AnalyzeHealthText"))

  override def postprocessResponse(responseOpt: Row): Option[Seq[Row]] = {
    val processedResponseOpt = Option(responseOpt).map { response =>
      response.getAs[Row]("results")
    }
    super.postprocessResponse(processedResponseOpt.orNull)
  }

  override def postprocessResponseUdf: UserDefinedFunction = {
    UDFUtils.oldUdf(postprocessResponse _, ArrayType(UnpackedAHTResponse.schema))
  }

  def unpackedResponseBinding: UnpackedAHTResponse.type = UnpackedAHTResponse

  override def urlPath: String = "/text/analytics/v3.1/entities/health/jobs"

  override protected def responseDataType: DataType = AnalyzeHealthTextResponse.schema

}

object TextAnalyze extends ComplexParamsReadable[TextAnalyze]

class TextAnalyze(override val uid: String) extends TextAnalyticsBaseNoBinding(uid)
  with BasicAsyncReply {
  logClass()

  def this() = this(Identifiable.randomUID("TextAnalyze"))

  val includeEntityRecognition = new BooleanParam(
    this, "includeEntityRecognition", "Whether to perform entity recognition")

  def setIncludeEntityRecognition(v: Boolean): this.type = set(includeEntityRecognition, v)

  def getIncludeEntityRecognition: Boolean = $(includeEntityRecognition)

  val entityRecognitionParams = new StringStringMapParam(
    this,
    "entityRecognitionParams",
    "the parameters to pass to the entity recognition model"
  )

  def getEntityRecognitionParams: Map[String, String] = $(entityRecognitionParams)

  def setEntityRecognitionParams(v: Map[String, String]): this.type = set(entityRecognitionParams, v)

  def setEntityRecognitionParams(v: java.util.HashMap[String, String]): this.type =
    set(entityRecognitionParams, v.asScala.toMap)

  val includePii = new BooleanParam(
    this, "includePii", "Whether to perform PII Detection")

  def setIncludePii(v: Boolean): this.type = set(includePii, v)

  def getIncludePii: Boolean = $(includePii)

  val piiParams = new StringStringMapParam(
    this,
    "piiParams",
    "the parameters to pass to the PII model"
  )

  def getPiiParams: Map[String, String] = $(piiParams)

  def setPiiParams(v: Map[String, String]): this.type = set(piiParams, v)

  def setPiiParams(v: java.util.HashMap[String, String]): this.type =
    set(piiParams, v.asScala.toMap)

  val includeEntityLinking = new BooleanParam(
    this, "includeEntityLinking", "Whether to perform EntityLinking")

  def setIncludeEntityLinking(v: Boolean): this.type = set(includeEntityLinking, v)

  def getIncludeEntityLinking: Boolean = $(includeEntityLinking)

  val entityLinkingParams = new StringStringMapParam(
    this,
    "entityLinkingParams",
    "the parameters to pass to the entityLinking model"
  )

  def getEntityLinkingParams: Map[String, String] = $(entityLinkingParams)

  def setEntityLinkingParams(v: Map[String, String]): this.type = set(entityLinkingParams, v)

  def setEntityLinkingParams(v: java.util.HashMap[String, String]): this.type =
    set(entityLinkingParams, v.asScala.toMap)

  val includeKeyPhraseExtraction = new BooleanParam(
    this, "includeKeyPhraseExtraction", "Whether to perform EntityLinking")

  def setIncludeKeyPhraseExtraction(v: Boolean): this.type = set(includeKeyPhraseExtraction, v)

  def getIncludeKeyPhraseExtraction: Boolean = $(includeKeyPhraseExtraction)

  val keyPhraseExtractionParams = new StringStringMapParam(
    this,
    "keyPhraseExtractionParams",
    "the parameters to pass to the keyPhraseExtraction model"
  )

  def getKeyPhraseExtractionParams: Map[String, String] = $(keyPhraseExtractionParams)

  def setKeyPhraseExtractionParams(v: Map[String, String]): this.type = set(keyPhraseExtractionParams, v)

  def setKeyPhraseExtractionParams(v: java.util.HashMap[String, String]): this.type =
    set(keyPhraseExtractionParams, v.asScala.toMap)

  val includeSentimentAnalysis = new BooleanParam(
    this, "includeSentimentAnalysis", "Whether to perform SentimentAnalysis")

  def setIncludeSentimentAnalysis(v: Boolean): this.type = set(includeSentimentAnalysis, v)

  def getIncludeSentimentAnalysis: Boolean = $(includeSentimentAnalysis)

  val sentimentAnalysisParams = new StringStringMapParam(
    this,
    "sentimentAnalysisParams",
    "the parameters to pass to the sentimentAnalysis model"
  )

  def getSentimentAnalysisParams: Map[String, String] = $(sentimentAnalysisParams)

  def setSentimentAnalysisParams(v: Map[String, String]): this.type = set(sentimentAnalysisParams, v)

  def setSentimentAnalysisParams(v: java.util.HashMap[String, String]): this.type =
    set(sentimentAnalysisParams, v.asScala.toMap)

  setDefault(
    includeEntityRecognition -> true,
    entityRecognitionParams -> Map[String, String]("model-version" -> "latest"),
    includePii -> true,
    piiParams -> Map[String, String]("model-version" -> "latest"),
    includeEntityLinking -> true,
    entityLinkingParams -> Map[String, String]("model-version" -> "latest"),
    includeKeyPhraseExtraction -> true,
    keyPhraseExtractionParams -> Map[String, String]("model-version" -> "latest"),
    includeSentimentAnalysis -> true,
    sentimentAnalysisParams -> Map[String, String]("model-version" -> "latest")
  )

  override def urlPath: String = "/text/analytics/v3.1/analyze"

  override protected def modifyPollingURI(originalURI: URI): URI = {
    // async API allows up to 25 results to be submitted in a batch, but defaults to 20 results per page
    // Add $top=25 to force the full batch in the response
    val originalQuery = originalURI.getQuery
    val newQuery = originalQuery match {
      case "" => "$top=25"
      case _ => "$top=25&" + originalQuery // The API picks up the first value of top so add as a prefix
    }
    new URI(
      originalURI.getScheme,
      originalURI.getUserInfo,
      originalURI.getHost,
      originalURI.getPort,
      originalURI.getPath,
      newQuery,
      originalURI.getFragment
    )
  }

  private def getTaskHelper(include: Boolean, params: Map[String, String]): Seq[TextAnalyzeTask] = {
    Seq(if (include) {
      Some(TextAnalyzeTask(params))
    } else {
      None
    }).flatten
  }

  override protected def inputFunc(schema: StructType): Row => Option[HttpRequestBase] = {
    { row: Row =>
      if (shouldSkip(row)) {
        None
      } else if (getValue(row, text).forall(Option(_).isEmpty)) {
        None
      } else {
        val post = new HttpPost(getUrl)
        getValueOpt(row, subscriptionKey).foreach(post.setHeader("Ocp-Apim-Subscription-Key", _))
        post.setHeader("Content-Type", "application/json")
        val tasks = TextAnalyzeTasks(
          entityRecognitionTasks = getTaskHelper(getIncludeEntityRecognition, getEntityRecognitionParams),
          entityLinkingTasks = getTaskHelper(getIncludeEntityLinking, getEntityLinkingParams),
          entityRecognitionPiiTasks = getTaskHelper(getIncludePii, getPiiParams),
          keyPhraseExtractionTasks = getTaskHelper(getIncludeKeyPhraseExtraction, getKeyPhraseExtractionParams),
          sentimentAnalysisTasks = getTaskHelper(getIncludeSentimentAnalysis, getSentimentAnalysisParams)
        )
        import TAJSONFormat._
        val json = TextAnalyzeRequest(
          "SynapseML", TextAnalyzeInput(makeDocuments(row)), tasks).toJson.compactPrint
        post.setEntity(new StringEntity(json, "UTF-8"))
        Some(post)
      }
    }
  }


  private def flattenTask(tasksOpt: Seq[Row]): Option[Seq[Row]] = {
    Option(tasksOpt).flatMap { tasks =>
      super.postprocessResponse(tasks.head.getAs[Row]("results"))
    }
  }

  override def postprocessResponse(responseOpt: Row): Option[Seq[Row]] = {
    Option(responseOpt).map { response =>
      val tasks = response.getAs[Row]("tasks")
      val flattenedTasks = Seq(
        flattenTask(tasks.getAs[Seq[Row]]("entityRecognitionTasks")),
        flattenTask(tasks.getAs[Seq[Row]]("entityLinkingTasks")),
        flattenTask(tasks.getAs[Seq[Row]]("entityRecognitionPiiTasks")),
        flattenTask(tasks.getAs[Seq[Row]]("keyPhraseExtractionTasks")),
        flattenTask(tasks.getAs[Seq[Row]]("sentimentAnalysisTasks"))
      ).map(ftOpt => ftOpt.map(_.toArray))
      val totalDocs = flattenedTasks.flatten.head.length

      assert(flattenedTasks.flatten.forall(t => t.length == totalDocs))

      val transposed = (0 until totalDocs).map(i =>
        Row.fromSeq(flattenedTasks.map(tOpt => tOpt.map(t => t(i))))
      )
      transposed
    }
  }

  override def postprocessResponseUdf: UserDefinedFunction = {
    UDFUtils.oldUdf(postprocessResponse _, ArrayType(UnpackedTextAnalyzeResponse.schema))
  }

  def unpackedResponseBinding: UnpackedTextAnalyzeResponse.type = UnpackedTextAnalyzeResponse

  override protected def responseDataType: DataType = TextAnalyzeResponse.schema

}
