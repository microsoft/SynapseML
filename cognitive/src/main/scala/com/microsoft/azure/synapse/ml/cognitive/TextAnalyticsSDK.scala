// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive

import com.azure.ai.textanalytics.models._
import com.azure.ai.textanalytics.{TextAnalyticsClient, TextAnalyticsClientBuilder}
import com.azure.core.credential.AzureKeyCredential
import com.azure.core.http.policy.RetryPolicy
import com.azure.core.util.{ClientOptions, Header, Context}
import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.cognitive.SDKConverters._
import com.microsoft.azure.synapse.ml.core.contracts.HasOutputCol
import com.microsoft.azure.synapse.ml.core.schema.SparkBindings
import com.microsoft.azure.synapse.ml.core.utils.AsyncUtils.bufferedAwait
import com.microsoft.azure.synapse.ml.io.http.{ConcurrencyParams, HasErrorCol, HasURL, HeaderValues}
import com.microsoft.azure.synapse.ml.logging.BasicLogging
import com.microsoft.azure.synapse.ml.stages.{FixedMiniBatchTransformer, FlattenBatch, HasBatchSize}
import org.apache.spark.ml.param.{ParamMap, ServiceParam}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import spray.json.DefaultJsonProtocol._

import java.time.temporal.ChronoUnit
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

trait HasOptions extends HasServiceParams {
  val modelVersion = new ServiceParam[String](
    this, name = "modelVersion", "modelVersion option")

  def getModelVersion: String = $(modelVersion).left.get

  def setModelVersion(v: String): this.type = setScalarParam(modelVersion, v)

  def setModelVersionCol(v: String): this.type = setVectorParam(modelVersion, v)

  val includeStatistics = new ServiceParam[Boolean](
    this, name = "includeStatistics", "includeStatistics option")

  def getIncludeStatistics: Boolean = $(includeStatistics).left.get

  def setIncludeStatistics(v: Boolean): this.type = setScalarParam(includeStatistics, v)

  def setIncludeStatisticsCol(v: String): this.type = setVectorParam(includeStatistics, v)

  val disableServiceLogs = new ServiceParam[Boolean](
    this, name = "disableServiceLogs", "disableServiceLogs option")

  def getDisableServiceLogs: Boolean = $(disableServiceLogs).left.get

  def setDisableServiceLogs(v: Boolean): this.type = setScalarParam(disableServiceLogs, v)

  def setDisableServiceLogsCol(v: String): this.type = setVectorParam(disableServiceLogs, v)

  setDefault(
    modelVersion -> Left("latest"),
    includeStatistics -> Left(false),
    disableServiceLogs -> Left(true)
  )

}

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

private[ml] abstract class TextAnalyticsSDKBase[T]()
  extends Transformer with HasErrorCol with HasURL with HasSetLocation with HasSubscriptionKey
    with TextAnalyticsInputParams with HasOutputCol with ConcurrencyParams with HasBatchSize with HasOptions
    with ComplexParamsWritable with BasicLogging {

  override def urlPath: String = ""

  val responseBinding: SparkBindings[TAResponseSDK[T]]

  def invokeTextAnalytics(client: TextAnalyticsClient,
                          text: Seq[String],
                          lang: Seq[String],
                          row: Row
                         ): Seq[TAResponseSDK[T]]

  setDefault(batchSize -> 5)

  protected def transformTextRows(toRow: TAResponseSDK[T] => Row)
                                 (rows: Iterator[Row]): Iterator[Row] = {
    if (rows.hasNext) {
      val key = new AzureKeyCredential("placeholder")
      val client = new TextAnalyticsClientBuilder()
        .retryPolicy(new RetryPolicy("Retry-After", ChronoUnit.SECONDS))
        .clientOptions(new ClientOptions().setHeaders(Seq(
          new Header("User-Agent", s"synapseml/${BuildInfo.version}${HeaderValues.PlatformInfo}")).asJava))
        .credential(key)
        .endpoint(getUrl)
        .buildClient()

      val dur = get(concurrentTimeout)
        .map(ct => Duration.fromNanos((ct * math.pow(10, 9)).toLong)) //scalastyle:ignore magic.number
        .getOrElse(Duration.Inf)

      val futures = rows.map { row =>
        Future {
          val validText = getValue(row, text)
          val langs = getValueOpt(row, language).getOrElse(Seq.fill(validText.length)(""))
          val validLanguages = if (langs.length == 1) {
            Seq.fill(validText.length)(langs.head)
          } else {
            langs
          }
          assert(validLanguages.length == validText.length)

          key.update(getValue(row, subscriptionKey))
          val results = invokeTextAnalytics(client, validText, validLanguages, row)
          Row.fromSeq(row.toSeq ++ Seq(results.map(toRow))) // Adding a new column
        }(ExecutionContext.global)
      }
      bufferedAwait(futures, getConcurrency, dur)(ExecutionContext.global)
    } else {
      Iterator.empty
    }
  }

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

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      val df = dataset.toDF

      val batchedDF = if (shouldAutoBatch(df.schema)) {
        new FixedMiniBatchTransformer().setBatchSize(getBatchSize).transform(df)
      } else {
        df
      }

      val enc = RowEncoder(batchedDF.schema.add(getOutputCol, ArrayType(responseBinding.schema)))
      val toRow = responseBinding.makeToRowConverter
      val resultDF = batchedDF.mapPartitions(transformTextRows(toRow))(enc)

      if (shouldAutoBatch(df.schema)) {
        new FlattenBatch().transform(resultDF)
      } else {
        resultDF
      }
    })
  }

  override def transformSchema(schema: StructType): StructType = {
    if (shouldAutoBatch(schema)) {
      schema.add(getOutputCol, responseBinding.schema)
    } else {
      schema.add(getOutputCol, ArrayType(responseBinding.schema))
    }
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

object LanguageDetectorSDK extends ComplexParamsReadable[LanguageDetectorSDK]

class LanguageDetectorSDK(override val uid: String)
  extends TextAnalyticsSDKBase[DetectedLanguageSDK]() {
  logClass()

  def this() = this(Identifiable.randomUID("LanguageDetectorSDK"))

  override val responseBinding: DetectLanguageResponseSDK.type = DetectLanguageResponseSDK

  override def invokeTextAnalytics(client: TextAnalyticsClient,
                                   input: Seq[String],
                                   hints: Seq[String],
                                   row: Row
                                  ): Seq[TAResponseSDK[DetectedLanguageSDK]] = {
    val documents = (input, hints, input.indices).zipped.map { (doc, hint, i) =>
      new DetectLanguageInput(i.toString, doc, hint)
    }.asJava

    val options = new TextAnalyticsRequestOptions()
      .setModelVersion(getValue(row, modelVersion))
      .setIncludeStatistics(getValue(row, includeStatistics))
      .setServiceLogsDisabled(getValue(row, disableServiceLogs))

    val response = client.detectLanguageBatchWithResponse(documents, options, Context.NONE).getValue
    toResponse(response.asScala)
  }
}

object KeyPhraseExtractorSDK extends ComplexParamsReadable[KeyPhraseExtractorSDK]

class KeyPhraseExtractorSDK(override val uid: String)
  extends TextAnalyticsSDKBase[KeyphraseSDK]() {
  logClass()

  def this() = this(Identifiable.randomUID("KeyPhraseExtractorSDK"))

  override val responseBinding: KeyPhraseResponseSDK.type = KeyPhraseResponseSDK

  override def invokeTextAnalytics(client: TextAnalyticsClient,
                                   input: Seq[String],
                                   lang: Seq[String],
                                   row: Row
                                  ): Seq[TAResponseSDK[KeyphraseSDK]] = {

    val documents = (input, lang, lang.indices).zipped.map { (doc, lang, i) =>
      new TextDocumentInput(i.toString, doc).setLanguage(lang)
    }.asJava
    val options = new TextAnalyticsRequestOptions()
      .setModelVersion(getValue(row, modelVersion))
      .setIncludeStatistics(getValue(row, includeStatistics))
      .setServiceLogsDisabled(getValue(row, disableServiceLogs))

    val response = client.extractKeyPhrasesBatchWithResponse(documents, options, Context.NONE).getValue
    toResponse(response.asScala)
  }
}

object TextSentimentSDK extends ComplexParamsReadable[TextSentiment]

class TextSentimentSDK(override val uid: String)
  extends TextAnalyticsSDKBase[SentimentScoredDocumentSDK]() with HasOpinionMining {
  logClass()

  def this() = this(Identifiable.randomUID("TextSentimentSDK"))

  override val responseBinding: SentimentResponseSDK.type = SentimentResponseSDK

  override def invokeTextAnalytics(client: TextAnalyticsClient,
                                   input: Seq[String],
                                   lang: Seq[String],
                                   row: Row
                                  ): Seq[TAResponseSDK[SentimentScoredDocumentSDK]] = {
    val documents = (input, lang, lang.indices).zipped.map { (doc, lang, i) =>
      new TextDocumentInput(i.toString, doc).setLanguage(lang)
    }.asJava

    val options = new AnalyzeSentimentOptions()
      .setModelVersion(getValue(row, modelVersion))
      .setIncludeStatistics(getValue(row, includeStatistics))
      .setServiceLogsDisabled(getValue(row, disableServiceLogs))
      .setIncludeOpinionMining(getValue(row, includeOpinionMining))

    val response = client.analyzeSentimentBatchWithResponse(documents, options, Context.NONE).getValue
    toResponse(response.asScala)
  }
}

object PIISDK extends ComplexParamsReadable[PII]

class PIISDK(override val uid: String) extends TextAnalyticsSDKBase[PIIEntityCollectionSDK]() {
  logClass()

  def this() = this(Identifiable.randomUID("PIISDK"))

  override val responseBinding: PIIResponseSDK.type = PIIResponseSDK

  override def invokeTextAnalytics(client: TextAnalyticsClient,
                                   input: Seq[String],
                                   lang: Seq[String],
                                   row: Row
                                  ): Seq[TAResponseSDK[PIIEntityCollectionSDK]] = {
    val documents = (input, lang, lang.indices).zipped.map { (doc, lang, i) =>
      new TextDocumentInput(i.toString, doc).setLanguage(lang)
    }.asJava

    val options = new RecognizePiiEntitiesOptions()
      .setModelVersion(getValue(row, modelVersion))
      .setIncludeStatistics(getValue(row, includeStatistics))
      .setServiceLogsDisabled(getValue(row, disableServiceLogs))

    val response = client.recognizePiiEntitiesBatchWithResponse(documents, options, Context.NONE).getValue
    toResponse(response.asScala)
  }
}

object HealthcareSDK extends ComplexParamsReadable[HealthcareSDK]

class HealthcareSDK(override val uid: String) extends TextAnalyticsSDKBase[HealthEntitiesResultSDK]() {
  logClass()

  def this() = this(Identifiable.randomUID("HealthcareSDK"))

  override val responseBinding: HealthcareResponseSDK.type = HealthcareResponseSDK

  override def invokeTextAnalytics(client: TextAnalyticsClient,
                                   input: Seq[String],
                                   lang: Seq[String],
                                   row: Row
                                  ): Seq[TAResponseSDK[HealthEntitiesResultSDK]] = {
    val documents = (input, lang, lang.indices).zipped.map { (doc, lang, i) =>
      new TextDocumentInput(i.toString, doc).setLanguage(lang)
    }.asJava

    val options = new AnalyzeHealthcareEntitiesOptions()
      .setModelVersion(getValue(row, modelVersion))
      .setIncludeStatistics(getValue(row, includeStatistics))
      .setServiceLogsDisabled(getValue(row, disableServiceLogs))

    val poller = client.beginAnalyzeHealthcareEntities(documents, options, Context.NONE)
    poller.waitForCompletion()

    val pagedResults = poller.getFinalResult.asScala
    toResponse(pagedResults.flatMap(_.asScala))
  }
}

object EntityDetectorSDK extends ComplexParamsReadable[EntityDetectorSDK]

class EntityDetectorSDK(override val uid: String) extends TextAnalyticsSDKBase[LinkedEntityCollectionSDK]() {
  logClass()

  def this() = this(Identifiable.randomUID("EntityDetectorSDK"))

  override val responseBinding: LinkedEntityResponseSDK.type = LinkedEntityResponseSDK

  override def invokeTextAnalytics(client: TextAnalyticsClient,
                                   input: Seq[String],
                                   lang: Seq[String],
                                   row: Row
                                  ): Seq[TAResponseSDK[LinkedEntityCollectionSDK]] = {
    val documents = (input, lang, lang.indices).zipped.map { (doc, lang, i) =>
      new TextDocumentInput(i.toString, doc).setLanguage(lang)
    }.asJava

    val options = new TextAnalyticsRequestOptions()
      .setModelVersion(getValue(row, modelVersion))
      .setIncludeStatistics(getValue(row, includeStatistics))
      .setServiceLogsDisabled(getValue(row, disableServiceLogs))

    val response = client.recognizeLinkedEntitiesBatchWithResponse(documents, options, Context.NONE).getValue
    toResponse(response.asScala)
  }
}

object NERSDK extends ComplexParamsReadable[NERSDK]

class NERSDK(override val uid: String) extends TextAnalyticsSDKBase[NERCollectionSDK]() {
  logClass()

  def this() = this(Identifiable.randomUID("NERSDK"))

  override val responseBinding: NERResponseSDK.type = NERResponseSDK

  override def invokeTextAnalytics(client: TextAnalyticsClient,
                                   input: Seq[String],
                                   lang: Seq[String],
                                   row: Row
                                  ): Seq[TAResponseSDK[NERCollectionSDK]] = {
    val documents = (input, lang, lang.indices).zipped.map { (doc, lang, i) =>
      new TextDocumentInput(i.toString, doc).setLanguage(lang)
    }.asJava

    val options = new TextAnalyticsRequestOptions()
      .setModelVersion(getValue(row, modelVersion))
      .setIncludeStatistics(getValue(row, includeStatistics))
      .setServiceLogsDisabled(getValue(row, disableServiceLogs))

    val response = client.recognizeEntitiesBatchWithResponse(documents, options, Context.NONE).getValue
    toResponse(response.asScala)
  }
}
