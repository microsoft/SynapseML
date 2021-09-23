// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import com.azure.ai.textanalytics.models._
import com.azure.ai.textanalytics.{TextAnalyticsClient, TextAnalyticsClientBuilder}
import com.azure.core.credential.AzureKeyCredential
import com.azure.core.http.policy.RetryPolicy
import com.azure.core.util.Context
import com.microsoft.ml.spark.cognitive.SDKConverters._
import com.microsoft.ml.spark.core.contracts._
import com.microsoft.ml.spark.core.schema.SparkBindings
import com.microsoft.ml.spark.core.utils.AsyncUtils.bufferedAwait
import com.microsoft.ml.spark.io.http.{ConcurrencyParams, HasErrorCol, HasURL}
import com.microsoft.ml.spark.logging.BasicLogging
import com.microsoft.ml.spark.stages.{FixedMiniBatchTransformer, FlattenBatch, HasBatchSize}
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import java.time.temporal.ChronoUnit
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

trait HasOptions extends HasServiceParams {
  val modelVersion = new Param[String](
    this, name = "modelVersion", "modelVersion option")

  def getModelVersion: Option[String] = get(modelVersion)

  def setModelVersion(v: String): this.type = set(modelVersion, v)

  val includeStatistics = new Param[Boolean](
    this, name = "includeStatistics", "includeStatistics option")

  def getIncludeStatistics: Option[Boolean] = get(includeStatistics)

  def setIncludeStatistics(v: Boolean): this.type = set(includeStatistics, v)

  val disableServiceLogs = new Param[Boolean](
    this, name = "disableServiceLogs", "disableServiceLogs option")

  def getDisableServiceLogs: Option[Boolean] = get(disableServiceLogs)

  def setDisableServiceLogs(v: Boolean): this.type = set(disableServiceLogs, v)

  val includeOpinionMining = new Param[Boolean](
    this, name = "includeOpinionMining", "includeOpinionMining option")

  def getIncludeOpinionMining: Option[Boolean] = get(includeOpinionMining)

  def setIncludeOpinionMining(v: Boolean): this.type = set(includeOpinionMining, v)

}

abstract class TextAnalyticsSDKBase[T]()
  extends Transformer with HasErrorCol with HasURL with HasSetLocation with HasSubscriptionKey
    with TextAnalyticsInputParams with HasOutputCol with ConcurrencyParams with HasBatchSize with HasOptions
    with ComplexParamsWritable with BasicLogging {

  override def urlPath: String = ""

  val responseBinding: SparkBindings[TAResponseV4[T]]

  def invokeTextAnalytics(client: TextAnalyticsClient,
                          text: Seq[String],
                          lang: Seq[String]): TAResponseV4[T]

  setDefault(batchSize -> 5)

  protected def transformTextRows(toRow: TAResponseV4[T] => Row)
                                 (rows: Iterator[Row]): Iterator[Row] = {
    if (rows.hasNext) {
      val client = new TextAnalyticsClientBuilder()
        .retryPolicy(new RetryPolicy("Retry-After", ChronoUnit.SECONDS))
        .credential(new AzureKeyCredential(getSubscriptionKey))
        .endpoint(getUrl)
        .buildClient()

      val dur = get(concurrentTimeout)
        .map(ct => Duration.fromNanos((ct * math.pow(10, 9)).toLong)) //scalastyle:ignore magic.number
        .getOrElse(Duration.Inf)

      val futures = rows.map { row =>
        Future {
          val results = invokeTextAnalytics(client, getValue(row, text), getValue(row, language))
          Row.fromSeq(row.toSeq ++ Seq(toRow(results))) // Adding a new column
        }(ExecutionContext.global)
      }
      bufferedAwait(futures, getConcurrency, dur)(ExecutionContext.global)
    } else {
      Iterator.empty
    }
  }

  private def repackResponse(toRow: TAResponseV4[T] => Row,
                             fromRow: Row => TAResponseV4[T],
                             row: Row): Seq[Row] = {
    val resp = fromRow(row)
    (resp.result, resp.error, resp.statistics).zipped.toSeq.map(tup =>
      toRow(TAResponseV4[T](Seq(tup._1), Seq(tup._2), Seq(tup._3), resp.modelVersion))
    )
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      val df = dataset.toDF
      val shouldAutoBatch = ($(text), $(language)) match {
        case (Right(a), Left(_)) =>
          df.schema(a).dataType.isInstanceOf[StringType]
        case (Left(_), Right(b)) =>
          df.schema(b).dataType.isInstanceOf[StringType]
        case (Right(a), Right(b)) =>
          (df.schema(a).dataType, df.schema(b).dataType) match {
            case (_: StringType, _: StringType) => true
            case (_: ArrayType, _: ArrayType) => false
            case (_: StringType, _: ArrayType) | (_: ArrayType, _: StringType) =>
              throw new IllegalArgumentException(s"Mismatched column types. " +
                s"Both columns $a and $b need to be StringType (for auto batching)" +
                s" or ArrayType(StringType) (for user batching)")
          }
        case _ => false
      }

      val batchedDF = if (shouldAutoBatch) {
        new FixedMiniBatchTransformer().setBatchSize(getBatchSize).transform(df)
      } else {
        df
      }

      val enc = RowEncoder(batchedDF.schema.add(getOutputCol, responseBinding.schema))
      val toRow = responseBinding.makeToRowConverter
      val resultDF = batchedDF.mapPartitions(transformTextRows(toRow))(enc)

      if (shouldAutoBatch) {
        val toRow = responseBinding.makeToRowConverter
        val fromRow = responseBinding.makeFromRowConverter
        val repackResponseUDF = UDFUtils.oldUdf(repackResponse(toRow, fromRow, _), ArrayType(responseBinding.schema))
        new FlattenBatch().transform(resultDF.withColumn(getOutputCol, repackResponseUDF(col(getOutputCol))))
      } else {
        resultDF
      }
    })
  }

  override def transformSchema(schema: StructType): StructType = {
    schema.add(getOutputCol, responseBinding.schema)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

object LanguageDetectionV4 extends ComplexParamsReadable[LanguageDetectionV4]

class LanguageDetectionV4(override val uid: String)
  extends TextAnalyticsSDKBase[DetectedLanguageV4]() {
  logClass()

  def this() = this(Identifiable.randomUID("LanguageDetectionV4"))

  override val responseBinding: SparkBindings[TAResponseV4[DetectedLanguageV4]] = DetectLanguageResponseV4

  override def invokeTextAnalytics(client: TextAnalyticsClient,
                                   input: Seq[String],
                                   hints: Seq[String]): TAResponseV4[DetectedLanguageV4] = {
    val documents = (input, hints, input.indices).zipped.map { (doc, hint, i) =>
      new DetectLanguageInput(i.toString, doc, hint)
    }.asJava

    val options = new TextAnalyticsRequestOptions()
      .setModelVersion(getModelVersion.getOrElse("latest"))
      .setIncludeStatistics(getIncludeStatistics.getOrElse(false))
      .setServiceLogsDisabled(getDisableServiceLogs.getOrElse(false))

    val response = client.detectLanguageBatchWithResponse(documents, options, Context.NONE).getValue
    toResponse(response.asScala, response.getModelVersion)
  }
}

object KeyphraseExtractionV4 extends ComplexParamsReadable[KeyphraseExtractionV4]

class KeyphraseExtractionV4(override val uid: String)
  extends TextAnalyticsSDKBase[KeyphraseV4]() {
  logClass()

  def this() = this(Identifiable.randomUID("KeyphraseExtractionV4"))

  override val responseBinding: SparkBindings[TAResponseV4[KeyphraseV4]] = KeyPhraseResponseV4

  override def invokeTextAnalytics(client: TextAnalyticsClient,
                                   input: Seq[String],
                                   lang: Seq[String]): TAResponseV4[KeyphraseV4] = {
    val documents = (input, lang, lang.indices).zipped.map { (doc, lang, i) =>
      new TextDocumentInput(i.toString, doc).setLanguage(lang)
    }.asJava
    val options = new TextAnalyticsRequestOptions()
      .setModelVersion(getModelVersion.getOrElse("latest"))
      .setIncludeStatistics(getIncludeStatistics.getOrElse(false))
      .setServiceLogsDisabled(getDisableServiceLogs.getOrElse(false))

    val response = client.extractKeyPhrasesBatchWithResponse(documents, options, Context.NONE).getValue
    toResponse(response.asScala, response.getModelVersion)
  }
}

object TextSentimentV4 extends ComplexParamsReadable[TextSentimentV4]

class TextSentimentV4(override val uid: String)
  extends TextAnalyticsSDKBase[SentimentScoredDocumentV4]() {
  logClass()

  def this() = this(Identifiable.randomUID("TextSentimentV4"))

  override val responseBinding: SparkBindings[TAResponseV4[SentimentScoredDocumentV4]] = SentimentResponseV4

  override def invokeTextAnalytics(client: TextAnalyticsClient,
                                   input: Seq[String],
                                   lang: Seq[String]): TAResponseV4[SentimentScoredDocumentV4] = {

    val documents = (input, lang, lang.indices).zipped.map { (doc, lang, i) =>
      new TextDocumentInput(i.toString, doc).setLanguage(lang)
    }.asJava

    val options = new AnalyzeSentimentOptions()
      .setModelVersion(getModelVersion.getOrElse("latest"))
      .setIncludeStatistics(getIncludeStatistics.getOrElse(false))
      .setServiceLogsDisabled(getDisableServiceLogs.getOrElse(false))
      .setIncludeOpinionMining(getIncludeOpinionMining.getOrElse(true))

    val response = client.analyzeSentimentBatchWithResponse(documents, options, Context.NONE).getValue
    toResponse(response.asScala, response.getModelVersion)
  }
}

object PIIV4 extends ComplexParamsReadable[PIIV4]

class PIIV4(override val uid: String) extends TextAnalyticsSDKBase[PIIEntityCollectionV4]() {
  logClass()

  def this() = this(Identifiable.randomUID("PIIV4"))

  override val responseBinding: SparkBindings[TAResponseV4[PIIEntityCollectionV4]] = PIIResponseV4

  override def invokeTextAnalytics(client: TextAnalyticsClient,
                                   input: Seq[String],
                                   lang: Seq[String]): TAResponseV4[PIIEntityCollectionV4] = {
    val documents = (input, lang, lang.indices).zipped.map { (doc, lang, i) =>
      new TextDocumentInput(i.toString, doc).setLanguage(lang)
    }.asJava

    val options = new RecognizePiiEntitiesOptions()
      .setModelVersion(getModelVersion.getOrElse("latest"))
      .setIncludeStatistics(getIncludeStatistics.getOrElse(false))
      .setServiceLogsDisabled(getDisableServiceLogs.getOrElse(false))

    val response = client.recognizePiiEntitiesBatchWithResponse(documents, options, Context.NONE).getValue
    toResponse(response.asScala, response.getModelVersion)
  }
}

object HealthcareV4 extends ComplexParamsReadable[HealthcareV4]

class HealthcareV4(override val uid: String) extends TextAnalyticsSDKBase[HealthEntitiesResultV4]() {
  logClass()

  def this() = this(Identifiable.randomUID("HealthcareV4"))

  override val responseBinding: SparkBindings[TAResponseV4[HealthEntitiesResultV4]] = HealthcareResponseV4

  override def invokeTextAnalytics(client: TextAnalyticsClient,
                                   input: Seq[String],
                                   lang: Seq[String]): TAResponseV4[HealthEntitiesResultV4] = {
    val documents = (input, lang, lang.indices).zipped.map { (doc, lang, i) =>
      new TextDocumentInput(i.toString, doc).setLanguage(lang)
    }.asJava
    val options = new AnalyzeHealthcareEntitiesOptions()
      .setModelVersion(getModelVersion.getOrElse("latest"))
      .setIncludeStatistics(getIncludeStatistics.getOrElse(false))
      .setServiceLogsDisabled(getDisableServiceLogs.getOrElse(false))

    val poller = client.beginAnalyzeHealthcareEntities(documents, options, Context.NONE)
    poller.waitForCompletion()

    val pagedResults = poller.getFinalResult.asScala
    toResponse(pagedResults.flatMap(_.asScala), pagedResults.head.getModelVersion)
  }
}

object EntityLinkingV4 extends ComplexParamsReadable[EntityLinkingV4]

class EntityLinkingV4(override val uid: String) extends TextAnalyticsSDKBase[LinkedEntityCollectionV4]() {
  logClass()

  def this() = this(Identifiable.randomUID("EntityLinkingV4"))

  override val responseBinding: SparkBindings[TAResponseV4[LinkedEntityCollectionV4]] = LinkedEntityResponseV4

  override def invokeTextAnalytics(client: TextAnalyticsClient,
                                   input: Seq[String],
                                   lang: Seq[String]): TAResponseV4[LinkedEntityCollectionV4] = {
    val documents = (input, lang, lang.indices).zipped.map { (doc, lang, i) =>
      new TextDocumentInput(i.toString, doc).setLanguage(lang)
    }.asJava

    val options = new RecognizeLinkedEntitiesOptions()
      .setModelVersion(getModelVersion.getOrElse("latest"))
      .setIncludeStatistics(getIncludeStatistics.getOrElse(false))
      .setServiceLogsDisabled(getDisableServiceLogs.getOrElse(false))

    val response = client.recognizeLinkedEntitiesBatchWithResponse(documents, options, Context.NONE).getValue
    toResponse(response.asScala, response.getModelVersion)
  }
}

object NERV4 extends ComplexParamsReadable[NERV4]

class NERV4(override val uid: String) extends TextAnalyticsSDKBase[NERCollectionV4]() {
  logClass()

  def this() = this(Identifiable.randomUID("NERV4"))

  override val responseBinding: SparkBindings[TAResponseV4[NERCollectionV4]] = NERResponseV4

  override def invokeTextAnalytics(client: TextAnalyticsClient,
                                   input: Seq[String],
                                   lang: Seq[String]): TAResponseV4[NERCollectionV4] = {
    val documents = (input, lang, lang.indices).zipped.map { (doc, lang, i) =>
      new TextDocumentInput(i.toString, doc).setLanguage(lang)
    }.asJava

    val options = new RecognizeEntitiesOptions()
      .setModelVersion(getModelVersion.getOrElse("latest"))
      .setIncludeStatistics(getIncludeStatistics.getOrElse(false))
      .setServiceLogsDisabled(getDisableServiceLogs.getOrElse(false))

    val response = client.recognizeEntitiesBatchWithResponse(documents, options, Context.NONE).getValue
    toResponse(response.asScala, response.getModelVersion)
  }
}
