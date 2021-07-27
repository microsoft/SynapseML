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
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable._
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import java.time.temporal.ChronoUnit
import scala.collection.JavaConverters._
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{ExecutionContext, Future}

abstract class TextAnalyticsSDKBase[T](val textAnalyticsOptions: Option[TextAnalyticsRequestOptionsV4] = None)
  extends Transformer with HasErrorCol with HasURL with HasSetLocation with HasSubscriptionKey
    with TextAnalyticsInputParams with HasOutputCol with ConcurrencyParams with HasBatchSize
    with ComplexParamsWritable with BasicLogging {

  override def urlPath: String = ""

  val responseBinding: SparkBindings[TAResponseV4[T]]

  def invokeTextAnalytics(client: TextAnalyticsClient, text: Seq[String], lang: Seq[String]): TAResponseV4[T]

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
      }.coalesce(1).cache()

      batchedDF.show()

      val enc = RowEncoder(batchedDF.schema.add(getOutputCol, responseBinding.schema))
      val toRow = responseBinding.makeToRowConverter
      val resultDF = batchedDF.mapPartitions(transformTextRows(toRow))(enc)

      //      if (shouldAutoBatch){
      //        new FlattenBatch().transform(resultDF)
      //      } else {
      //        resultDF
      //      }
      resultDF
    })
  }

  override def transformSchema(schema: StructType): StructType = {
    schema.add(getOutputCol, responseBinding.schema)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

object TextAnalyticsLanguageDetection extends ComplexParamsReadable[TextAnalyticsLanguageDetection]

class TextAnalyticsLanguageDetection(override val textAnalyticsOptions: Option[TextAnalyticsRequestOptionsV4] = None,
                                     override val uid: String = randomUID("TextAnalyticsLanguageDetection"))
  extends TextAnalyticsSDKBase[DetectedLanguageV4](textAnalyticsOptions) {
  logClass()

  override val responseBinding: SparkBindings[TAResponseV4[DetectedLanguageV4]] = DetectLanguageResponseV4

  override def invokeTextAnalytics(client: TextAnalyticsClient,
                                   input: Seq[String],
                                   hints: Seq[String]): TAResponseV4[DetectedLanguageV4] = {
    val documents = (input, hints, hints.indices).zipped.map { (doc, hint, i) =>
      new DetectLanguageInput(i.toString, doc, hint)
    }.asJava
    client.detectLanguageBatchWithResponse(documents, null, Context.NONE).getValue
  }
}

object TextAnalyticsKeyphraseExtraction extends ComplexParamsReadable[TextAnalyticsKeyphraseExtraction]

class TextAnalyticsKeyphraseExtraction(override val textAnalyticsOptions: Option[TextAnalyticsRequestOptionsV4] = None,
                                       override val uid: String = randomUID("TextAnalyticsKeyphraseExtraction"))
  extends TextAnalyticsSDKBase[KeyphraseV4](textAnalyticsOptions) {
  logClass()

  override val responseBinding: SparkBindings[TAResponseV4[KeyphraseV4]] = KeyPhraseResponseV4

  override def invokeTextAnalytics(client: TextAnalyticsClient,
                                   input: Seq[String],
                                   lang: Seq[String]): TAResponseV4[KeyphraseV4] = {
    val documents = (input, lang, lang.indices).zipped.map { (doc, lang, i) =>
      new TextDocumentInput(i.toString, doc).setLanguage(lang)
    }.asJava
    client.extractKeyPhrasesBatchWithResponse(documents, null, Context.NONE).getValue
  }

}

object TextSentimentV4 extends ComplexParamsReadable[TextSentimentV4]

class TextSentimentV4(override val textAnalyticsOptions: Option[TextAnalyticsRequestOptionsV4] = None,
                      override val uid: String = randomUID("TextSentimentV4"))
  extends TextAnalyticsSDKBase[SentimentScoredDocumentV4](textAnalyticsOptions) {
  logClass()

  override val responseBinding: SparkBindings[TAResponseV4[SentimentScoredDocumentV4]] = SentimentResponseV4

  override def invokeTextAnalytics(client: TextAnalyticsClient,
                                   input: Seq[String],
                                   lang: Seq[String]): TAResponseV4[SentimentScoredDocumentV4] = {
    val documents = (input, lang, lang.indices).zipped.map { (doc, lang, i) =>
      new TextDocumentInput(i.toString, doc).setLanguage(lang)
    }.asJava
    client.analyzeSentimentBatchWithResponse(documents, null, Context.NONE).getValue
  }

}
