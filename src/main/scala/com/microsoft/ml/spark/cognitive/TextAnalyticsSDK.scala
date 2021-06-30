package com.microsoft.ml.spark.cognitive

import com.azure.ai.textanalytics.implementation.models.SentenceOpinionSentiment
import com.azure.ai.textanalytics.models.{AssessmentSentiment, DocumentSentiment, SentenceSentiment, SentimentConfidenceScores, TargetSentiment, TextAnalyticsRequestOptions, TextDocumentInput}
import com.azure.ai.textanalytics.{TextAnalyticsClient, TextAnalyticsClientBuilder}
import com.azure.core.credential.AzureKeyCredential
import com.microsoft.ml.spark.core.contracts.{HasConfidenceScoreCol, HasInputCol}
import com.microsoft.ml.spark.core.schema.{DatasetExtensions, SparkBindings}
import com.microsoft.ml.spark.io.http.{HasErrorCol, SimpleHTTPTransformer}
import com.microsoft.ml.spark.logging.BasicLogging
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.param.{Param, ParamMap, ServiceParam}
import org.apache.spark.ml.util.Identifiable._
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, NamespaceInjections, PipelineModel, Transformer}
import org.apache.spark.sql.functions.{array, col, struct}
import org.apache.spark.sql.types.{ArrayType, DataTypes, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import com.azure.ai.textanalytics.models
import com.microsoft.ml.spark.stages.{DropColumns, Lambda, UDFTransformer}

import java.net.URI
import scala.collection.JavaConverters._

abstract class TextAnalyticsSDKBase[T](val textAnalyticsOptions: Option[TextAnalyticsRequestOptions] = None)
  extends Transformer
  with HasInputCol with HasErrorCol
  with HasEndpoint with HasSubscriptionKey
  with ComplexParamsWritable with BasicLogging {

  protected val invokeTextAnalytics: String => TAResponseV4[T]
  protected val invokeTextAnalyticsBatch: Seq[String] => TAResponseBatchV4[T]

  protected def outputSchema: StructType

  protected lazy val textAnalyticsClient: TextAnalyticsClient =
    new TextAnalyticsClientBuilder()
      .credential(new AzureKeyCredential(getSubscriptionKey))
      .endpoint(getEndpoint)
      .buildClient()

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      val invokeTextAnalyticsUdf = UDFUtils.oldUdf(invokeTextAnalytics, outputSchema)
      val inputColNames = dataset.columns.mkString(",")
      dataset.withColumn("Out", invokeTextAnalyticsUdf(col($(inputCol))))
        .select(inputColNames, "Out.result.*", "Out.error.*", "Out.statistics.*", "Out.*")
        .drop("result", "error", "statistics")
    })
  }

  override def transformSchema(schema: StructType): StructType = {
    // Validate input schema
    val inputType = schema($(inputCol)).dataType
    require(inputType.equals(DataTypes.StringType), s"The input column must be of type String, but got $inputType")
    // Making sure input schema doesn't overlap with output schema
    val fieldsIntersection = schema.map(sf => sf.name.toLowerCase)
      .intersect(outputSchema.map(sf => sf.name.toLowerCase()))
    require(fieldsIntersection.isEmpty, s"Input schema overlaps with transformer output schema. " +
      s"Rename the following input columns: [${fieldsIntersection.mkString(", ")}]")

    //Creating output schema (input schema + output schema)
    val consolidatedSchema = (schema ++ outputSchema).toSet
    StructType(consolidatedSchema.toSeq)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

object TextAnalyticsLanguageDetection extends ComplexParamsReadable[TextAnalyticsLanguageDetection]

class TextAnalyticsLanguageDetection(override val textAnalyticsOptions: Option[TextAnalyticsRequestOptions] = None,
                                     override val uid: String = randomUID("TextAnalyticsLanguageDetection"))
  extends TextAnalyticsSDKBase[DetectedLanguageV4](textAnalyticsOptions)
  with HasConfidenceScoreCol {
  logClass()

  /**
   * Params for optional input column names.
   */
  // Country Hint
  final val countryHintCol: Param[String] = new Param[String](this, "countryHintCol", "country hint column name")

  final def getCountryHintCol: String = $(countryHintCol)

  final def setCountryHintCol(value: String): this.type = set(countryHintCol, value)
  setDefault(countryHintCol -> "CountryHint")

  override def outputSchema: StructType = DetectLanguageResponseV4.schema
  override protected val invokeTextAnalyticsBatch: String => TAResponseBatchV4[DetectedLanguageV4] = (text: String) =>{
    TAResponseBatchV4[DetectedLanguageV4](
     null,
      null,
      null,
      null)
  }

  override protected val invokeTextAnalytics: String => TAResponseV4[DetectedLanguageV4] = (text: String) =>
    {
      val detectLanguageResultCollection = textAnalyticsClient.detectLanguageBatch(
        Seq(text).asJava, null, textAnalyticsOptions.orNull)
      val detectLanguageResult = detectLanguageResultCollection.asScala.head

      val languageResult = if (detectLanguageResult.isError) {
        None
      } else {
        Some(DetectedLanguageV4(
          detectLanguageResult.getPrimaryLanguage.getName,
          detectLanguageResult.getPrimaryLanguage.getIso6391Name,
          detectLanguageResult.getPrimaryLanguage.getConfidenceScore))
      }

      val error = if (detectLanguageResult.isError) {
        val error = detectLanguageResult.getError
        Some(TAErrorV4(error.getErrorCode.toString, error.getMessage, error.getTarget))
      } else {
        None
      }

      val stats = Option(detectLanguageResult.getStatistics) match {
        case Some(s) => Some(DocumentStatistics(s.getCharacterCount, s.getTransactionCount))
        case None => None
      }

      TAResponseV4[DetectedLanguageV4](
        languageResult,
        error,
        stats,
        Some(detectLanguageResultCollection.getModelVersion))
    }
}

object TextSentimentV4 extends ComplexParamsReadable[TextSentimentV4]
class TextSentimentV4(override val textAnalyticsOptions: Option[TextAnalyticsRequestOptions] = None,
                      override val uid: String = randomUID("TextSentimentV4"))
  extends TextAnalyticsSDKBase[SentimentScoredDocumentV4](textAnalyticsOptions)
    with HasConfidenceScoreCol {
  logClass()

  override def outputSchema: StructType = SentimentResponseV4.schema
  override protected val invokeTextAnalytics: String => TAResponseV4[SentimentScoredDocumentV4] = (text: String) =>{
    TAResponseV4[SentimentScoredDocumentV4](
      null,
      null,
      null,
      null)
  }
  override protected val invokeTextAnalyticsBatch: Seq[String] => TAResponseBatchV4[SentimentScoredDocumentV4]=
    (text: Seq[String]) => {
      val document = text.seq.map(item => TextDocumentInput(item, item)
      )

    val textSentimentResultCollection = textAnalyticsClient.analyzeSentimentBatchWithResponse(
      document, null, None)
//if docs > allowed amount split to many calls
    def getConfidenceScore(score: SentimentConfidenceScores): SentimentConfidenceScoreV4 = {
      SentimentConfidenceScoreV4(
        score.getNegative,
        score.getNeutral,
        score.getPositive)
    }

    def getTarget(target: TargetSentiment): TargetV4 = {
      TargetV4(
        target.getText,
        target.getSentiment.toString,
        getConfidenceScore(target.getConfidenceScores),
        target.getOffset,
        target.getLength)
    }

    def getAssessment(assess: AssessmentSentiment): AssessmentV4 = {
      AssessmentV4(
        assess.getText,
        assess.getSentiment.toString,
        getConfidenceScore(assess.getConfidenceScores),
        assess.isNegated,
        assess.getOffset,
        assess.getLength)
    }

    def getSentenceSentiment(sentencesent: SentenceSentiment): SentimentSentenceV4 = {
      SentimentSentenceV4(
        sentencesent.getText,
        sentencesent.getSentiment.toString,
        getConfidenceScore(sentencesent.getConfidenceScores),
        Option(sentencesent.getOpinions).map(sentmap =>
          sentmap.asScala.toList.map(op =>
            OpinionV4(getTarget(op.getTarget)
              ,op.getAssessments.asScala.toList.map(assessment =>
                getAssessment(assessment))))),
        sentencesent.getOffset,
        sentencesent.getLength)
    }

    def getDocumentSentiment(doc: DocumentSentiment): SentimentScoredDocumentV4 = {
      SentimentScoredDocumentV4(
        doc.getSentiment.toString,
        getConfidenceScore(doc.getConfidenceScores),
        doc.getSentences.asScala.toList.map(sentenceSentiment =>
          getSentenceSentiment(sentenceSentiment)),
        doc.getWarnings.asScala.toList.map(warnings =>
          WarningsV4(warnings.getMessage, warnings.getWarningCode.toString)))
    }

    val textSentimentResult = textSentimentResultCollection.asScala.head
    //loop through sentiment result collection
    val documentSentiment = textSentimentResult.getDocumentSentiment();

    val sentimentResult = if (textSentimentResult.isError){
      None
    } else {
      Some(getDocumentSentiment(documentSentiment))
    }

    val error = if(textSentimentResult.isError){
      val error = textSentimentResult.getError
      Some(TAErrorV4(error.getErrorCode.toString, error.getMessage, error.getTarget))
    } else {
      None
    }

    val stats = Option(textSentimentResult.getStatistics).map(s =>
       DocumentStatistics(s.getCharacterCount, s.getTransactionCount))

    TAResponseBatchV4[SentimentScoredDocumentV4](
      sentimentResult,
      error,
      stats,
      Some(textSentimentResultCollection.getModelVersion))
  }
}
object DetectLanguageResponseV4 extends SparkBindings[TAResponseBatchV4[DetectedLanguageV4]]
object SentimentResponseV4 extends SparkBindings[TAResponseBatchV4[SentimentScoredDocumentV4]]

case class TAResponseV4[T](result: Option[T],
                           error: Option[TAErrorV4],
                           statistics: Option[DocumentStatistics],
                           modelVersion: Option[String])

case class TAResponseBatchV4[T](result: List[Option[T]],
                                error: List[Option[TAErrorV4]],
                                statistics: Option[DocumentStatistics],
                                modelVersion: Option[String])

case class TAErrorV4(errorCode: String, errorMessage: String, target: String)

case class DetectedLanguageV4(name: String, iso6391Name: String, confidenceScore: Double)

case class SentimentConfidenceScoreV4(negative: Double, neutral: Double, positive: Double)

case class SentimentScoredDocumentV4(sentiment: String,
                                     confidenceScores: SentimentConfidenceScoreV4,
                                     sentences: List[SentimentSentenceV4],
                                     warnings: List[WarningsV4])

case class SentimentSentenceV4(text: String,
                               sentiment: String,
                               confidenceScores: SentimentConfidenceScoreV4,
                               opinion: Option[List[OpinionV4]],
                               offset: Int,
                               length: Int)

case class OpinionV4(target: TargetV4, assessment: List[AssessmentV4])
case class TargetV4(text: String,
                    sentiment: String,
                    confidenceScores: SentimentConfidenceScoreV4,
                    offset: Int,
                    length: Int)

case class AssessmentV4(text: String,
                        sentiment: String,
                        confidenceScores: SentimentConfidenceScoreV4,
                        isNegated: Boolean,
                        offset: Int,
                        length: Int)

case class WarningsV4(text: String, warningCode: String)

case class TextDocumentInputV4(id: String,
                             text: String,
                             language: Option[String])
