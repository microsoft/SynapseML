package com.microsoft.ml.spark.cognitive
import com.azure.ai.textanalytics.models.{ExtractKeyPhraseResult, KeyPhrasesCollection, TextAnalyticsRequestOptions,
  TextAnalyticsWarning}
import com.azure.ai.textanalytics.{TextAnalyticsClient, TextAnalyticsClientBuilder}
import com.azure.core.credential.AzureKeyCredential
import com.microsoft.ml.spark.core.contracts.{HasConfidenceScoreCol, HasInputCol}
import com.microsoft.ml.spark.core.schema.SparkBindings
import com.microsoft.ml.spark.io.http.HasErrorCol
import com.microsoft.ml.spark.logging.BasicLogging
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable._
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Transformer}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.JavaConverters._

abstract class TextAnalyticsSDKBase[T](val textAnalyticsOptions: Option[TextAnalyticsRequestOptions] = None)
  extends Transformer
  with HasInputCol with HasErrorCol
  with HasEndpoint with HasSubscriptionKey
  with ComplexParamsWritable with BasicLogging {

  protected val invokeTextAnalytics: String => TAResponseV4[T]

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

    // Creating output schema (input schema + output schema)
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

object TextAnalyticsKeyphraseExtraction extends ComplexParamsReadable[TextAnalyticsKeyphraseExtraction]

class TextAnalyticsKeyphraseExtraction (override val textAnalyticsOptions: Option[TextAnalyticsRequestOptions] = None,
                                     override val uid: String = randomUID("TextAnalyticsKeyphraseExtraction"))
  extends TextAnalyticsSDKBase[KeyphraseV4](textAnalyticsOptions) {
  logClass()

  override def outputSchema: StructType =  KeyPhraseResponseV4.schema

  override protected val invokeTextAnalytics: String => TAResponseV4[KeyphraseV4] = (text: String) =>
  {
    val ExtractKeyPhrasesResultCollection = textAnalyticsClient.extractKeyPhrasesBatch(
      Seq(text).asJava,"en", textAnalyticsOptions.orNull)
    val keyPhraseExtractionResult = ExtractKeyPhrasesResultCollection.asScala.head
    val keyPhraseDocument = keyPhraseExtractionResult.getKeyPhrases()

    val keyphraseResult = if (keyPhraseExtractionResult.isError) {
      None
    } else {
      Some(KeyphraseV4(
        keyPhraseDocument.asScala.toList,
        keyPhraseDocument.getWarnings.asScala.map(
          item => TAWarningV4(item.getWarningCode.toString,item.getMessage)
        ).toList))
    }


    val error = if (keyPhraseExtractionResult.isError) {
      val error = keyPhraseExtractionResult.getError
      Some(TAErrorV4(error.getErrorCode.toString, error.getMessage, error.getTarget))
    } else {
      None
    }

    val stats = Option(keyPhraseExtractionResult.getStatistics) match {
      case Some(s) => Some(DocumentStatistics(s.getCharacterCount, s.getTransactionCount))
      case None => None
    }

    TAResponseV4[KeyphraseV4](
      keyphraseResult,
      error,
      stats,
      Some(ExtractKeyPhrasesResultCollection.getModelVersion))
  }
}

