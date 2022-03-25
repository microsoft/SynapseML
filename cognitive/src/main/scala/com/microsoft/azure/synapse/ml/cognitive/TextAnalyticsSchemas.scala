// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive

import com.microsoft.azure.synapse.ml.core.schema.SparkBindings
import spray.json.RootJsonFormat
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.param.ParamValidators

// General Text Analytics Schemas

case class TADocument(language: Option[String], id: String, text: String)

object TADocument extends SparkBindings[TADocument]

case class TARequest(documents: Seq[TADocument])

object TARequest extends SparkBindings[TARequest]

case class TAError(id: String, error: String)

object TAError extends SparkBindings[TAError]

trait HasDocId {
  val id: String
}

case class TAResponse[T <: HasDocId](statistics: Option[TAResponseStatistics],
                                     documents: Seq[T],
                                     errors: Option[Seq[TAError]],
                                     modelVersion: Option[String])

case class TAResponseStatistics(documentsCount: Int,
                                validDocumentsCount: Int,
                                erroneousDocumentsCount: Int,
                                transactionsCount: Int)

object TAJSONFormat {

  import spray.json.DefaultJsonProtocol._

  implicit val DocumentFormat: RootJsonFormat[TADocument] = jsonFormat3(TADocument.apply)
  implicit val RequestFormat: RootJsonFormat[TARequest] = jsonFormat1(TARequest.apply)
  implicit val AnalysisInputsFormat: RootJsonFormat[TextAnalyzeInput] = jsonFormat1(TextAnalyzeInput.apply)
  implicit val AnalysisTaskFormat: RootJsonFormat[TextAnalyzeTask] = jsonFormat1(TextAnalyzeTask.apply)
  implicit val AnalysisTasksFormat: RootJsonFormat[TextAnalyzeTasks] = jsonFormat5(TextAnalyzeTasks.apply)
  implicit val AnalyzeRequestFormat: RootJsonFormat[TextAnalyzeRequest] = jsonFormat3(TextAnalyzeRequest.apply)
}




// SentimentV3 Schemas

object SentimentResponseV3 extends SparkBindings[TAResponse[SentimentScoredDocumentV3]]

object SentimentScoredDocumentV3 extends SparkBindings[SentimentScoredDocumentV3]

case class SentimentScoredDocumentV3(id: String,
                                     sentiment: String,
                                     statistics: Option[DocumentStatistics],
                                     confidenceScores: SentimentScoreV3,
                                     sentences: Seq[Sentence],
                                     warnings: Seq[TAWarning]) extends HasDocId

case class SentimentScoreV3(positive: Double, neutral: Double, negative: Double)

case class BinarySentimentScoreV3(positive: Double, negative: Double)

case class SentimentRelation(ref: String, relationType: String)

case class SentimentTarget(confidenceScores: BinarySentimentScoreV3,
                           length: Int,
                           offset: Int,
                           text: Option[String],
                           sentiment: String,
                           relations: Seq[SentimentRelation])

case class SentimentAssessment(confidenceScores: BinarySentimentScoreV3,
                               length: Int,
                               offset: Int,
                               text: Option[String],
                               sentiment: String,
                               isNegated: Boolean)

case class Sentence(text: Option[String],
                    sentiment: String,
                    confidenceScores: SentimentScoreV3,
                    targets: Option[Seq[SentimentTarget]],
                    assessments: Option[Seq[SentimentAssessment]],
                    offset: Int,
                    length: Int)

case class DocumentStatistics(charactersCount: Int, transactionsCount: Int)

// Detect Language Schemas

object DetectLanguageResponseV3 extends SparkBindings[TAResponse[DocumentLanguageV3]]

case class DocumentLanguageV3(id: String,
                              detectedLanguage: Option[DetectedLanguageV3],
                              warnings: Seq[TAWarning],
                              statistics: Option[DocumentStatistics]) extends HasDocId

case class DetectedLanguageV3(name: String, iso6391Name: String, confidenceScore: Double)

// Detect Entities Schemas

object DetectEntitiesResponseV3 extends SparkBindings[TAResponse[DetectEntitiesScoreV3]]

case class DetectEntitiesScoreV3(id: String,
                                 entities: Seq[EntityV3],
                                 warnings: Seq[TAWarning],
                                 statistics: Option[DocumentStatistics]) extends HasDocId

case class EntityV3(name: String,
                    matches: Seq[MatchV3],
                    language: String,
                    id: Option[String],
                    url: String,
                    dataSource: String)

case class MatchV3(confidenceScore: Double, text: String, offset: Int, length: Int)

// NER Schemas

object NERResponseV3 extends SparkBindings[TAResponse[NERDocV3]]


case class NERDocV3(id: String,
                    entities: Seq[NEREntityV3],
                    warnings: Seq[TAWarning],
                    statistics: Option[DocumentStatistics]) extends HasDocId

case class NEREntityV3(text: String,
                       category: String,
                       subcategory: Option[String] = None,
                       offset: Integer,
                       length: Integer,
                       confidenceScore: Double)

// NER Pii Schemas

object PIIResponseV3 extends SparkBindings[TAResponse[PIIDocV3]]

case class PIIDocV3(id: String,
                    entities: Seq[PIIEntityV3],
                    redactedText: String,
                    warnings: Seq[TAWarning],
                    statistics: Option[DocumentStatistics]) extends HasDocId

case class PIIEntityV3(text: String,
                       category: String,
                       subcategory: Option[String] = None,
                       offset: Integer,
                       length: Integer,
                       confidenceScore: Double)

// KeyPhrase Schemas

object KeyPhraseResponseV3 extends SparkBindings[TAResponse[KeyPhraseScoreV3]]

case class KeyPhraseScoreV3(id: String,
                            keyPhrases: Seq[String],
                            warnings: Seq[TAWarning],
                            statistics: Option[DocumentStatistics]) extends HasDocId

case class TAWarning( // Error code.
                      code: String,
                      // Warning message.
                      message: String,
                      // A JSON pointer reference indicating the target object.
                      targetRef: Option[String] = None)
