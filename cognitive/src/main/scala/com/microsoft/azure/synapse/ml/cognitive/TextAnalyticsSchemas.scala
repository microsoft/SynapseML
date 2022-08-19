// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive

import com.microsoft.azure.synapse.ml.core.schema.SparkBindings
import spray.json.RootJsonFormat

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

case class AsyncTAResponse[T <: HasDocId](jobId: String,
                                          lastUpdateDateTime: String,
                                          createdDateTime: String,
                                          expirationDateTime: String,
                                          status: String,
                                          errors: Seq[TAError],
                                          results: TAResponse[T])

case class UnpackedTAResponse[T <: HasDocId](statistics: Option[TAResponseStatistics],
                                             document: Option[T],
                                             error: Option[TAError],
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

object TextSentimentResponse extends SparkBindings[TAResponse[TextSentimentScoredDoc]]

object UnpackedTextSentimentResponse extends SparkBindings[UnpackedTAResponse[TextSentimentScoredDoc]]

object TextSentimentScoredDoc extends SparkBindings[TextSentimentScoredDoc]

case class TextSentimentScoredDoc(id: String,
                                  sentiment: String,
                                  statistics: Option[DocumentStatistics],
                                  confidenceScores: SentimentScore,
                                  sentences: Seq[Sentence],
                                  warnings: Seq[TAWarning]) extends HasDocId

case class SentimentScore(positive: Double, neutral: Double, negative: Double)

case class BinarySentimentScore(positive: Double, negative: Double)

case class SentimentRelation(ref: String, relationType: String)

case class SentimentTarget(confidenceScores: BinarySentimentScore,
                           length: Int,
                           offset: Int,
                           text: Option[String],
                           sentiment: String,
                           relations: Seq[SentimentRelation])

case class SentimentAssessment(confidenceScores: BinarySentimentScore,
                               length: Int,
                               offset: Int,
                               text: Option[String],
                               sentiment: String,
                               isNegated: Boolean)

case class Sentence(text: Option[String],
                    sentiment: String,
                    confidenceScores: SentimentScore,
                    targets: Option[Seq[SentimentTarget]],
                    assessments: Option[Seq[SentimentAssessment]],
                    offset: Int,
                    length: Int)

case class DocumentStatistics(charactersCount: Int, transactionsCount: Int)

// Detect Language Schemas

object LanguageDetectorResponse extends SparkBindings[TAResponse[LanguageDetectorScoredDoc]]

object UnpackedLanguageDetectorResponse extends SparkBindings[UnpackedTAResponse[LanguageDetectorScoredDoc]]

case class LanguageDetectorScoredDoc(id: String,
                                     detectedLanguage: Option[DetectedLanguage],
                                     warnings: Seq[TAWarning],
                                     statistics: Option[DocumentStatistics]) extends HasDocId

case class DetectedLanguage(name: String, iso6391Name: String, confidenceScore: Double)

// Detect Entities Schemas

object EntityDetectorResponse extends SparkBindings[TAResponse[EntityDetectorScoredDoc]]

object UnpackedEntityDetectorResponse extends SparkBindings[UnpackedTAResponse[EntityDetectorScoredDoc]]

case class EntityDetectorScoredDoc(id: String,
                                   entities: Seq[Entity],
                                   warnings: Seq[TAWarning],
                                   statistics: Option[DocumentStatistics]) extends HasDocId

case class Entity(name: String,
                  matches: Seq[Match],
                  language: String,
                  id: Option[String],
                  url: String,
                  dataSource: String)

case class Match(confidenceScore: Double, text: String, offset: Int, length: Int)

// NER Schemas

object NERResponse extends SparkBindings[TAResponse[NERScoredDoc]]

object UnpackedNERResponse extends SparkBindings[UnpackedTAResponse[NERScoredDoc]]

case class NERScoredDoc(id: String,
                        entities: Seq[NEREntity],
                        warnings: Seq[TAWarning],
                        statistics: Option[DocumentStatistics]) extends HasDocId

case class NEREntity(text: String,
                     category: String,
                     subcategory: Option[String] = None,
                     offset: Integer,
                     length: Integer,
                     confidenceScore: Double)

// NER Pii Schemas

object PIIResponse extends SparkBindings[TAResponse[PIIScoredDoc]]

object UnpackedPIIResponse extends SparkBindings[UnpackedTAResponse[PIIScoredDoc]]

case class PIIScoredDoc(id: String,
                        entities: Seq[PIIEntity],
                        redactedText: String,
                        warnings: Seq[TAWarning],
                        statistics: Option[DocumentStatistics]) extends HasDocId

case class PIIEntity(text: String,
                     category: String,
                     subcategory: Option[String] = None,
                     offset: Int,
                     length: Int,
                     confidenceScore: Double)

// KeyPhrase Schemas

object KeyPhraseExtractorResponse extends SparkBindings[TAResponse[KeyPhraseScoredDoc]]

object UnpackedKPEResponse extends SparkBindings[UnpackedTAResponse[KeyPhraseScoredDoc]]

case class KeyPhraseScoredDoc(id: String,
                              keyPhrases: Seq[String],
                              warnings: Seq[TAWarning],
                              statistics: Option[DocumentStatistics]) extends HasDocId


object AnalyzeHealthTextResponse extends SparkBindings[AsyncTAResponse[AnalyzeHealthTextScoredDoc]]

object UnpackedAHTResponse extends SparkBindings[UnpackedTAResponse[AnalyzeHealthTextScoredDoc]]

case class AnalyzeHealthTextScoredDoc(id: String,
                                      entities: Seq[HealthEntity],
                                      relations: Seq[HealthRelation],
                                      warnings: Seq[TAWarning],
                                      statistics: Option[DocumentStatistics]) extends HasDocId

case class HealthEntity(offset: Int,
                        length: Int,
                        text: String,
                        category: String,
                        confidenceScore: Double)

case class HealthRelation(relationType: String,
                          entities: Seq[HealthEntityRef])

case class HealthEntityRef(ref: String, role: String)

case class TAWarning( // Error code.
                      code: String,
                      // Warning message.
                      message: String,
                      // A JSON pointer reference indicating the target object.
                      targetRef: Option[String] = None)
