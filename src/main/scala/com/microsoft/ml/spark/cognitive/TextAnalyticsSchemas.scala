// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import com.microsoft.ml.spark.core.schema.SparkBindings
import spray.json.RootJsonFormat

// General Text Analytics Schemas

case class TADocument(language: Option[String], id: String, text: String)

object TADocument extends SparkBindings[TADocument]

case class TARequest(documents: Seq[TADocument])

object TARequest extends SparkBindings[TARequest]

case class TAError(id: String, message: String)

object TAError extends SparkBindings[TAError]

case class TAResponse[T](statistics: Option[TAResponseStatistics],
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

}

// SentimentV3 Schemas

object SentimentResponseV3 extends SparkBindings[TAResponse[SentimentScoredDocumentV3]]

case class SentimentScoredDocumentV3(id: String,
                                     sentiment: String,
                                     statistics: Option[DocumentStatistics],
                                     documentScores: SentimentScoreV3,
                                     sentences: Seq[Sentence],
                                     warnings: Seq[TAWarning])

case class SentimentScoreV3(positive: Double, neutral: Double, negative: Double)

case class Sentence(text: Option[String],
                    sentiment: String,
                    confidenceScores: SentimentScoreV3,
                    offset: Int,
                    length: Int)

case class DocumentStatistics(charactersCount: Int, transactionsCount: Int)

// Detect Language Schemas

object DetectLanguageResponseV3 extends SparkBindings[TAResponse[DocumentLanguageV3]]

case class DocumentLanguageV3(id: String,
                              detectedLanguage: Option[DetectedLanguageV3],
                              warnings: Seq[TAWarning],
                              statistics: Option[DocumentStatistics])

case class DetectedLanguageV3(name: String, iso6391Name: String, confidenceScore: Double)

// Detect Entities Schemas

object DetectEntitiesResponseV3 extends SparkBindings[TAResponse[DetectEntitiesScoreV3]]

case class DetectEntitiesScoreV3(id: String,
                               entities: Seq[EntityV3],
                               warnings: Seq[TAWarning],
                               statistics: Option[DocumentStatistics])

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
                    statistics: Option[DocumentStatistics])

case class NEREntityV3(text: String,
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
                            statistics: Option[DocumentStatistics])

case class TAWarning(// Error code.
                    code: String,
                    // Warning message.
                    message: String,
                    // A JSON pointer reference indicating the target object.
                    targetRef: Option[String] = None)
