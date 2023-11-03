// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.language

import com.microsoft.azure.synapse.ml.services.text.{
  DetectedLanguage, DocumentStatistics,
  Sentence, SentimentScore, TADocument, TAWarning
}
import com.microsoft.azure.synapse.ml.core.schema.SparkBindings
import spray.json.RootJsonFormat

// scalastyle:off number.of.types
case class MultiLanguageAnalysisInput(documents: Seq[TADocument])

object MultiLanguageAnalysisInput extends SparkBindings[MultiLanguageAnalysisInput]

case class LanguageDetectionAnalysisInput(documents: Seq[LanguageInput])

object LanguageDetectionAnalysisInput extends SparkBindings[LanguageDetectionAnalysisInput]

case class LanguageInput(countryHint: Option[String], id: String, text: String)

object LanguageInput extends SparkBindings[LanguageInput]

case class EntityTaskParameters(loggingOptOut: Boolean,
                                modelVersion: String,
                                stringIndexType: String)

object EntityTaskParameters extends SparkBindings[EntityTaskParameters]

case class KPnLDTaskParameters(loggingOptOut: Boolean, modelVersion: String)

object KPnLDTaskParameters extends SparkBindings[KPnLDTaskParameters]

case class PiiTaskParameters(domain: String,
                             loggingOptOut: Boolean,
                             modelVersion: String,
                             piiCategories: Option[Seq[String]],
                             stringIndexType: String)

object PiiTaskParameters extends SparkBindings[PiiTaskParameters]

case class SentimentAnalysisTaskParameters(loggingOptOut: Boolean,
                                           modelVersion: String,
                                           opinionMining: Boolean,
                                           stringIndexType: String)

object SentimentAnalysisTaskParameters extends SparkBindings[SentimentAnalysisTaskParameters]

// Entity Recognition schemas

case class EntityRecognitionResponse(kind: String, results: EntityRecognitionResult)

object EntityRecognitionResponse extends SparkBindings[EntityRecognitionResponse]

case class EntityRecognitionResult(documents: Seq[EntityRecognitionDocument],
                                   errors: Option[Seq[ATError]],
                                   modelVersion: String,
                                   statistics: Option[RequestStatistics])

object EntityRecognitionResult extends SparkBindings[EntityRecognitionResult]

case class EntityRecognitionDocument(entities: Seq[Entity],
                                     id: String,
                                     statistics: Option[DocumentStatistics],
                                     warnings: Seq[TAWarning])

object EntityRecognitionDocument extends SparkBindings[EntityRecognitionDocument]

// Language Detection schemas

case class LanguageDetectionResponse(kind: String, results: LanguageDetectionResult)

object LanguageDetectionResponse extends SparkBindings[LanguageDetectionResponse]

case class LanguageDetectionResult(documents: Seq[LanguageDetectionDocumentResult],
                                   errors: Option[Seq[ATError]],
                                   modelVersion: String,
                                   statistics: Option[RequestStatistics])

object LanguageDetectionResult extends SparkBindings[LanguageDetectionResult]

case class LanguageDetectionDocumentResult(detectedLanguage: DetectedLanguage,
                                           id: String,
                                           statistics: Option[DocumentStatistics],
                                           warnings: Seq[TAWarning])

object LanguageDetectionDocumentResult extends SparkBindings[LanguageDetectionDocumentResult]

// Entity Linking schemas

case class EntityLinkingResponse(kind: String, results: EntityLinkingResult)

object EntityLinkingResponse extends SparkBindings[EntityLinkingResponse]

case class EntityLinkingResult(documents: Seq[LinkedEntityDocumentResult],
                               errors: Option[Seq[ATError]],
                               modelVersion: String,
                               statistics: Option[RequestStatistics])

object EntityLinkingResult extends SparkBindings[EntityLinkingResult]

case class LinkedEntityDocumentResult(detectedLanguage: Option[DetectedLanguage],
                                      id: String,
                                      entities: Seq[LinkedEntity],
                                      statistics: Option[DocumentStatistics],
                                      warnings: Seq[TAWarning])

object LinkedEntityDocumentResult extends SparkBindings[LinkedEntityDocumentResult]

// Key Phrase Extraction schemas

case class KeyPhraseExtractionResponse(kind: String, results: KeyPhraseExtractionResult)

object KeyPhraseExtractionResponse extends SparkBindings[KeyPhraseExtractionResponse]

case class KeyPhraseExtractionResult(documents: Seq[KeyPhraseExtractionDocumentResult],
                                     errors: Option[Seq[ATError]],
                                     modelVersion: String,
                                     statistics: Option[RequestStatistics])

object KeyPhraseExtractionResult extends SparkBindings[KeyPhraseExtractionResult]

case class KeyPhraseExtractionDocumentResult(id: String,
                                             keyPhrases: Seq[String],
                                             statistics: Option[DocumentStatistics],
                                             warnings: Seq[TAWarning])

object KeyPhraseExtractionDocumentResult extends SparkBindings[KeyPhraseExtractionDocumentResult]

// PII schemas

case class PIIResponse(kind: String, results: PIIResult)

object PIIResponse extends SparkBindings[PIIResponse]

case class PIIResult(documents: Seq[PIIDocumentResult],
                     errors: Option[Seq[ATError]],
                     modelVersion: String,
                     statistics: Option[RequestStatistics])

object PIIResult extends SparkBindings[PIIResult]

case class PIIDocumentResult(entities: Seq[Entity],
                             id: String,
                             redactedText: String,
                             statistics: Option[DocumentStatistics],
                             warnings: Seq[TAWarning])

object PIIDocumentResult extends SparkBindings[PIIDocumentResult]

// Sentiment schemas

case class SentimentResponse(kind: String, results: SentimentResult)

object SentimentResponse extends SparkBindings[SentimentResponse]

case class SentimentResult(documents: Seq[SentimentDocumentResult],
                           errors: Option[Seq[ATError]],
                           modelVersion: String,
                           statistics: Option[RequestStatistics])

object SentimentResult extends SparkBindings[SentimentResult]

case class SentimentDocumentResult(id: String,
                                   sentiment: String,
                                   statistics: Option[DocumentStatistics],
                                   confidenceScores: SentimentScore,
                                   sentences: Seq[Sentence],
                                   warnings: Seq[TAWarning])

object SentimentDocumentResult extends SparkBindings[SentimentDocumentResult]


case class Entity(category: String,
                  confidenceScore: Double,
                  length: Int,
                  offset: Int,
                  subcategory: Option[String],
                  text: String)

object Entity extends SparkBindings[Entity]

case class LinkedEntity(bingId: String,
                        dataSource: String,
                        id: String,
                        language: String,
                        matches: Seq[Match],
                        name: String,
                        url: String)

object LinkedEntity extends SparkBindings[LinkedEntity]

case class Match(confidenceScore: Double,
                 length: Int,
                 offset: Int,
                 text: String)

object Match extends SparkBindings[Match]

case class RequestStatistics(documentsCount: Int, erroneousDocumentsCount: Int,
                             transactionsCount: Int, validDocumentsCount: Int)

object RequestStatistics extends SparkBindings[RequestStatistics]

case class ATError(id: String, error: DocumentError)

object ATError extends SparkBindings[ATError]

case class DocumentError(code: String,
                         details: Option[Seq[Map[String, String]]], // TODO: resolve the recursive reference
                         innererror: Option[InnerError],
                         message: String,
                         target: Option[String])

case class InnerError(code: Option[String], innerError: Option[String])

object ATJSONFormat {

  import spray.json.DefaultJsonProtocol._
  import com.microsoft.azure.synapse.ml.services.text.TAJSONFormat._

  implicit val LanguageInputFormat: RootJsonFormat[LanguageInput] = jsonFormat3(LanguageInput.apply)
  implicit val LanguageDetectionAnalysisInputFormat: RootJsonFormat[LanguageDetectionAnalysisInput] =
    jsonFormat1(LanguageDetectionAnalysisInput.apply)
  implicit val MultiLanguageAnalysisInputFormat: RootJsonFormat[MultiLanguageAnalysisInput] =
    jsonFormat1(MultiLanguageAnalysisInput.apply)
  implicit val EntityTaskParametersFormat: RootJsonFormat[EntityTaskParameters] =
    jsonFormat3(EntityTaskParameters.apply)
  implicit val KPnLDTaskParametersFormat: RootJsonFormat[KPnLDTaskParameters] =
    jsonFormat2(KPnLDTaskParameters.apply)
  implicit val PiiTaskParametersFormat: RootJsonFormat[PiiTaskParameters] =
    jsonFormat5(PiiTaskParameters.apply)
  implicit val SentimentAnalysisTPFormat: RootJsonFormat[SentimentAnalysisTaskParameters] =
    jsonFormat4(SentimentAnalysisTaskParameters.apply)
}
