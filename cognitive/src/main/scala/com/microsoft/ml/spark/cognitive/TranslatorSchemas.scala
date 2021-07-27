// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import com.microsoft.ml.spark.core.schema.SparkBindings
import spray.json._

object TranslateResponse extends SparkBindings[TranslateResponse]

case class TranslateResponse(detectedLanguage: Option[DetectedLanguage],
                             translations: Seq[Translation],
                             sourceText: Option[SourceText])

case class DetectedLanguage(language: String, score: Double)

case class Translation(to: String,
                       text: String,
                       transliteration: Option[Transliteration],
                       alignment: Option[Alignment],
                       sentLen: Option[SentLen])

case class Transliteration(script: String, text: String)

case class Alignment(proj: String)

case class SentLen(srcSentLen: Seq[Int], transSentLen: Seq[Int])

case class SourceText(text: String)

object DetectResponse extends SparkBindings[DetectResponse]

case class DetectResponse(language: String,
                          score: Double,
                          isTranslationSupported: Boolean,
                          isTransliterationSupported: Boolean,
                          alternative: Seq[AlternativeDetectResponse])

case class AlternativeDetectResponse(language: String, score: Double,
                                     isTranslationSupported: Boolean,
                                     isTransliterationSupported: Boolean)

object BreakSentenceResponse extends SparkBindings[BreakSentenceResponse]

case class BreakSentenceResponse(sentLen: Seq[Int],
                                 detectedLanguage: DetectedLanguage)

object TransliterateResponse extends SparkBindings[TransliterateResponse]

case class TransliterateResponse(text: String, script: String)

object DictionaryLookupResponse extends SparkBindings[DictionaryLookupResponse]

case class DictionaryLookupResponse(normalizedSource: String,
                                    displaySource: String,
                                    translations: Seq[DictionaryTranslation])

case class DictionaryTranslation(normalizedTarget: String,
                                 displayTarget: String,
                                 posTag: String,
                                 confidence: Double,
                                 prefixWord: String,
                                 backTranslations: Seq[BackTranslation])

case class BackTranslation(normalizedText: String,
                           displayText: String,
                           numExamples: Int,
                           frequencyCount: Int)

object DictionaryExamplesResponse extends SparkBindings[DictionaryExamplesResponse]

case class DictionaryExamplesResponse(normalizedSource: String,
                                      normalizedTarget: String,
                                      examples: Seq[Example])

case class Example(sourcePrefix: String, sourceTerm: String, sourceSuffix: String,
                   targetPrefix: String, targetTerm: String, targetSuffix: String)

case class DocumentTranslationInput(inputs: Seq[BatchRequest])

case class BatchRequest(source: SourceInput,
                        storageType: Option[String],
                        targets: Seq[TargetInput])

case class SourceInput(filter: Option[DocumentFilter],
                       language: Option[String],
                       sourceUrl: String,
                       storageSource: Option[String])

case class DocumentFilter(prefix: Option[String], suffix: Option[String])

case class TargetInput(category: Option[String],
                       glossaries: Option[Seq[Glossary]],
                       targetUrl: String,
                       language: String,
                       storageSource: Option[String])

case class Glossary(format: String,
                    glossaryUrl: String,
                    storageSource: Option[String],
                    version: Option[String])

object TranslationStatusResponse extends SparkBindings[TranslationStatusResponse]

case class TranslationStatusResponse(id: String,
                                     createdDateTimeUtc: String,
                                     lastActionDateTimeUtc: String,
                                     status: String,
                                     summary: StatusSummary)

case class StatusSummary(total: Int,
                         failed: Int,
                         success: Int,
                         inProgress: Int,
                         notYetStarted: Int,
                         cancelled: Int,
                         totalCharacterCharged: Int)

object TranslatorJsonProtocol extends DefaultJsonProtocol {

  implicit val GlossaryFormat: RootJsonFormat[Glossary] = jsonFormat4(Glossary.apply)

  implicit val TargetInputFormat: RootJsonFormat[TargetInput] = jsonFormat5(TargetInput.apply)

  implicit val DocumentFilterFormat: RootJsonFormat[DocumentFilter] = jsonFormat2(DocumentFilter.apply)

  implicit val SourceInputFormat: RootJsonFormat[SourceInput] = jsonFormat4(SourceInput.apply)

  implicit val BatchRequestFormat: RootJsonFormat[BatchRequest] = jsonFormat3(BatchRequest.apply)

  implicit val DocumentTranslationInputFormat: RootJsonFormat[DocumentTranslationInput] =
    jsonFormat1(DocumentTranslationInput.apply)

}
