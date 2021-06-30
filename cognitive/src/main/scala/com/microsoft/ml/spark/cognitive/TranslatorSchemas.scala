// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import com.microsoft.ml.spark.core.schema.SparkBindings

object TranslateResponse extends SparkBindings[TranslateResponse]

case class TranslateResponse(detectedLanguage: Option[DetectedLanguage],
                             translations: Array[Translation],
                             sourceText: Option[SourceText])

case class DetectedLanguage(language: String, score: Double)

case class Translation(to: String,
                       text: String,
                       transliteration: Option[Transliteration],
                       alignment: Option[Alignment],
                       sentLen: Option[SentLen])

case class Transliteration(script: String, text: String)

case class Alignment(proj: String)

case class SentLen(srcSentLen: Array[Int], transSentLen: Array[Int])

case class SourceText(text: String)

object DetectResponse extends SparkBindings[DetectResponse]

case class DetectResponse(language: String,
                          score: Double,
                          isTranslationSupported: Boolean,
                          isTransliterationSupported: Boolean,
                          alternative: Array[AlternativeDetectResponse])

case class AlternativeDetectResponse(language: String, score: Double,
                                     isTranslationSupported: Boolean,
                                     isTransliterationSupported: Boolean)

object BreakSentenceResponse extends SparkBindings[BreakSentenceResponse]

case class BreakSentenceResponse(sentLen: Array[Int],
                                 detectedLanguage: DetectedLanguage)

object TransliterateResponse extends SparkBindings[TransliterateResponse]

case class TransliterateResponse(text: String, script: String)

object DictionaryLookupResponse extends SparkBindings[DictionaryLookupResponse]

case class DictionaryLookupResponse(normalizedSource: String,
                                    displaySource: String,
                                    translations: Array[DictionaryTranslation])

case class DictionaryTranslation(normalizedTarget: String,
                                 displayTarget: String,
                                 posTag: String,
                                 confidence: Double,
                                 prefixWord: String,
                                 backTranslations: Array[BackTranslation])

case class BackTranslation(normalizedText: String,
                           displayText: String,
                           numExamples: Int,
                           frequencyCount: Int)

object DictionaryExamplesResponse extends SparkBindings[DictionaryExamplesResponse]

case class DictionaryExamplesResponse(normalizedSource: String,
                                      normalizedTarget: String,
                                      examples: Array[Example])

case class Example(sourcePrefix: String, sourceTerm: String, sourceSuffix: String,
                   targetPrefix: String, targetTerm: String, targetSuffix: String)
