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