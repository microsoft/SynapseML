// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import com.microsoft.ml.spark.core.schema.SparkBindings

// Sentiment schemas

object SentimentResponse extends SparkBindings[TAResponse[SentimentScore]]

case class SentimentScore(id: String, score: Float)

// Detect Language Schemas

object DetectLanguageResponse extends SparkBindings[TAResponse[DetectLanguageScore]]

case class DetectLanguageScore(id: String, detectedLanguages: Seq[DetectedLanguage])

case class DetectedLanguage(name: String, iso6391Name: String, score: Double)

// Detect Entities Schemas

object DetectEntitiesResponse extends SparkBindings[TAResponse[DetectEntitiesScore]]

case class DetectEntitiesScore(id: String, entities: Seq[Entity])

case class Entity(name: String,
                  matches: Seq[Match],
                  wikipediaLanguage: String,
                  wikipediaId: String,
                  wikipediaUrl: String,
                  bingId: String)

case class Match(text: String, offset: Int, length: Int)

// NER Schemas

object LocalNERResponse extends SparkBindings[TAResponse[LocalNERScore]]

case class LocalNERScore(id: String, entities: Seq[LocalNEREntity])

case class LocalNEREntity(value: String,
                          startIndex: Int,
                          precision: Double,
                          category: String)

object NERResponse extends SparkBindings[TAResponse[NERDoc]]

case class NERDoc(id: String, entities: Seq[NEREntity])

case class NEREntity(name: String,
                     matches: Seq[NERMatch],
                     `type`: Option[String],
                     subtype: Option[String],
                     wikipediaLanguage: Option[String],
                     wikipediaId: Option[String],
                     wikipediaUrl: Option[String],
                     bingId: Option[String])

case class NERMatch(text: String,
                    offset: Int,
                    length: Int)

// KeyPhrase Schemas

object KeyPhraseResponse extends SparkBindings[TAResponse[KeyPhraseScore]]

case class KeyPhraseScore(id: String, keyPhrases: Seq[String])
