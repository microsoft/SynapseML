// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive

import com.microsoft.azure.synapse.ml.core.schema.SparkBindings

// Sentiment schemas

object SentimentResponseV2 extends SparkBindings[TAResponse[SentimentScoreV2]]

case class SentimentScoreV2(id: String, score: Float)

// Detect Language Schemas

object DetectLanguageResponseV2 extends SparkBindings[TAResponse[DetectLanguageScoreV2]]

case class DetectLanguageScoreV2(id: String, detectedLanguages: Seq[DetectedLanguageV2])

case class DetectedLanguageV2(name: String, iso6391Name: String, score: Double)

// Detect Entities Schemas

object DetectEntitiesResponseV2 extends SparkBindings[TAResponse[DetectEntitiesScoreV2]]

case class DetectEntitiesScoreV2(id: String, entities: Seq[EntityV2])

case class EntityV2(name: String,
                    matches: Seq[Match],
                    wikipediaLanguage: String,
                    wikipediaId: String,
                    wikipediaUrl: String,
                    bingId: String)

case class Match(text: String, offset: Int, length: Int)

// NER Schemas

object LocalNERResponseV2 extends SparkBindings[TAResponse[LocalNERScoreV2]]

case class LocalNERScoreV2(id: String, entities: Seq[LocalNEREntityV2])

case class LocalNEREntityV2(value: String,
                            startIndex: Int,
                            precision: Double,
                            category: String)

object NERResponseV2 extends SparkBindings[TAResponse[NERDocV2]]

case class NERDocV2(id: String, entities: Seq[NEREntityV2])

case class NEREntityV2(name: String,
                       matches: Seq[NERMatchV2],
                       `type`: Option[String],
                       subtype: Option[String],
                       wikipediaLanguage: Option[String],
                       wikipediaId: Option[String],
                       wikipediaUrl: Option[String],
                       bingId: Option[String])

case class NERMatchV2(text: String,
                      offset: Int,
                      length: Int)

// KeyPhrase Schemas

object KeyPhraseResponseV2 extends SparkBindings[TAResponse[KeyPhraseScoreV2]]

case class KeyPhraseScoreV2(id: String, keyPhrases: Seq[String])
