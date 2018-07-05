// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.schema.SparkBindings
import spray.json.RootJsonFormat

// General Text Analytics Schemas

case class TADocument(language: Option[String], id: String, text: String)

case class TARequest(documents: Seq[TADocument])

object TARequest extends SparkBindings[TARequest]

case class TAError(id: String, message: String)

case class TAResponse[T](documents: Seq[T], errors: Option[Seq[TAError]])

object TAJSONFormat {

  import spray.json.DefaultJsonProtocol._

  implicit val documentFormat: RootJsonFormat[TADocument] = jsonFormat3(TADocument.apply)
  implicit val requestFormat: RootJsonFormat[TARequest] = jsonFormat1(TARequest.apply)
}

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

object NERResponse extends SparkBindings[TAResponse[NERScore]]

case class NERScore(id: String, entities: Seq[NEREntity])

case class NEREntity(value: String,
                     startIndex: Int,
                     precision: Double,
                     category: String)

// KeyPhrase Schemas

object KeyPhraseResponse extends SparkBindings[TAResponse[KeyPhraseScore]]

case class KeyPhraseScore(id: String, keyPhrases: Seq[String])
