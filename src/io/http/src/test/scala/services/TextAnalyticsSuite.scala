// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

trait TextKey {
  val textKey = sys.env("TEXT_API_KEY")
}

class LanguageDetectorSuite extends TransformerFuzzing[LanguageDetector] with TextKey {

  import session.implicits._

  lazy val df: DataFrame = Seq(
    ("1", "Hello World"),
    ("2", "Bonjour tout le monde"),
    ("3", "La carretera estaba atascada. Había mucho tráfico el día de ayer."),
    ("4", ":) :( :D")
  ).toDF("id", "text")

  lazy val detector: LanguageDetector = new LanguageDetector()
    .setSubscriptionKey(textKey)
    .setUrl("https://eastus.api.cognitive.microsoft.com/text/analytics/v2.0/languages")
    .setIdCol("id")
    .setTextCol("text")
    .setOutputCol("replies")

  test("Basic Usage") {

    val replies = detector.transform(df)
      .withColumn("lang", col("replies.documents").getItem(0)
        .getItem("detectedLanguages").getItem(0)
        .getItem("name"))
      .select("id", "lang")
      .collect().toList

    assert(replies(0).getString(1) == "English" && replies(2).getString(1) == "Spanish")

  }

  override def testObjects(): Seq[TestObject[LanguageDetector]] =
    Seq(new TestObject[LanguageDetector](detector, df))

  override def reader: MLReadable[_] = LanguageDetector
}

class EntityDetectorSuite extends TransformerFuzzing[EntityDetector] with TextKey {

  import session.implicits._

  lazy val df: DataFrame = Seq(
    ("1", "Microsoft released Windows 10"),
    ("2", "In 1975, Bill Gates III and Paul Allen founded the company.")
  ).toDF("id", "text")

  lazy val detector: EntityDetector = new EntityDetector()
    .setSubscriptionKey(textKey)
    .setUrl("https://eastus.api.cognitive.microsoft.com/text/analytics/v2.0/entities")
    .setIdCol("id")
    .setTextCol("text")
    .setLanguage("en")
    .setOutputCol("replies")

  test("Basic Usage") {
    val results = detector.transform(df)
      .withColumn("entities",
        col("replies.documents").getItem(0).getItem("entities").getItem("name"))
      .select("id", "entities").collect().toList
    assert(results.head.getSeq[String](1).toSet == Set("Windows 10", "Microsoft"))
  }

  override def testObjects(): Seq[TestObject[EntityDetector]] =
    Seq(new TestObject[EntityDetector](detector, df))

  override def reader: MLReadable[_] = EntityDetector
}

class TextSentimentSuite extends TransformerFuzzing[TextSentiment] with TextKey {

  import session.implicits._

  lazy val df: DataFrame = Seq(
    ("1", "en", "Hello world. This is some input text that I love."),
    ("2", "fr", "Bonjour tout le monde"),
    ("3", "es", "La carretera estaba atascada. Había mucho tráfico el día de ayer.")

  ).toDF("id", "lang", "text")

  lazy val t: TextSentiment = new TextSentiment()
    .setSubscriptionKey(textKey)
    .setUrl("https://eastus.api.cognitive.microsoft.com/text/analytics/v2.0/sentiment")
    .setIdCol("id")
    .setTextCol("text")
    .setLanguageCol("lang")
    .setOutputCol("replies")

  test("Basic Usage") {
    val results = t.transform(df).withColumn("score",
      col("replies.documents").getItem(0).getItem("score"))
      .select("id", "score").collect().toList

    assert(results(0).getFloat(1) > .5 && results(2).getFloat(1) < .5)
  }

  override def testObjects(): Seq[TestObject[TextSentiment]] =
    Seq(new TestObject[TextSentiment](t, df))

  override def reader: MLReadable[_] = TextSentiment
}

class KeyPhraseExtractorSuite extends TransformerFuzzing[KeyPhraseExtractor] with TextKey {

  import session.implicits._

  lazy val df: DataFrame = Seq(
    ("1", "en", "Hello world. This is some input text that I love."),
    ("2", "fr", "Bonjour tout le monde"),
    ("3", "es", "La carretera estaba atascada. Había mucho tráfico el día de ayer.")
  ).toDF("id", "lang", "text")

  lazy val t: KeyPhraseExtractor = new KeyPhraseExtractor()
    .setSubscriptionKey(textKey)
    .setUrl("https://eastus.api.cognitive.microsoft.com/text/analytics/v2.0/keyPhrases")
    .setIdCol("id")
    .setTextCol("text")
    .setLanguageCol("lang")
    .setOutputCol("replies")

  test("Basic Usage") {
    val results = t.transform(df).withColumn("phrases",
      col("replies.documents").getItem(0).getItem("keyPhrases"))
      .select("id", "phrases").collect().toList

    assert(results(0).getSeq[String](1).toSet === Set("world", "input text"))
    assert(results(2).getSeq[String](1).toSet === Set("carretera", "tráfico", "día"))
  }

  override def testObjects(): Seq[TestObject[KeyPhraseExtractor]] =
    Seq(new TestObject[KeyPhraseExtractor](t, df))

  override def reader: MLReadable[_] = KeyPhraseExtractor
}


