// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}

trait TextKey {
  lazy val textKey = sys.env("TEXT_API_KEY")
}

class LanguageDetectorSuite extends TransformerFuzzing[LanguageDetector] with TextKey {

  import session.implicits._

  lazy val df: DataFrame = Seq(
    "Hello World",
    "Bonjour tout le monde",
    "La carretera estaba atascada. Había mucho tráfico el día de ayer.",
    ":) :( :D"
  ).toDF("text2")

  lazy val detector: LanguageDetector = new LanguageDetector()
    .setSubscriptionKey(textKey)
    .setUrl("https://eastus.api.cognitive.microsoft.com/text/analytics/v2.0/languages")
    .setTextCol("text2")
    .setOutputCol("replies")

  test("Basic Usage") {
    val replies = detector.transform(df)
      .withColumn("lang", col("replies").getItem(0)
        .getItem("detectedLanguages").getItem(0)
        .getItem("name"))
      .select("lang")
      .collect().toList
    assert(replies(0).getString(0) == "English" && replies(2).getString(0) == "Spanish")
  }

  test("Is serializable ") {
    val replies = detector.transform(df.repartition(3))
      .withColumn("lang", col("replies").getItem(0)
        .getItem("detectedLanguages").getItem(0)
        .getItem("name"))
      .select("text2","lang")
      .sort("text2")
      .collect().toList
    assert(replies(2).getString(1) == "English" && replies(3).getString(1) == "Spanish")
  }

  test("Batch Usage") {
    val batchedDF = new FixedMiniBatchTransformer().setBatchSize(10).transform(df.coalesce(1))
    val tdf = detector.transform(batchedDF)
    val replies = tdf.collect().head.getAs[Seq[Row]]("replies")
    assert(replies.length == 4)
    val languages = replies.map(_.getAs[Seq[Row]]("detectedLanguages").head.getAs[String]("name")).toSet
    assert(languages("Spanish") && languages("English"))
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
    .setLanguage("en")
    .setOutputCol("replies")

  test("Basic Usage") {
    val results = detector.transform(df)
      .withColumn("entities",
        col("replies").getItem(0).getItem("entities").getItem("name"))
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
    ("en", "Hello world. This is some input text that I love."),
    ("fr", "Bonjour tout le monde"),
    ("es", "La carretera estaba atascada. Había mucho tráfico el día de ayer."),
    (null, "ich bin ein berliner"),
    (null, null),
    ("en", null)
  ).toDF("lang", "text")

  lazy val t: TextSentiment = new TextSentiment()
    .setSubscriptionKey(textKey)
    .setUrl("https://eastus.api.cognitive.microsoft.com/text/analytics/v2.0/sentiment")
    .setLanguageCol("lang")
    .setDefaultLanguage("de")
    .setOutputCol("replies")

  test("Basic Usage") {
    val results = t.transform(df).withColumn("score",
      col("replies").getItem(0).getItem("score"))
      .select("score").collect().toList

    assert(List(4,5).forall(results(_).get(0) == null))
    assert(results(0).getFloat(0) > .5 && results(2).getFloat(0) < .5)
  }

  test("batch usage"){
    val t = new TextSentiment()
      .setSubscriptionKey(textKey)
      .setLocation("eastus")
      .setTextCol("text")
      .setLanguage("en")
      .setOutputCol("score")
    val batchedDF = new FixedMiniBatchTransformer().setBatchSize(10).transform(df.coalesce(1))
    val tdf = t.transform(batchedDF)
    val replies = tdf.collect().head.getAs[Seq[Row]]("score")
    assert(replies.length == 6)
  }

  override def testObjects(): Seq[TestObject[TextSentiment]] =
    Seq(new TestObject[TextSentiment](t, df))

  override def reader: MLReadable[_] = TextSentiment
}

class KeyPhraseExtractorSuite extends TransformerFuzzing[KeyPhraseExtractor] with TextKey {

  import session.implicits._

  lazy val df: DataFrame = Seq(
    ("en", "Hello world. This is some input text that I love."),
    ("fr", "Bonjour tout le monde"),
    ("es", "La carretera estaba atascada. Había mucho tráfico el día de ayer."),
    ("en", null)
  ).toDF("lang", "text")

  lazy val t: KeyPhraseExtractor = new KeyPhraseExtractor()
    .setSubscriptionKey(textKey)
    .setUrl("https://eastus.api.cognitive.microsoft.com/text/analytics/v2.0/keyPhrases")
    .setLanguageCol("lang")
    .setOutputCol("replies")

  test("Basic Usage") {
    val results = t.transform(df).withColumn("phrases",
      col("replies").getItem(0).getItem("keyPhrases"))
      .select("phrases").collect().toList

    println(results)

    assert(results(0).getSeq[String](0).toSet === Set("world", "input text"))
    assert(results(2).getSeq[String](0).toSet === Set("carretera", "tráfico", "día"))
  }

  override def testObjects(): Seq[TestObject[KeyPhraseExtractor]] =
    Seq(new TestObject[KeyPhraseExtractor](t, df))

  override def reader: MLReadable[_] = KeyPhraseExtractor
}

class NERSuite extends TransformerFuzzing[NER] with TextKey {
  import session.implicits._

  lazy val df: DataFrame = Seq(
    ("1", "en", "Jeff bought three dozen eggs because there was a 50% discount."),
    ("2", "en", "The Great Depression began in 1929. By 1933, the GDP in America fell by 25%.")
  ).toDF("id", "language", "text")

  lazy val n: NER = new NER()
    .setSubscriptionKey(textKey)
    .setLocation("eastus")
    .setLanguage("en")
    .setOutputCol("response")

  test("Basic Usage") {
    val results = n.transform(df)
    val matches = results.withColumn("match",
      col("response")
        .getItem(0)
        .getItem("entities")
        .getItem("matches")
        .getItem(0)
        .getItem(0))
      .select("match")

    val testRow = matches.collect().head(0).asInstanceOf[GenericRowWithSchema]

    assert(testRow.getAs[String]("text") === "Jeff")
    assert(testRow.getAs[Int]("offset") === 0)
    assert(testRow.getAs[Int]("length") === 4)

  }

  override def testObjects(): Seq[TestObject[NER]] =
    Seq(new TestObject[NER](n, df))

  override def reader: MLReadable[_] = NER
}
