// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive.split1

import com.microsoft.azure.synapse.ml.cognitive._
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.stages.FixedMiniBatchTransformer
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}

class LanguageDetectorSDKSuite extends TransformerFuzzing[LanguageDetectorSDK] with TextEndpoint {

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    "Hello World",
    "Bonjour tout le monde",
    "La carretera estaba atascada. Había mucho tráfico el día de ayer.",
    ":) :( :D"
  ).toDF("text")

  lazy val detector: LanguageDetectorSDK = new LanguageDetectorSDK()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setOutputCol("replies")

  test("Basic Usage") {
    val replies = detector.transform(df)
      .select("replies.result.name")
      .collect().toList
    assert(replies.head.getString(0) == "English" && replies(2).getString(0) == "Spanish")
  }

  test("Manual Batching") {
    val batchedDF = new FixedMiniBatchTransformer().setBatchSize(10).transform(df.coalesce(1))
    val tdf = detector.transform(batchedDF)
    val replies = tdf.collect().head.getAs[Seq[Row]]("replies")
    assert(replies.length == 4)
    val languages = replies.map(_.getAs[Row]("result").getAs[String]("name")).toSet
    assert(languages("Spanish") && languages("English"))
  }

  override def testObjects(): Seq[TestObject[LanguageDetectorSDK]] =
    Seq(new TestObject[LanguageDetectorSDK](detector, df))

  override def reader: MLReadable[_] = LanguageDetectorSDK
}

class EntityDetectorSDKSuite extends TransformerFuzzing[EntityDetectorSDK] with TextEndpoint {

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    ("1", "Microsoft released Windows 10"),
    ("2", "In 1975, Bill Gates III and Paul Allen founded the company.")
  ).toDF("id", "text")

  lazy val detector: EntityDetectorSDK = new EntityDetectorSDK()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setLanguage("en")
    .setOutputCol("replies")

  test("Basic Usage") {
    val results = detector.transform(df)
      .withColumn("entities",
        col("replies.result.entities").getItem("name"))
      .select("id", "entities").collect().toList
    println(results)
    assert(results.head.getSeq[String](1).toSet
      .intersect(Set("Windows 10", "Windows 10 Mobile", "Microsoft")).size == 2)
  }

  override def testObjects(): Seq[TestObject[EntityDetectorSDK]] =
    Seq(new TestObject[EntityDetectorSDK](detector, df))

  override def reader: MLReadable[_] = EntityDetectorSDK
}

class TextSentimentSDKSuite extends TransformerFuzzing[TextSentimentSDK] with TextSentimentBaseSuite {
  lazy val t: TextSentimentSDK = new TextSentimentSDK()
    .setSubscriptionKey(textKey)
    .setLocation("eastus")
    .setLanguageCol("lang")
    .setModelVersion("latest")
    .setIncludeStatistics(true)
    .setOutputCol("replies")

  test("Basic Usage") {
    val results = t.transform(df).select(
      col("replies.result.sentiment")
    ).collect().map(_.getAs[String](0))

    assert(List(4, 5).forall(results(_) == null))
    assert(results(0) == "positive" && results(2) == "negative")
  }

  test("batch usage") {
    val t = new TextSentimentSDK()
      .setSubscriptionKey(textKey)
      .setLocation(textApiLocation)
      .setTextCol("text")
      .setLanguage("en")
      .setOutputCol("score")
    val batchedDF = new FixedMiniBatchTransformer().setBatchSize(10).transform(df.coalesce(1))
    val tdf = t.transform(batchedDF)
    val replies = tdf.collect().head.getAs[Seq[Row]]("score")
    assert(replies.length == 6)
  }

  override def testObjects(): Seq[TestObject[TextSentimentSDK]] =
    Seq(new TestObject[TextSentimentSDK](t, df))

  override def reader: MLReadable[_] = TextSentimentSDK
}

class KeyPhraseExtractorSDKSuite extends TransformerFuzzing[KeyPhraseExtractorSDK] with TextEndpoint {

  import spark.implicits._

  //noinspection ScalaStyle
  lazy val df: DataFrame = Seq(
    ("en", "Hello world. This is some input text that I love."),
    ("fr", "Bonjour tout le monde"),
    ("es", "La carretera estaba atascada. Había mucho tráfico el día de ayer."),
    ("en", null)
  ).toDF("lang", "text")

  lazy val t: KeyPhraseExtractorSDK = new KeyPhraseExtractorSDK()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setLanguageCol("lang")
    .setOutputCol("replies")

  test("Basic Usage") {
    val results = t.transform(df).withColumn("phrases",
      col("replies.result.keyPhrases"))
      .select("phrases").collect().toList

    println(results)

    assert(results.head.getSeq[String](0).toSet === Set("Hello world", "input text"))
    assert(results(2).getSeq[String](0).toSet === Set("mucho tráfico", "día", "carretera", "ayer"))
  }

  override def testObjects(): Seq[TestObject[KeyPhraseExtractorSDK]] =
    Seq(new TestObject[KeyPhraseExtractorSDK](t, df))

  override def reader: MLReadable[_] = KeyPhraseExtractorSDK
}

class NERSDKSuite extends TransformerFuzzing[NERSDK] with TextEndpoint {

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    ("en", "I had a wonderful trip to Seattle last week."),
    ("en", "I visited Space Needle 2 times.")
  ).toDF("language", "text")

  lazy val n: NERSDK = new NERSDK()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setLanguage("en")
    .setOutputCol("response")

  test("Basic Usage") {
    val results = n.transform(df)
    val matches = results.withColumn("match",
      col("response.result.entities")
        .getItem(0))
      .select("match")

    val testRow = matches.collect().head(0).asInstanceOf[GenericRowWithSchema]

    assert(testRow.getAs[String]("text") === "trip")
    assert(testRow.getAs[Int]("offset") === 18)
    assert(testRow.getAs[Int]("length") === 4)
    assert(testRow.getAs[Double]("confidenceScore") > 0.7)
    assert(testRow.getAs[String]("category") === "Event")

  }

  override def testObjects(): Seq[TestObject[NERSDK]] =
    Seq(new TestObject[NERSDK](n, df))

  override def reader: MLReadable[_] = NERSDK
}

class PIISDKSuite extends TransformerFuzzing[PIISDK] with TextEndpoint {

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    ("en", "My SSN is 859-98-0987"),
    ("en", "Your ABA number - 111000025 - is the first 9 digits in the lower left hand corner of check."),
    ("en", "Is 998.214.865-68 your Brazilian CPF number?")
  ).toDF("language", "text")

  lazy val n: PIISDK = new PIISDK()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setLanguage("en")
    .setOutputCol("response")

  test("Basic Usage") {
    val results = n.transform(df)

    val redactedText = results
      .select("response.result.redactedText")
      .collect().head(0).toString
    assert(redactedText === "My SSN is ***********")

    val matches = results.withColumn("match",
      col("response.result.entities").getItem(0))
      .select("match")

    val testRow = matches.collect().head(0).asInstanceOf[GenericRowWithSchema]

    assert(testRow.getAs[String]("text") === "859-98-0987")
    assert(testRow.getAs[Int]("offset") === 10)
    assert(testRow.getAs[Int]("length") === 11)
    assert(testRow.getAs[Double]("confidenceScore") > 0.6)
    assert(testRow.getAs[String]("category") === "USSocialSecurityNumber")
  }

  override def testObjects(): Seq[TestObject[PIISDK]] =
    Seq(new TestObject[PIISDK](n, df))

  override def reader: MLReadable[_] = PIISDK
}

class HealthcareSDKSuite extends TransformerFuzzing[HealthcareSDK] with TextEndpoint {

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    ("en", "20mg of ibuprofen twice a day"),
    ("en", "1tsp of Tylenol every 4 hours"),
    ("en", "6-drops of Vitamin B-12 every evening")
  ).toDF("language", "text")

  lazy val extractor: HealthcareSDK = new HealthcareSDK()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setLanguage("en")
    .setOutputCol("response")

  test("Basic Usage") {
    val results = extractor.transform(df)
    val fromRow = HealthcareResponseSDK.makeFromRowConverter
    val parsed = results.select("response").collect().map(r => fromRow(r.getStruct(0)))
    assert(parsed.head.result.head.entities.head.category === "Dosage")
    assert(parsed.head.result.head.entityRelation.head.relationType === "DosageOfMedication")
  }

  override def testObjects(): Seq[TestObject[HealthcareSDK]] = Seq(new TestObject(
    extractor, df
  ))

  override def reader: MLReadable[_] = HealthcareSDK
}
