// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.text

import com.microsoft.azure.synapse.ml.Secrets
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.stages.{FixedMiniBatchTransformer, FlattenBatch}
import org.apache.spark.SparkException
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}
import org.scalactic.{Equality, TolerantNumerics}

//scalastyle:off null
trait TextEndpoint {
  lazy val textKey: String = sys.env.getOrElse("TEXT_API_KEY", Secrets.CognitiveApiKey)
  lazy val textApiLocation: String = sys.env.getOrElse("TEXT_API_LOCATION", "eastus")
}

trait TATestBase[S <: TextAnalyticsBaseNoBinding with HasUnpackedBinding]
  extends TransformerFuzzing[S] with TextEndpoint {

  def model: S

  def df: DataFrame

  def prepResults(df: DataFrame): Seq[Option[UnpackedTAResponse[S#T]]] = {
    val fromRow = model.unpackedResponseBinding.makeFromRowConverter
    df.collect().map(row => Option(row.getAs[Row](model.getOutputCol)).map(fromRow))
      .toSeq.asInstanceOf[Seq[Option[UnpackedTAResponse[S#T]]]] // This asInstanceOf is necessary!
  }

}

class LanguageDetectorSuite extends TATestBase[LanguageDetector] {

  import spark.implicits._

  override def df: DataFrame = Seq(
    "Hello World",
    "Bonjour tout le monde",
    "La carretera estaba atascada. Había mucho tráfico el día de ayer.",
    ":) :( :D",
    "日本国（にほんこく、にっぽんこく、英: Japan）、または日本（にほん、にっぽん）は、東アジアに位置する民主制国家 [1]。首都は東京都[注 2][2][3]。"
  ).toDF("text")

  override def model: LanguageDetector = new LanguageDetector()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setOutputCol("output")

  test("Basic Usage") {
    val results = prepResults(model.transform(df))
    val langs = results.map(_.get.document.get.detectedLanguage.get.name)
    assert(langs.head == "English")
    assert(langs(1) == "French")
  }

  def versionModel: LanguageDetector = new LanguageDetector()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setModelVersion("latest")
    .setOutputCol("output")

  test("Set Model Version"){
    val results = prepResults(versionModel.transform(df))
    val langs = results.map(_.get.document.get.detectedLanguage.get.name)
    assert(langs.head == "English")
    assert(langs(1) == "French")
    assert(langs(4) == "Japanese")
  }
  override def reader: MLReadable[_] = LanguageDetector

  override def testObjects(): Seq[TestObject[LanguageDetector]] =
    Seq(new TestObject[LanguageDetector](model, df))
}

class EntityDetectorSuite extends TATestBase[EntityDetector] {

  import spark.implicits._

  override def df: DataFrame = Seq(
    ("1", "Microsoft released Windows 10"),
    ("2", "In 1975, Bill Gates III and Paul Allen founded the company.")
  ).toDF("id", "text")

  override def model: EntityDetector = new EntityDetector()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setLanguage("en")
    .setOutputCol("output")

  test("Basic Usage") {
    val results = prepResults(model.transform(df))
    assert(results.head.get.document.get.entities.map(_.name).toSet
      .intersect(Set("Windows 10", "Windows 10 Mobile", "Microsoft")).size == 2)
  }

  override def reader: MLReadable[_] = EntityDetector

  override def testObjects(): Seq[TestObject[EntityDetector]] =
    Seq(new TestObject[EntityDetector](model, df))

}

class TextSentimentSuite extends TATestBase[TextSentiment] {

  import spark.implicits._

  //noinspection ScalaStyle
  override def df: DataFrame = Seq(
    ("en", "Hello world. This is some input text that I love."),
    ("fr", "Bonjour tout le monde"),
    ("es", "La carretera estaba atascada. Había mucho tráfico el día de ayer."),
    (null, "ich bin ein berliner"),
    (null, null),
    ("en", null),
    ("en", "Another piece of text!")
  ).toDF("lang", "text")

  override def model: TextSentiment = new TextSentiment()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setLanguageCol("lang")
    .setModelVersion("latest")
    .setShowStats(true)
    .setOutputCol("output")

  test("Basic Usage") {
    val results = prepResults(model.transform(df))
    assert(results.head.get.document.get.sentiment == "positive" &&
      results(2).get.document.get.sentiment == "negative")
  }

  test("Automatic batching") {
    val results = prepResults(model.transform(df.coalesce(1)))
    assert(List(4, 5).forall(results(_).get.document.isEmpty))
    assert(results.head.get.document.get.sentiment == "positive" &&
      results(2).get.document.get.sentiment == "negative")
  }

  test("Opinion Mining") {
    val results = prepResults(model.setOpinionMining(true).transform(df))
    assert(results.head.get.document.get.sentences(1).assessments.get.head.sentiment == "positive")
  }

  test("Manual batching") {
    val batchedDF = new FixedMiniBatchTransformer().setBatchSize(10).transform(df.coalesce(1))
    val fromRow = UnpackedTextSentimentResponse.makeFromRowConverter
    val replies = model.transform(batchedDF)
      .collect().map(row => row.getSeq[Row](3).map(fromRow)).head
    assert(replies.length == 7)
    assert(replies.head.document.get.sentiment == "positive")
  }

  override def reader: MLReadable[_] = TextSentiment

  override def testObjects(): Seq[TestObject[TextSentiment]] =
    Seq(new TestObject[TextSentiment](model, df))

}

class KeyPhraseExtractorSuite extends TATestBase[KeyPhraseExtractor] {

  import spark.implicits._

  //noinspection ScalaStyle
  override def df: DataFrame = Seq(
    ("en", "Hello world. This is some input text that I love."),
    ("fr", "Bonjour tout le monde"),
    ("es", "La carretera estaba atascada. Había mucho tráfico el día de ayer."),
    ("en", null)
  ).toDF("lang", "text")

  override def model: KeyPhraseExtractor = new KeyPhraseExtractor()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setLanguageCol("lang")
    .setOutputCol("output")

  test("Basic Usage") {
    val results = prepResults(model.transform(df))
    val keyPhrases = results.take(3).map(_.get.document.get.keyPhrases.toSet)
    assert(keyPhrases.head === Set("Hello world", "input text"))
    assert(keyPhrases(2) === Set("mucho tráfico", "día", "carretera", "ayer"))
  }

  override def reader: MLReadable[_] = KeyPhraseExtractor

  override def testObjects(): Seq[TestObject[KeyPhraseExtractor]] =
    Seq(new TestObject[KeyPhraseExtractor](model, df))

}

class NERSuite extends TATestBase[NER] {

  import spark.implicits._

  override def df: DataFrame = Seq(
    ("en", "I had a wonderful trip to Seattle last week."),
    ("en", "I visited Space Needle 2 times.")
  ).toDF("language", "text")

  override def model: NER = new NER()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setLanguage("en")
    .setOutputCol("output")

  test("Basic Usage") {
    val results = prepResults(model.transform(df))
    val testEntity = results.head.get.document.get.entities.head
    assert(testEntity.text === "trip")
    assert(testEntity.offset === 18)
    assert(testEntity.length === 4)
    assert(testEntity.confidenceScore > 0.5)
    assert(testEntity.category === "Event")
  }

  override def reader: MLReadable[_] = NER

  override def testObjects(): Seq[TestObject[NER]] =
    Seq(new TestObject[NER](model, df))
}

class PIISuite extends TATestBase[PII] {

  import spark.implicits._

  override def df: DataFrame = Seq(
    ("en", "My SSN is 859-98-0987"),
    ("en", "Your ABA number - 111000025 - is the first 9 digits in the lower left hand corner."),
    ("en", "Is 998.214.865-68 your Brazilian CPF number?")
  ).toDF("language", "text")

  override def model: PII = new PII()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setLanguage("en")
    .setOutputCol("output")

  test("Basic Usage") {
    val results = prepResults(model.transform(df))
    val testDoc = results.map(_.get.document.get).head
    val testEntity = testDoc.entities.head
    assert(testDoc.redactedText === "My SSN is ***********")
    assert(testEntity.text === "859-98-0987")
    assert(testEntity.offset === 10)
    assert(testEntity.length === 11)
    assert(testEntity.confidenceScore > 0.6)
    assert(testEntity.category.contains("Number"))
  }

  override def reader: MLReadable[_] = PII

  override def testObjects(): Seq[TestObject[PII]] =
    Seq(new TestObject[PII](model, df))

}

class AnalyzeHealthTextSuite extends TATestBase[AnalyzeHealthText] {

  import spark.implicits._

  override def df: DataFrame = Seq(
    ("en", "20mg of ibuprofen twice a day"),
    ("en", "1tsp of Tylenol every 4 hours"),
    ("en", "6-drops of Vitamin B-12 every evening"),
    ("en", null),
    ("en", "7-drops of Vitamin D every evening")
  ).toDF("language", "text")

  override def model: AnalyzeHealthText = new AnalyzeHealthText()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setLanguage("en")
    .setOutputCol("output")

  test("Basic Usage") {
    val results = prepResults(model.transform(df.coalesce(1)))
    assert(results.head.get.document.get.entities.head.category == "Dosage")
    assert(results(3).get.document.isEmpty)
    assert(results(4).get.document.get.entities.head.category == "Dosage")
  }

  override def reader: MLReadable[_] = AnalyzeHealthText

  override def testObjects(): Seq[TestObject[AnalyzeHealthText]] =
    Seq(new TestObject[AnalyzeHealthText](model, df))

}

//class TextAnalyzeSuite extends TransformerFuzzing[TextAnalyze] with TextEndpoint {
//
//  import spark.implicits._
//
//  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-3)
//
//  def df: DataFrame = Seq(
//    ("en", "I had a wonderful trip to Seattle last week and visited Microsoft."),
//    ("en", "Another document bites the dust"),
//    (null, "ich bin ein berliner"),
//    (null, null),
//    ("en", null),
//    ("invalid", "This is irrelevant as the language is invalid")
//  ).toDF("language", "text")
//
//  def model: TextAnalyze = new TextAnalyze()
//    .setSubscriptionKey(textKey)
//    .setLocation(textApiLocation)
//    .setLanguageCol("language")
//    .setOutputCol("response")
//    .setErrorCol("error")
//
//  def prepResults(df: DataFrame): Seq[Option[UnpackedTextAnalyzeResponse]] = {
//    val fromRow = model.unpackedResponseBinding.makeFromRowConverter
//    df.collect().map(row => Option(row.getAs[Row](model.getOutputCol)).map(fromRow)).toList
//  }
//
//  test("Basic usage") {
//    val topResult = prepResults(model.transform(df.limit(1).coalesce(1))).head.get
//    assert(topResult.pii.get.document.get.entities.head.text == "last week")
//    assert(topResult.sentimentAnalysis.get.document.get.sentiment == "positive")
//    assert(topResult.entityLinking.get.document.get.entities.head.dataSource == "Wikipedia")
//    assert(topResult.keyPhraseExtraction.get.document.get.keyPhrases.head == "wonderful trip")
//    assert(topResult.entityRecognition.get.document.get.entities.head.text == "trip")
//  }
//
//  test("Manual Batching") {
//    val batchedDf = new FixedMiniBatchTransformer().setBatchSize(10).transform(df.coalesce(1))
//    val resultDF = new FlattenBatch().transform(model.transform(batchedDf))
//    val topResult = prepResults(resultDF).head.get
//    assert(topResult.pii.get.document.get.entities.head.text == "last week")
//    assert(topResult.sentimentAnalysis.get.document.get.sentiment == "positive")
//    assert(topResult.entityLinking.get.document.get.entities.head.dataSource == "Wikipedia")
//    assert(topResult.keyPhraseExtraction.get.document.get.keyPhrases.head == "wonderful trip")
//    assert(topResult.entityRecognition.get.document.get.entities.head.text == "trip")
//  }
//
//  test("Large Batching") {
//    val bigDF = (0 until 25).map(i => s"This is fantastic sentence number $i").toDF("text")
//    val model2 = model.setLanguage("en").setBatchSize(25)
//    val results = prepResults(model2.transform(bigDF.coalesce(1)))
//    assert(results.length == 25)
//    assert(results(24).get.sentimentAnalysis.get.document.get.sentiment == "positive")
//  }
//
//  test("Exceeded Retries Info") {
//    val badModel = model
//      .setPollingDelay(0)
//      .setInitialPollingDelay(0)
//      .setMaxPollingRetries(1)
//
//    val results = badModel
//      .setSuppressMaxRetriesException(true)
//      .transform(df.coalesce(1))
//    assert(results.where(!col("error").isNull).count() > 0)
//
//    assertThrows[SparkException] {
//      badModel.setSuppressMaxRetriesException(false)
//        .transform(df.coalesce(1))
//        .collect()
//    }
//  }
//
//  override def testObjects(): Seq[TestObject[TextAnalyze]] =
//    Seq(new TestObject[TextAnalyze](model, df))
//
//  override def reader: MLReadable[_] = TextAnalyze
//}
