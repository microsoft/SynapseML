// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive.split1

import com.microsoft.azure.synapse.ml.Secrets
import com.microsoft.azure.synapse.ml.cognitive._
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.featurize.text.PageSplitter
import com.microsoft.azure.synapse.ml.stages.FixedMiniBatchTransformer
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, explode, lit, posexplode}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.mutable.WrappedArray

trait TextEndpoint {
  lazy val textKey: String = sys.env.getOrElse("TEXT_API_KEY", Secrets.CognitiveApiKey)
  lazy val textApiLocation: String = sys.env.getOrElse("TEXT_API_LOCATION", "eastus")
}

class LanguageDetectorSuite extends TransformerFuzzing[LanguageDetectorV2] with TextEndpoint {

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    "Hello World",
    "Bonjour tout le monde",
    "La carretera estaba atascada. Había mucho tráfico el día de ayer.",
    ":) :( :D"
  ).toDF("text2")

  lazy val detector: LanguageDetectorV2 = new LanguageDetectorV2()
    .setSubscriptionKey(textKey)
    .setUrl(s"https://$textApiLocation.api.cognitive.microsoft.com/text/analytics/v2.0/languages")
    .setTextCol("text2")
    .setOutputCol("replies")

  test("Basic Usage") {
    val replies = detector.transform(df)
      .withColumn("lang", col("replies").getItem(0)
        .getItem("detectedLanguages").getItem(0)
        .getItem("name"))
      .select("lang")
      .collect().toList
    assert(replies.head.getString(0) == "English" && replies(2).getString(0) == "Spanish")
  }

  test("Is serializable ") {
    val replies = detector.transform(df.repartition(3))
      .withColumn("lang", col("replies").getItem(0)
        .getItem("detectedLanguages").getItem(0)
        .getItem("name"))
      .select("text2", "lang")
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

  override def testObjects(): Seq[TestObject[LanguageDetectorV2]] =
    Seq(new TestObject[LanguageDetectorV2](detector, df))

  override def reader: MLReadable[_] = LanguageDetectorV2
}

class LanguageDetectorV3Suite extends TransformerFuzzing[LanguageDetector] with TextEndpoint {

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    "Hello World",
    "Bonjour tout le monde",
    "La carretera estaba atascada. Había mucho tráfico el día de ayer.",
    ":) :( :D"
  ).toDF("text")

  lazy val detector: LanguageDetector = new LanguageDetector()
    .setSubscriptionKey(textKey)
    .setUrl(s"https://$textApiLocation.api.cognitive.microsoft.com/text/analytics/v3.0/languages")
    .setOutputCol("replies")

  test("Basic Usage") {
    val replies = detector.transform(df)
      .withColumn("lang", col("replies").getItem(0)
        .getItem("detectedLanguage")
        .getItem("name"))
      .select("lang")
      .collect().toList
    assert(replies.head.getString(0) == "English" && replies(2).getString(0) == "Spanish")
  }

  override def testObjects(): Seq[TestObject[LanguageDetector]] =
    Seq(new TestObject[LanguageDetector](detector, df))

  override def reader: MLReadable[_] = LanguageDetector
}

class EntityDetectorSuite extends TransformerFuzzing[EntityDetectorV2] with TextEndpoint {

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    ("1", "Microsoft released Windows 10"),
    ("2", "In 1975, Bill Gates III and Paul Allen founded the company.")
  ).toDF("id", "text")

  lazy val detector: EntityDetectorV2 = new EntityDetectorV2()
    .setSubscriptionKey(textKey)
    .setUrl(s"https://$textApiLocation.api.cognitive.microsoft.com/text/analytics/v2.0/entities")
    .setLanguage("en")
    .setOutputCol("replies")

  test("Basic Usage") {
    val results = detector.transform(df)
      .withColumn("entities",
        col("replies").getItem(0).getItem("entities").getItem("name"))
      .select("id", "entities").collect().toList
    assert(results.head.getSeq[String](1).toSet
      .intersect(Set("Windows 10", "Windows 10 Mobile", "Microsoft")).size == 2)
  }

  override def testObjects(): Seq[TestObject[EntityDetectorV2]] =
    Seq(new TestObject[EntityDetectorV2](detector, df))

  override def reader: MLReadable[_] = EntityDetectorV2
}

class EntityDetectorV3Suite extends TransformerFuzzing[EntityDetector] with TextEndpoint {

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    ("1", "Microsoft released Windows 10"),
    ("2", "In 1975, Bill Gates III and Paul Allen founded the company.")
  ).toDF("id", "text")

  lazy val detector: EntityDetector = new EntityDetector()
    .setSubscriptionKey(textKey)
    .setLocation("eastus")
    .setLanguage("en")
    .setOutputCol("replies")

  test("Basic Usage") {
    val results = detector.transform(df)
      .withColumn("entities",
        col("replies").getItem(0).getItem("entities").getItem("name"))
      .select("id", "entities").collect().toList
    println(results)
    assert(results.head.getSeq[String](1).toSet
      .intersect(Set("Windows 10", "Windows 10 Mobile", "Microsoft")).size == 2)
  }

  override def testObjects(): Seq[TestObject[EntityDetector]] =
    Seq(new TestObject[EntityDetector](detector, df))

  override def reader: MLReadable[_] = EntityDetector
}

trait TextSentimentBaseSuite extends TestBase with TextEndpoint {

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    ("en", "Hello world. This is some input text that I love."),
    ("fr", "Bonjour tout le monde"),
    ("es", "La carretera estaba atascada. Había mucho tráfico el día de ayer."),
    (null, "ich bin ein berliner"),
    (null, null),
    ("en", null)
  ).toDF("lang", "text")
}

class TextSentimentV3Suite extends TransformerFuzzing[TextSentiment] with TextSentimentBaseSuite {
  lazy val t: TextSentiment = new TextSentiment()
    .setSubscriptionKey(textKey)
    .setLocation("eastus")
    .setLanguageCol("lang")
    .setModelVersion("latest")
    .setShowStats(true)
    .setOutputCol("replies")

  test("Basic Usage") {

    val results = t.transform(df).select(
      col("replies").alias("scoredDocuments")
    ).collect().toList

    assert(List(4, 5).forall(results(_).get(0) == null))
    assert(
      results(0).getSeq[Row](0).head.getString(0) == "positive" &&
        results(2).getSeq[Row](0).head.getString(0) == "negative")
  }

  test("Opinion Mining") {
    val result = t.setOpinionMining(true)
      .transform(df)
      .select(explode(col("replies")))
      .select(explode(col("col.sentences")))
      .select(explode(col("col.targets")))
      .select(explode(col("col.relations")))
      .select(col("col.relationType"))
      .collect().toList.head.getString(0)
    assert(result === "assessment")
  }

  test("batch usage") {
    val t = new TextSentiment()
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

  override def testObjects(): Seq[TestObject[TextSentiment]] =
    Seq(new TestObject[TextSentiment](t, df))

  override def reader: MLReadable[_] = TextSentiment
}

class TextSentimentSuite extends TransformerFuzzing[TextSentimentV2] with TextSentimentBaseSuite {

  lazy val t: TextSentimentV2 = new TextSentimentV2()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setLanguageCol("lang")
    .setOutputCol("replies")

  test("Basic Usage") {
    val results = t.transform(df).withColumn("score",
      col("replies").getItem(0).getItem("score"))
      .select("score").collect().toList

    assert(List(4, 5).forall(results(_).get(0) == null))
    assert(results.head.getFloat(0) > .5 && results(2).getFloat(0) < .5)
  }

  test("batch usage") {
    val t = new TextSentimentV2()
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

  override def testObjects(): Seq[TestObject[TextSentimentV2]] =
    Seq(new TestObject[TextSentimentV2](t, df))

  override def reader: MLReadable[_] = TextSentimentV2
}

class KeyPhraseExtractorSuite extends TransformerFuzzing[KeyPhraseExtractorV2] with TextEndpoint {

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    ("en", "Hello world. This is some input text that I love."),
    ("fr", "Bonjour tout le monde"),
    ("es", "La carretera estaba atascada. Había mucho tráfico el día de ayer."),
    ("en", null)
  ).toDF("lang", "text")

  lazy val t: KeyPhraseExtractorV2 = new KeyPhraseExtractorV2()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setLanguageCol("lang")
    .setOutputCol("replies")

  test("Basic Usage") {
    val results = t.transform(df).withColumn("phrases",
      col("replies").getItem(0).getItem("keyPhrases"))
      .select("phrases").collect().toList

    println(results)

    assert(results.head.getSeq[String](0).toSet === Set("world", "input text"))
    assert(results(2).getSeq[String](0).toSet === Set("carretera", "tráfico", "día"))
  }

  override def testObjects(): Seq[TestObject[KeyPhraseExtractorV2]] =
    Seq(new TestObject[KeyPhraseExtractorV2](t, df))

  override def reader: MLReadable[_] = KeyPhraseExtractorV2
}

class KeyPhraseExtractorV3Suite extends TransformerFuzzing[KeyPhraseExtractor] with TextEndpoint {

  import spark.implicits._

  //noinspection ScalaStyle
  lazy val df: DataFrame = Seq(
    ("en", "Hello world. This is some input text that I love."),
    ("fr", "Bonjour tout le monde"),
    ("es", "La carretera estaba atascada. Había mucho tráfico el día de ayer."),
    ("en", null)
  ).toDF("lang", "text")

  lazy val t: KeyPhraseExtractor = new KeyPhraseExtractor()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setLanguageCol("lang")
    .setOutputCol("replies")

  test("Basic Usage") {
    val results = t.transform(df).withColumn("phrases",
      col("replies").getItem(0).getItem("keyPhrases"))
      .select("phrases").collect().toList

    println(results)

    assert(results.head.getSeq[String](0).toSet === Set("Hello world", "input text"))
    assert(results(2).getSeq[String](0).toSet === Set("mucho tráfico", "día", "carretera", "ayer"))
  }

  override def testObjects(): Seq[TestObject[KeyPhraseExtractor]] =
    Seq(new TestObject[KeyPhraseExtractor](t, df))

  override def reader: MLReadable[_] = KeyPhraseExtractor
}

class NERSuite extends TransformerFuzzing[NERV2] with TextEndpoint {

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    ("en", "Jeff bought three dozen eggs because there was a 50% discount."),
    ("en", "The Great Depression began in 1929. By 1933, the GDP in America fell by 25%.")
  ).toDF("language", "text")

  lazy val n: NERV2 = new NERV2()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
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

  override def testObjects(): Seq[TestObject[NERV2]] =
    Seq(new TestObject[NERV2](n, df))

  override def reader: MLReadable[_] = NERV2
}

class NERV3Suite extends TransformerFuzzing[NER] with TextEndpoint {

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    ("en", "I had a wonderful trip to Seattle last week."),
    ("en", "I visited Space Needle 2 times.")
  ).toDF("language", "text")

  lazy val n: NER = new NER()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setLanguage("en")
    .setOutputCol("response")

  test("Basic Usage") {
    val results = n.transform(df)
    val matches = results.withColumn("match",
      col("response")
        .getItem(0)
        .getItem("entities")
        .getItem(0))
      .select("match")

    val testRow = matches.collect().head(0).asInstanceOf[GenericRowWithSchema]

    assert(testRow.getAs[String]("text") === "trip")
    assert(testRow.getAs[Int]("offset") === 18)
    assert(testRow.getAs[Int]("length") === 4)
    assert(testRow.getAs[Double]("confidenceScore") > 0.7)
    assert(testRow.getAs[String]("category") === "Event")

  }

  override def testObjects(): Seq[TestObject[NER]] =
    Seq(new TestObject[NER](n, df))

  override def reader: MLReadable[_] = NER
}

class PIIV3Suite extends TransformerFuzzing[PII] with TextEndpoint {

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    ("en", "My SSN is 859-98-0987"),
    ("en",
      "Your ABA number - 111000025 - is the first 9 digits in the lower left hand corner of your personal check."),
    ("en", "Is 998.214.865-68 your Brazilian CPF number?")
  ).toDF("language", "text")

  lazy val n: PII = new PII()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setLanguage("en")
    .setOutputCol("response")

  test("Basic Usage") {
    val results = n.transform(df)

    val redactedTexts = results.withColumn("redactedText",
      col("response")
        .getItem(0)
        .getItem("redactedText"))
      .select("redactedText")
    val redactedText = redactedTexts.collect().head(0).toString
    assert(redactedText === "My SSN is ***********")

    val matches = results.withColumn("match",
      col("response")
        .getItem(0)
        .getItem("entities")
        .getItem(0))
      .select("match")

    val testRow = matches.collect().head(0).asInstanceOf[GenericRowWithSchema]

    assert(testRow.getAs[String]("text") === "859-98-0987")
    assert(testRow.getAs[Int]("offset") === 10)
    assert(testRow.getAs[Int]("length") === 11)
    assert(testRow.getAs[Double]("confidenceScore") > 0.6)
    assert(testRow.getAs[String]("category") === "USSocialSecurityNumber")

  }

  override def testObjects(): Seq[TestObject[PII]] =
    Seq(new TestObject[PII](n, df))

  override def reader: MLReadable[_] = PII
}

class TextAnalyzeSuite extends TransformerFuzzing[TextAnalyze] with TextEndpoint {

  import spark.implicits._

  lazy val dfBasic: DataFrame = Seq(
    ("en", "I had a wonderful trip to Seattle last week and visited Microsoft."),
    ("invalid", "This is irrelevant as the language is invalid")
  ).toDF("language", "text")

  val batchSize = 25
  lazy val dfBatched: DataFrame = Seq(
    (
      Seq("en", "invalid") ++ Seq.fill(batchSize - 2)("en"),
      Seq("I had a wonderful trip to Seattle last week and visited Microsoft.",
        "This is irrelevant as the language is invalid") ++ Seq.fill(batchSize - 2)("Placeholder content")
    )
  ).toDF("language", "text")

  lazy val n: TextAnalyze = new TextAnalyze()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setLanguageCol("language")
    .setOutputCol("response")
    .setErrorCol("error")
    .setEntityRecognitionTasks(Seq(TextAnalyzeTask(Map("model-version" -> "latest"))))
    .setEntityLinkingTasks(Seq(TextAnalyzeTask(Map("model-version" -> "latest"))))
    .setEntityRecognitionPiiTasks(Seq(TextAnalyzeTask(Map("model-version" -> "latest"))))
    .setKeyPhraseExtractionTasks(Seq(TextAnalyzeTask(Map("model-version" -> "latest"))))
    .setSentimentAnalysisTasks(Seq(TextAnalyzeTask(Map("model-version" -> "latest"))))

  def getEntityRecognitionResults(results: Dataset[Row], resultIndex: Int): Array[Row] = {
    results.withColumn("entityRecognition",
      col("response")
        .getItem(resultIndex)
        .getItem("entityRecognition")
        .getItem(0)
        .getItem("result")
        .getItem("entities")
        .getItem(0)
    ).select("entityRecognition")
      .collect()
  }

  def getEntityRecognitionErrors(results: Dataset[Row], resultIndex: Int): Array[Row] = {
    results.withColumn("error",
      col("response")
        .getItem(resultIndex)
        .getItem("entityRecognition")
        .getItem(0)
        .getItem("error")
        .getItem("error")
    ).select("error")
      .collect()
  }

  def getEntityRecognitionPiiResults(results: Dataset[Row], resultIndex: Int): Array[Row] = {
    results.withColumn("entityRecognitionPii",
      col("response")
        .getItem(resultIndex)
        .getItem("entityRecognitionPii")
        .getItem(0)
        .getItem("result")
        .getItem("entities")
        .getItem(0)
    ).select("entityRecognitionPii")
      .collect()
  }

  def getKeyPhraseResults(results: Dataset[Row], resultIndex: Int): Array[Row] = {
    results.withColumn("keyPhrase",
      col("response")
        .getItem(resultIndex)
        .getItem("keyPhraseExtraction")
        .getItem(0)
        .getItem("result")
        .getItem("keyPhrases")
        .getItem(0)
    ).select("keyPhrase")
      .collect()
  }

  def getSentimentAnalysisResults(results: Dataset[Row], resultIndex: Int): Array[Row] = {
    results.withColumn("sentimentAnalysis",
      col("response")
        .getItem(resultIndex)
        .getItem("sentimentAnalysis")
        .getItem(0)
        .getItem("result")
        .getItem("sentiment")
    ).select("sentimentAnalysis")
      .collect()
  }

  def getEntityLinkingResults(results: Dataset[Row], resultIndex: Int): Array[Row] = {
    results.withColumn("entityLinking",
      col("response")
        .getItem(resultIndex)
        .getItem("entityLinking")
        .getItem(0)
        .getItem("result")
        .getItem("entities")
        .getItem(0)
    ).select("entityLinking")
      .collect()
  }

  test("Basic Usage") {
    val results = n.transform(dfBasic).cache()
    // Validate first row (successful execution)

    // entity recognition
    val entityRows = getEntityRecognitionResults(results, resultIndex = 0)
    val entityRow = entityRows(0)
    val entityResult = entityRow(0).asInstanceOf[GenericRowWithSchema]

    assert(entityResult.getAs[String]("text") === "trip")
    assert(entityResult.getAs[Int]("offset") === 18)
    assert(entityResult.getAs[Int]("length") === 4)
    assert(entityResult.getAs[Double]("confidenceScore") > 0.66)
    assert(entityResult.getAs[String]("category") === "Event")

    // entity recognition pii
    val entityPiiRows = getEntityRecognitionPiiResults(results, resultIndex = 0)
    val entityPiiRow = entityPiiRows(0)
    val entityPiiResult = entityPiiRow(0).asInstanceOf[GenericRowWithSchema]
    assert(entityPiiResult.getAs[String]("text") === "last week")
    assert(entityPiiResult.getAs[Int]("offset") === 34)
    assert(entityPiiResult.getAs[Int]("length") === 9)
    assert(entityPiiResult.getAs[Double]("confidenceScore") > 0.79)
    assert(entityPiiResult.getAs[String]("category") === "DateTime")

    // key phrases
    val keyPhraseRows = getKeyPhraseResults(results, resultIndex = 0)
    val keyPhraseRow = keyPhraseRows(0).asInstanceOf[GenericRowWithSchema]
    assert(keyPhraseRow.getAs[String](0) === "wonderful trip")

    // text sentiment
    val sentimentAnalysisRows = getSentimentAnalysisResults(results, resultIndex = 0)
    val sentimentAnalysisRow = sentimentAnalysisRows(0).asInstanceOf[GenericRowWithSchema]
    assert(sentimentAnalysisRow.getAs[String](0) === "positive")

    // entity linking
    val entityLinkingRows = getEntityLinkingResults(results, resultIndex = 0)
    val entityLinkingRow = entityLinkingRows(0).asInstanceOf[GenericRowWithSchema]
    val entityLinkingResult = entityLinkingRow(0).asInstanceOf[GenericRowWithSchema]
    assert(entityLinkingResult.getAs[String]("name") === "Seattle")

    // Validate second row has error
    val entityRows2 = getEntityRecognitionErrors(results, resultIndex = 0)
    val entityRow2 = entityRows2(1).asInstanceOf[GenericRowWithSchema]
    assert(entityRow2.getAs[String]("error").contains("\"code\":\"UnsupportedLanguageCode\""))
  }

  test("Batched Usage") {
    val results = n.transform(dfBatched).cache()

    // Check we have the correct number of responses
    val response = results.select("response").collect()(0).asInstanceOf[GenericRowWithSchema](0)
    val responseCount = response match {
      case a: Seq[_] => a.length
      case _ => -1
    }
    assert(responseCount == batchSize)

    // First batch entry

    // entity recognition
    val entityRows = getEntityRecognitionResults(results, resultIndex = 0)
    val entityRow = entityRows(0)
    val entityResult = entityRow(0).asInstanceOf[GenericRowWithSchema]

    assert(entityResult.getAs[String]("text") === "trip")
    assert(entityResult.getAs[Int]("offset") === 18)
    assert(entityResult.getAs[Int]("length") === 4)
    assert(entityResult.getAs[Double]("confidenceScore") > 0.66)
    assert(entityResult.getAs[String]("category") === "Event")

    // entity recognition pii
    val entityPiiRows = getEntityRecognitionPiiResults(results, resultIndex = 0)
    val entityPiiRow = entityPiiRows(0)
    val entityPiiResult = entityPiiRow(0).asInstanceOf[GenericRowWithSchema]
    assert(entityPiiResult.getAs[String]("text") === "last week")
    assert(entityPiiResult.getAs[Int]("offset") === 34)
    assert(entityPiiResult.getAs[Int]("length") === 9)
    assert(entityPiiResult.getAs[Double]("confidenceScore") > 0.79)
    assert(entityPiiResult.getAs[String]("category") === "DateTime")

    // key phrases
    val keyPhraseRows = getKeyPhraseResults(results, resultIndex = 0)
    val keyPhraseRow = keyPhraseRows(0).asInstanceOf[GenericRowWithSchema]
    assert(keyPhraseRow.getAs[String](0) === "wonderful trip")

    // text sentiment
    val sentimentAnalysisRows = getSentimentAnalysisResults(results, resultIndex = 0)
    val sentimentAnalysisRow = sentimentAnalysisRows(0).asInstanceOf[GenericRowWithSchema]
    assert(sentimentAnalysisRow.getAs[String](0) === "positive")

    // entity linking
    val entityLinkingRows = getEntityLinkingResults(results, resultIndex = 0)
    val entityLinkingRow = entityLinkingRows(0).asInstanceOf[GenericRowWithSchema]
    val entityLinkingResult = entityLinkingRow(0).asInstanceOf[GenericRowWithSchema]
    assert(entityLinkingResult.getAs[String]("name") === "Seattle")

    // Second batch entry
    val entityRows2 = getEntityRecognitionErrors(results, resultIndex = 1)
    val entityRow2 = entityRows2(0).asInstanceOf[GenericRowWithSchema]
    assert(entityRow2.getAs[String]("error").contains("\"code\":\"UnsupportedLanguageCode\""))
  }

  override def testObjects(): Seq[TestObject[TextAnalyze]] =
    Seq(new TestObject[TextAnalyze](n, dfBasic, dfBatched))

  override def reader: MLReadable[_] = TextAnalyze
}
