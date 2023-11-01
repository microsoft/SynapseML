// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.language

import com.microsoft.azure.synapse.ml.services.text.{SentimentAssessment, TextEndpoint}
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, flatten, map}
import org.scalactic.{Equality, TolerantNumerics}

class EntityLinkingSuite extends TransformerFuzzing[AnalyzeText] with TextEndpoint {

  import spark.implicits._

  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-3)

  def df: DataFrame = Seq(
    ("en", "Microsoft was founded by Bill Gates and Paul Allen."),
    ("en", "Pike place market is my favorite Seattle attraction.")
  ).toDF("language", "text")

  def model: AnalyzeText = new AnalyzeText()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setLanguageCol("language")
    .setTextCol("text")
    .setKind("EntityLinking")
    .setOutputCol("response")
    .setErrorCol("error")

  test("Basic usage") {
    val result = model.transform(df)
      .withColumn("documents", col("response.documents"))
      .withColumn("entityNames", map(col("documents.id"), col("documents.entities.name")))
    val entities = result.head.getAs[Map[String, Seq[String]]]("entityNames")("0")
    assert(entities.contains("Microsoft"))
    assert(entities.contains("Bill Gates"))
  }

  test("api-version 2022-10-01-preview") {
    val result = model.setApiVersion("2022-10-01-preview").transform(df)
      .withColumn("documents", col("response.documents"))
      .withColumn("entityNames", map(col("documents.id"), col("documents.entities.name")))
    val entities = result.head.getAs[Map[String, Seq[String]]]("entityNames")("0")
    assert(entities.contains("Microsoft"))
    assert(entities.contains("Bill Gates"))
  }

  test("Show stats") {
    val result = model.setShowStats(true).transform(df)
      .withColumn("documents", col("response.documents"))
      .withColumn("entityNames", map(col("documents.id"), col("documents.entities.name")))
      .withColumn("validDocumentsCount", col("response.statistics.validDocumentsCount"))
    val entities = result.head.getAs[Map[String, Seq[String]]]("entityNames")("0")
    assert(entities.contains("Microsoft"))
    assert(entities.contains("Bill Gates"))
    assert(result.head.getAs[Int]("validDocumentsCount") == 1)
  }

  override def testObjects(): Seq[TestObject[AnalyzeText]] =
    Seq(new TestObject[AnalyzeText](model, df))

  override def reader: MLReadable[_] = AnalyzeText
}

class EntityRecognitionSuite extends TransformerFuzzing[AnalyzeText] with TextEndpoint {

  import spark.implicits._

  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-3)

  def df: DataFrame = Seq(
    ("en", "Microsoft was founded by Bill Gates and Paul Allen."),
    ("en", "Pike place market is my favorite Seattle attraction.")
  ).toDF("language", "text")

  def model: AnalyzeText = new AnalyzeText()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setLanguageCol("language")
    .setTextCol("text")
    .setKind("EntityRecognition")
    .setOutputCol("response")
    .setErrorCol("error")

  test("Basic usage") {
    val result = model.transform(df)
      .withColumn("documents", col("response.documents"))
      .withColumn("entityNames", map(col("documents.id"), col("documents.entities.text")))
    val entities = result.head.getAs[Map[String, Seq[String]]]("entityNames")("0")
    assert(entities.contains("Microsoft"))
    assert(entities.contains("Bill Gates"))
  }

  test("api-version 2022-10-01-preview") {
    val result = model.setApiVersion("2022-10-01-preview").transform(df)
      .withColumn("documents", col("response.documents"))
      .withColumn("entityNames", map(col("documents.id"), col("documents.entities.text")))
    val entities = result.head.getAs[Map[String, Seq[String]]]("entityNames")("0")
    assert(entities.contains("Microsoft"))
    assert(entities.contains("Bill Gates"))
  }

  test("Show stats") {
    val result = model.setShowStats(true).transform(df)
      .withColumn("documents", col("response.documents"))
      .withColumn("entityNames", map(col("documents.id"), col("documents.entities.text")))
      .withColumn("validDocumentsCount", col("response.statistics.validDocumentsCount"))
    val entities = result.head.getAs[Map[String, Seq[String]]]("entityNames")("0")
    assert(entities.contains("Microsoft"))
    assert(entities.contains("Bill Gates"))
    assert(result.head.getAs[Int]("validDocumentsCount") == 1)
  }

  override def testObjects(): Seq[TestObject[AnalyzeText]] =
    Seq(new TestObject[AnalyzeText](model, df))

  override def reader: MLReadable[_] = AnalyzeText
}

class KeyPhraseSuite extends TransformerFuzzing[AnalyzeText] with TextEndpoint {

  import spark.implicits._

  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-3)

  def df: DataFrame = Seq(
    ("en", "Microsoft was founded by Bill Gates and Paul Allen."),
    ("en", "Text Analytics is one of the Azure Cognitive Services."),
    ("en", "My cat might need to see a veterinarian.")
  ).toDF("language", "text")

  def model: AnalyzeText = new AnalyzeText()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setLanguageCol("language")
    .setTextCol("text")
    .setKind("KeyPhraseExtraction")
    .setOutputCol("response")
    .setErrorCol("error")

  test("Basic usage") {
    val result = model.transform(df)
      .withColumn("documents", col("response.documents"))
      .withColumn("keyPhrases", col("documents.keyPhrases"))
    val keyPhrases = result.collect()(1).getAs[Seq[String]]("keyPhrases")
    assert(keyPhrases.contains("Azure Cognitive Services"))
    assert(keyPhrases.contains("Text Analytics"))
  }

  test("api-version 2022-10-01-preview") {
    val result = model.setApiVersion("2022-10-01-preview").transform(df)
      .withColumn("documents", col("response.documents"))
      .withColumn("keyPhrases", col("documents.keyPhrases"))
    val keyPhrases = result.collect()(1).getAs[Seq[String]]("keyPhrases")
    assert(keyPhrases.contains("Azure Cognitive Services"))
    assert(keyPhrases.contains("Text Analytics"))
  }

  test("Show stats") {
    val result = model.setShowStats(true).transform(df)
      .withColumn("documents", col("response.documents"))
      .withColumn("keyPhrases", col("documents.keyPhrases"))
      .withColumn("validDocumentsCount", col("response.statistics.validDocumentsCount"))
    val keyPhrases = result.collect()(1).getAs[Seq[String]]("keyPhrases")
    assert(keyPhrases.contains("Azure Cognitive Services"))
    assert(keyPhrases.contains("Text Analytics"))
    assert(result.head.getAs[Int]("validDocumentsCount") == 1)
  }

  override def testObjects(): Seq[TestObject[AnalyzeText]] =
    Seq(new TestObject[AnalyzeText](model, df))

  override def reader: MLReadable[_] = AnalyzeText
}

class LanguageDetectionSuite extends TransformerFuzzing[AnalyzeText] with TextEndpoint {

  import spark.implicits._

  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-3)

  def df: DataFrame = Seq(
    Tuple1(Array("Hello world")),
    Tuple1(Array("Bonjour tout le monde", "Hola mundo", "Tumhara naam kya hai?")),
    Tuple1(Array("你好")),
    Tuple1(Array("日本国（にほんこく、にっぽんこく、英"))
  ).toDF("text")

  def model: AnalyzeText = new AnalyzeText()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setTextCol("text")
    .setKind("LanguageDetection")
    .setOutputCol("response")
    .setErrorCol("error")

  test("Basic usage") {
    val result = model.transform(df)
      .withColumn("documents", col("response.documents"))
      .withColumn("detectedLanguage", col("documents.detectedLanguage.name"))
    val detectedLanguages = result.collect()(1).getAs[Seq[String]]("detectedLanguage")
    assert(detectedLanguages.contains("French"))
    assert(detectedLanguages.contains("Spanish"))
  }

  test("api-version 2022-10-01-preview") {
    val result = model.setApiVersion("2022-10-01-preview").transform(df)
      .withColumn("documents", col("response.documents"))
      .withColumn("detectedLanguage", col("documents.detectedLanguage.name"))
    val detectedLanguages = result.collect()(1).getAs[Seq[String]]("detectedLanguage")
    assert(detectedLanguages.contains("French"))
    assert(detectedLanguages.contains("Spanish"))
  }

  test("Show stats") {
    val result = model.setShowStats(true).transform(df)
      .withColumn("documents", col("response.documents"))
      .withColumn("detectedLanguage", col("documents.detectedLanguage.name"))
      .withColumn("validDocumentsCount", col("response.statistics.validDocumentsCount"))
    val detectedLanguages = result.collect()(1).getAs[Seq[String]]("detectedLanguage")
    assert(detectedLanguages.contains("French"))
    assert(detectedLanguages.contains("Spanish"))
    assert(result.collect()(1).getAs[Seq[Int]]("validDocumentsCount") == Seq(3, 3, 3))
  }

  override def testObjects(): Seq[TestObject[AnalyzeText]] =
    Seq(new TestObject[AnalyzeText](model, df))

  override def reader: MLReadable[_] = AnalyzeText
}

class AnalyzeTextPIISuite extends TransformerFuzzing[AnalyzeText] with TextEndpoint {

  import spark.implicits._

  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-3)

  def df: DataFrame = Seq(
    "My SSN is 859-98-0987",
    "Your ABA number - 111000025 - is the first 9 digits in the lower left hand corner of your personal check.",
    "Is 998.214.865-68 your Brazilian CPF number?"
  ).toDF("text")

  def model: AnalyzeText = new AnalyzeText()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setTextCol("text")
    .setKind("PiiEntityRecognition")
    .setOutputCol("response")
    .setErrorCol("error")

  test("Basic usage") {
    val result = model.transform(df)
      .withColumn("documents", col("response.documents"))
      .withColumn("redactedText", col("documents.redactedText"))
      .withColumn("entities", col("documents.entities.text"))
      .collect()
    val entities = result.head.getAs[Seq[String]]("entities")
    assert(entities.contains("859-98-0987"))
    val redactedText = result(1).getAs[String]("redactedText")
    assert(!redactedText.contains("111000025"))
  }

  test("api-version 2022-10-01-preview") {
    val result = model.setApiVersion("2022-10-01-preview").transform(df)
      .withColumn("documents", col("response.documents"))
      .withColumn("redactedText", col("documents.redactedText"))
      .withColumn("entities", col("documents.entities.text"))
      .collect()
    val entities = result.head.getAs[Seq[String]]("entities")
    assert(entities.contains("859-98-0987"))
    val redactedText = result(1).getAs[String]("redactedText")
    assert(!redactedText.contains("111000025"))
  }

  test("Show stats") {
    val result = model.setShowStats(true).transform(df)
      .withColumn("documents", col("response.documents"))
      .withColumn("redactedText", col("documents.redactedText"))
      .withColumn("entities", col("documents.entities.text"))
      .withColumn("validDocumentsCount", col("response.statistics.validDocumentsCount"))
      .collect()
    val entities = result.head.getAs[Seq[String]]("entities")
    assert(entities.contains("859-98-0987"))
    val redactedText = result(1).getAs[String]("redactedText")
    assert(!redactedText.contains("111000025"))
    assert(result.head.getAs[Int]("validDocumentsCount") == 1)
  }

  override def testObjects(): Seq[TestObject[AnalyzeText]] =
    Seq(new TestObject[AnalyzeText](model, df))

  override def reader: MLReadable[_] = AnalyzeText
}

class SentimentAnalysisSuite extends TransformerFuzzing[AnalyzeText] with TextEndpoint {

  import spark.implicits._

  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-3)

  def df: DataFrame = Seq(
    "Great atmosphere. Close to plenty of restaurants, hotels, and transit! Staff are friendly and helpful.",
    "What a sad story!"
  ).toDF("text")

  def model: AnalyzeText = new AnalyzeText()
    .setSubscriptionKey(textKey)
    .setLocation(textApiLocation)
    .setTextCol("text")
    .setKind("SentimentAnalysis")
    .setOutputCol("response")
    .setErrorCol("error")

  test("Basic usage") {
    val result = model.transform(df)
      .withColumn("documents", col("response.documents"))
      .withColumn("sentiment", col("documents.sentiment"))
      .collect()
    assert(result.head.getAs[String]("sentiment") == "positive")
    assert(result(1).getAs[String]("sentiment") == "negative")
  }

  test("api-version 2022-10-01-preview") {
    val result = model.setApiVersion("2022-10-01-preview").transform(df)
      .withColumn("documents", col("response.documents"))
      .withColumn("sentiment", col("documents.sentiment"))
      .collect()
    assert(result.head.getAs[String]("sentiment") == "positive")
    assert(result(1).getAs[String]("sentiment") == "negative")
  }

  test("Show stats") {
    val result = model.setShowStats(true).transform(df)
      .withColumn("documents", col("response.documents"))
      .withColumn("sentiment", col("documents.sentiment"))
      .withColumn("validDocumentsCount", col("response.statistics.validDocumentsCount"))
      .collect()
    assert(result.head.getAs[String]("sentiment") == "positive")
    assert(result(1).getAs[String]("sentiment") == "negative")
    assert(result.head.getAs[Int]("validDocumentsCount") == 1)
  }

  test("Opinion Mining") {
    val result = model.setOpinionMining(true).transform(df)
      .withColumn("documents", col("response.documents"))
      .withColumn("sentiment", col("documents.sentiment"))
      .withColumn("assessments", flatten(col("documents.sentences.assessments")))
      .collect()
    assert(result.head.getAs[String]("sentiment") == "positive")
    assert(result(1).getAs[String]("sentiment") == "negative")
    val fromRow = SentimentAssessment.makeFromRowConverter
    assert(result.head.getAs[Seq[Row]]("assessments").map(fromRow).head.sentiment == "positive")
  }

  override def testObjects(): Seq[TestObject[AnalyzeText]] =
    Seq(new TestObject[AnalyzeText](model, df))

  override def reader: MLReadable[_] = AnalyzeText
}
