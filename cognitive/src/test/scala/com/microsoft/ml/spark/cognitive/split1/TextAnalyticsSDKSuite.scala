// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive.split1

import com.microsoft.ml.spark.cognitive._
import com.microsoft.ml.spark.core.test.base.TestBase
import org.apache.spark.SparkException
import org.apache.spark.ml.param.DataFrameEquality
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{col, explode}

class DetectedLanguageSuiteV4 extends TestBase with DataFrameEquality with TextKey {

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    (Seq("us", ""), Seq("Hello World", "La carretera estaba atascada. Había mucho tráfico el día de ayer.")),
    (Seq("fr", ""), Seq("Bonjour tout le monde", "世界您好")),
    (Seq(""), Seq(":) :( :D")),
  ).toDF("lang", "text")

  lazy val invalidDocDf: DataFrame = Seq(
    (Seq("us", ""), Seq("", null))
  ).toDF("lang", "text")

  def getDetector: LanguageDetectionV4 = new LanguageDetectionV4()
    .setSubscriptionKey(textKey)
    .setLocation("eastus")
    .setTextCol("text")
    .setLanguageCol("lang")
    .setOutputCol("output")

  test("Language Detection - Output Assertion") {
    val replies = getDetector.transform(df)
      .select("output")
      .collect()
    assert(replies(0).schema(0).name == "output")
    df.printSchema()
    df.show()
    replies.foreach { row =>
      row.toSeq.foreach { col => println(col) }
    }
  };

  test("Language Detection - Batch Usage") {
    val replies = getDetector.transform(df)
      .select("output.result.name", "output.result.iso6391Name")
      .collect()

    val language = replies.map(row => row.getList(0))
    assert(language(0).get(0).toString == "English" && language(0).get(1).toString == "Spanish")
    assert(language(1).get(0).toString == "French" && language(1).get(1).toString == "Chinese")

    val iso = replies.map(row => row.getList(1))
    assert(iso(0).get(0).toString == "en" && iso(0).get(1).toString == "es")
    assert(iso(1).get(0).toString == "fr" && iso(1).get(1).toString == "zh")
  }

  test("Language Detection - Check Confidence Score") {
    val replies = getDetector.transform(df)
      .select("output", "output.result.confidenceScore")
      .collect()
    assert(replies(0).schema(0).name == "output", "output.result.confidenceScore")
    val firstRow = replies(0)
    val secondRow = replies(1)
    assert(replies(0).schema(0).name == "output")
    val fromRow = DetectLanguageResponseV4.makeFromRowConverter
    val resFirstRow = fromRow(firstRow.getAs[GenericRowWithSchema]("output"))
    val resSecondRow = fromRow(secondRow.getAs[GenericRowWithSchema]("output"))
    val positiveScoreFirstRow = resFirstRow.result.head.get.confidenceScore
    assert(positiveScoreFirstRow > 0.5)
  }


  test("Language Detection - Invalid Document Input") {
    val replies = getDetector.transform(invalidDocDf)
      .select("output.error.errorMessage", "output.error.errorCode")
      .collect()
    val errors = replies.map(row => row.getList(0))
    val codes = replies.map(row => row.getList(1))

    assert(errors(0).get(0).toString == "Document text is empty.")
    assert(codes(0).get(0).toString == "InvalidDocument")

    assert(errors(0).get(1).toString == "Document text is empty.")
    assert(codes(0).get(1).toString == "InvalidDocument")
  }

  test("Invalid Subscription Key Caught") {
    val invalidKey = "12345"
    val caught =
      intercept[SparkException] {
        getDetector.setSubscriptionKey(invalidKey)
          .transform(df)
          .show()
      }
    assert(caught.getMessage.contains("Status code 401"))
    assert(caught.getMessage.contains("invalid subscription key or wrong API endpoint"))
  }

  test("Wrong API Endpoint Caught") {
    val invalidEndpoint = "invalidendpoint"
    val caught =
      intercept[SparkException] {
        getDetector.setLocation(invalidEndpoint)
          .setSubscriptionKey(textKey)
          .transform(df)
          .show()
      }
    assert(caught.getMessage.contains("failed to resolve 'invalidendpoint.api.cognitive.microsoft.com'"))
  }

  test("Asynch Functionality with Parameters") {
    val concurrency = 10
    val timeout = 45

    val replies = getDetector
      .setConcurrency(concurrency)
      .setTimeout(timeout)
      .transform(df)
      .select("output.result.name", "output.result.iso6391Name")
      .collect()

    val language = replies.map(row => row.getList(0))
    assert(language(0).get(0).toString == "English" && language(0).get(1).toString == "Spanish")
    assert(language(1).get(0).toString == "French" && language(1).get(1).toString == "Chinese")
  }

  test("Asynch Incorrect Concurrency Functionality") {
    val caught = intercept[SparkException] {
      getDetector.setConcurrency(-1)
        .setTimeout(45)
        .transform(df)
        .select("output.result.name", "output.result.iso6391Name")
        .collect()
    }
    assert(caught.getMessage.contains("java.lang.IllegalArgumentException"))
  }
}

class TextSentimentSuiteV4 extends TestBase with DataFrameEquality with TextKey {

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    Seq("Hello world. This is some input text that I love."),
    Seq("I am sad"),
    Seq("I am feeling okay")
  ).toDF("text")

  lazy val unbatchedDF: DataFrame = Seq(
    ("en", "Hello world. This is some input text that I love."),
    ("fr", "Bonjour tout le monde"),
    ("es", "La carretera estaba atascada. Había mucho tráfico el día de ayer."),
    ("es", "La carretera estaba atascada."),
    ("es", "Había mucho tráfico el día de ayer."),
    ("es", "La carretera estaba atascada. Había mucho tráfico el día de ayer."),
    (null, "ich bin ein berliner")
  ).toDF("lang", "text")

  lazy val batchedDF: DataFrame = Seq(
    (Seq("en", "en", "en"), Seq("I hate the rain.", "I love the sun", "This sucks")),
    (Seq("en"), Seq("I love Vancouver."))
  ).toDF("lang", "text")

  lazy val invalidDocDf: DataFrame = Seq(
    (Seq("us", ""), Seq("", null))
  ).toDF("lang", "text")

  lazy val invalidLanguageDf: DataFrame = Seq(
    (Seq("abc", "/."), Seq("Today is a wonderful day.", "I hate the cold"))
  ).toDF("lang", "text")

  def getDetector: TextSentimentV4 = new TextSentimentV4()
    .setSubscriptionKey(textKey)
    .setLocation("eastus")
    .setTextCol("text")
    .setOutputCol("output")

  test("Sentiment Analysis - Output Assertion") {
    val replies = getDetector.transform(batchedDF)
      .select("output")
      .collect()
    assert(replies(0).schema(0).name == "output")
    df.printSchema()
    df.show()
    replies.foreach { row =>
      row.toSeq.foreach { col => println(col) }
    }
  }

  test("Sentiment - Auto batching") {
    val detector: TextSentimentV4 = getDetector
      .setLanguageCol("lang")
      .setBatchSize(2)

    val results = detector.transform(unbatchedDF.coalesce(1)).cache()
    results.show()

    val tdf = results
      .select("output.result.sentiment", "lang")
      .collect()

    assert(tdf(0).getSeq(0).length == 1)
    assert(tdf(0).getString(1) == "en")
    assert(tdf(3).getSeq(0).length == 1)
  }

  test("Sentiment Analysis - Check Model Version") {
    val replies = getDetector.transform(batchedDF)
      .select("output")
      .collect()

    assert(replies(0).schema(0).name == "output")

    val fromRow = SentimentResponseV4.makeFromRowConverter
    replies.foreach(row => {
      val outResponse = fromRow(row.getAs[GenericRowWithSchema]("output"))
      val modelCheck = outResponse.result.head.get
      modelCheck.toString.matches("\\d{4}-\\d{2}-\\d{2}")
    })
  }

  test("Sentiment Analysis - Basic Usage") {
    val detector: TextSentimentV4 = getDetector
      .setLanguageCol("lang")

    val replies = detector.transform(batchedDF)
      .select("output.result.sentiment")
      .collect()

    val data = replies.map(row => row.getList(0))
    assert(data(0).get(0).toString == "negative" && data(0).get(1).toString == "positive" &&
      data(0).get(2).toString == "negative")
    assert(data(1).get(0).toString == "positive")
  }

  test("Sentiment Analysis - Invalid Document Input") {
    val replies = getDetector.transform(invalidDocDf)
      .select("output.error.errorMessage", "output.error.errorCode")
      .collect()
    val errors = replies.map(row => row.getList(0))
    val codes = replies.map(row => row.getList(1))

    assert(errors(0).get(0).toString == "Document text is empty.")
    assert(codes(0).get(0).toString == "InvalidDocument")
  }

  test("Sentiment Analysis - Assert Confidence Score") {
    val replies = getDetector.transform(batchedDF)
      .select("output")
      .collect()

    val firstRow = replies(0)
    val secondRow = replies(1)
    assert(replies(0).schema(0).name == "output")
    val fromRow = SentimentResponseV4.makeFromRowConverter
    val resFirstRow = fromRow(firstRow.getAs[GenericRowWithSchema]("output"))
    val resSecondRow = fromRow(secondRow.getAs[GenericRowWithSchema]("output"))
    val negativeScoreFirstRow = resFirstRow.result.head.get.confidenceScores.negative
    val positiveScoreSecondRow = resSecondRow.result.head.get.confidenceScores.positive
    assert(negativeScoreFirstRow > 0.5)
    assert(positiveScoreSecondRow > 0.5)
  }
}

class KeyPhraseExtractionSuiteV4 extends TestBase with DataFrameEquality with TextKey {

  import spark.implicits._

  lazy val df2: DataFrame = Seq(
    (Seq("en", "es"), Seq("Hello world. This is some input text that I love.",
      "La carretera estaba atascada. Había mucho tráfico el día de ayer.")),
    (Seq("fr"), Seq("Bonjour tout le monde")),
  ).toDF("lang", "text")

  lazy val unbatchedDf: DataFrame = Seq(
    ("en", "Hello world. This is some input text that I love."),
    ("es", "La carretera estaba atascada. Había mucho tráfico el día de ayer."),
    ("fr", "Bonjour tout le monde")
  ).toDF("lang", "text")

  lazy val blankLanguageDf: DataFrame = Seq(
    (Seq("", ""), Seq("Hello world. This is some input text that I love.",
      "La carretera estaba atascada. Había mucho tráfico el día de ayer.")),
    (Seq(""), Seq("Bonjour tout le monde")),
  ).toDF("lang", "text")

  lazy val invalidDocDf: DataFrame = Seq(
    (Seq("us", ""), Seq("", null))
  ).toDF("lang", "text")

  lazy val invalidLanguageDf: DataFrame = Seq(
    (Seq("abc", "/."), Seq("Today is a wonderful day.", "I hate the cold"))
  ).toDF("lang", "text")


  def extractor: KeyphraseExtractionV4 = new KeyphraseExtractionV4()
    .setSubscriptionKey(textKey)
    .setLocation("eastus")
    .setTextCol("text")
    .setLanguageCol("lang")
    .setOutputCol("output")

  test("KPE - Basic Usage") {
    val replies = extractor.transform(df2)
      .select(explode(col("output.result.keyPhrases")))
      .collect()

    assert(replies(1).getSeq[String](0).toSet == Set("mucho tráfico", "carretera", "ayer"))
    assert(replies(2).getSeq[String](0).toSet == Set("Bonjour", "monde"))
    assert(replies(0).getSeq[String](0).toSet == Set("Hello world", "input text"))
  }

  test("KPE - Output Assertion") {
    val replies = extractor.transform(df2)
      .select("output")
      .collect()

    assert(replies(0).schema(0).name == "output")

    df2.printSchema()
    df2.show()
    replies.foreach { row =>
      row.toSeq.foreach { col => println(col) }
    }
  }

  test("KPE - Blank Language Input") {
    val replies = extractor.transform(blankLanguageDf)
      .select(explode(col("output.result.keyPhrases")))
      .collect()

    assert(replies(1).getSeq[String](0).toSet == Set("mucho tráfico", "carretera", "ayer"))
    assert(replies(2).getSeq[String](0).toSet == Set("Bonjour", "monde"))
    assert(replies(0).getSeq[String](0).toSet == Set("Hello world", "input text"))
  }

  test("KPE - Invalid Document Input") {
    val replies = extractor.transform(invalidDocDf)
    val errors = replies
      .select(explode(col("output.error.errorMessage")))
      .collect()
    val codes = replies
      .select(explode(col("output.error.errorCode")))
      .collect()

    assert(errors(0).get(0).toString == "Document text is empty.")
    assert(codes(0).get(0).toString == "InvalidDocument")

    assert(errors(1).get(0).toString == "Document text is empty.")
    assert(codes(1).get(0).toString == "InvalidDocument")
  }

  test("KPE - Invalid Language Input") {
    val replies = extractor.transform(invalidLanguageDf)
    val errors = replies
      .select(explode(col("output.error.errorMessage")))
      .collect()
    val codes = replies
      .select(explode(col("output.error.errorCode")))
      .collect()

    assert(errors(0).get(0).toString.contains("Invalid language code."))
    assert(codes(0).get(0).toString == "UnsupportedLanguageCode")

    assert(errors(1).get(0).toString.contains("Invalid language code."))
    assert(codes(1).get(0).toString == "UnsupportedLanguageCode")
  }

  test("Keyphrase - batch usage") {
    extractor.setLanguageCol("lang")
    val results = extractor.transform(unbatchedDf.coalesce(1)).cache()
    results.show()
    val tdf = results
      .select("lang", "output.result.keyPhrases")
      .collect()
    assert(tdf.length == 3)
  }
}

class PIISuiteV4 extends TestBase with DataFrameEquality with TextKey {

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    (Seq("en", "en", "en"), Seq("This person is named John Doe", "He lives on 123 main street",
      "His phone number was 12345677")),
    (Seq("en"), Seq("I live in Vancouver."))
  ).toDF("lang", "text")

  lazy val invalidDocDf: DataFrame = Seq(
    (Seq("us", ""), Seq("", null))
  ).toDF("lang", "text")

  lazy val unbatcheddf: DataFrame = Seq(
    ("en", "This person is named John Doe"),
    ("en", "He lives on 123 main street."),
    ("en", "His phone number was 12345677")
  ).toDF("lang", "text")

  df.printSchema()
  df.show(10, false)

  def extractor: PIIV4 = new PIIV4()
    .setSubscriptionKey(textKey)
    .setLocation("eastus")
    .setTextCol("text")
    .setLanguageCol("lang")
    .setOutputCol("output")

  test("PII - Basic Usage") {
    val replies = extractor.transform(df)
      .select("output.result.redactedText")
      .collect()
    assert(replies(0).schema(0).name == "redactedText")
    df.printSchema()
    df.show(10, false)
    replies.foreach { row =>
      row.toSeq.foreach { col => println(col) }
    }
  }

  test("PII - Invalid Document Input") {
    val replies = extractor.transform(invalidDocDf)
    val errors = replies
      .select(explode(col("output.error.errorMessage")))
      .collect()
    val codes = replies
      .select(explode(col("output.error.errorCode")))
      .collect()

    assert(errors(0).get(0).toString == "Document text is empty.")
    assert(codes(0).get(0).toString == "InvalidDocument")

    assert(errors(1).get(0).toString == "Document text is empty.")
    assert(codes(1).get(0).toString == "InvalidDocument")
  }

  test("PII - batch usage") {
    extractor.setLanguageCol("lang")
      .setBatchSize(2)
    val results = extractor.transform(unbatcheddf.coalesce(1)).cache()
    results.show()
    val tdf = results
      .select("lang", "output.result.redactedText")
      .collect()
    assert(tdf.length == 3)
  }
}