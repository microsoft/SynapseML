package com.microsoft.ml.spark.cognitive.split1

import com.microsoft.ml.spark.cognitive._
import com.microsoft.ml.spark.core.test.base.TestBase
import org.apache.spark.SparkException
import org.apache.spark.ml.param.DataFrameEquality
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, explode}

class DetectedLanguageSuitev4 extends TestBase with DataFrameEquality with TextKey {

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    (Seq("us", ""),Seq("Hello World","La carretera estaba atascada. Había mucho tráfico el día de ayer.")),
    (Seq("fr",""),Seq("Bonjour tout le monde","世界您好")),
    (Seq(""),Seq(":) :( :D")),
  ).toDF("lang", "text")

  val options: Option[TextAnalyticsRequestOptionsV4] = Some(new TextAnalyticsRequestOptionsV4("", true, false))

  lazy val detector: TextAnalyticsLanguageDetection = new TextAnalyticsLanguageDetection(options)
    .setSubscriptionKey(textKey)
    .setEndpoint("https://eastus.api.cognitive.microsoft.com/")
    .setInputCol("text")
    .setLangCol("lang")
    .setOutputCol("output")

  test("Language Detection - Output Assertion") {
    val replies = detector.transform(df)
      .select("output")
      .collect()
    assert(replies(0).schema(0).name == "output")
    df.printSchema()
    df.show()
    replies.foreach { row =>
      row.toSeq.foreach{col => println(col) }
    }
  }

  test("Language Detection - Batch Usage") {
    val replies = detector.transform(df)
    .select("output.result.name","output.result.iso6391Name")
    .collect()

    val language = replies.map(row => row.getList(0))
    assert(language(0).get(0).toString == "English" && language(0).get(1).toString == "Spanish")
    assert(language(1).get(0).toString == "French" && language(1).get(1).toString == "Chinese")

    val iso = replies.map(row => row.getList(1))
    assert(iso(0).get(0).toString == "en" && iso(0).get(1).toString == "es" &&
      iso(1).get(0).toString == "fr" && iso(1).get(1).toString == "zh")
  }

  test("Asynch Functionality with Parameters") {
    val concurrency = 10
    val timeout = 45

    val replies = detector
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
    val badConcurrency = -1
    val timeout = 45
    val caught =
      intercept[SparkException] {
        detector
          .setConcurrency(badConcurrency)
          .setTimeout(timeout)
          .transform(df)
          .select("output.result.name","output.result.iso6391Name")
          .collect()
      }
    assert(caught.getMessage.contains("java.lang.IllegalArgumentException"))
  }

  test("Asynch Incorrect Timeout Functionality") {
    val badTimeout = .01
    val concurrency = 1
    val caught =
      intercept[SparkException] {
        detector
          .setTimeout(badTimeout)
          .setConcurrency(1)
          .transform(df)
          .select("output.result.name","output.result.iso6391Name")
          .collect()
      }
    assert(caught.getMessage.contains("java.util.concurrent.TimeoutException"))
  }
}

class TextSentimentSuiteV4 extends TestBase with DataFrameEquality with TextKey {

  import spark.implicits._

  val options: Option[TextAnalyticsRequestOptionsV4] = Some(new TextAnalyticsRequestOptionsV4("", true, false))

  lazy val df: DataFrame = Seq(
    Seq("Hello world. This is some input text that I love."),
    Seq("I am sad"),
    Seq("I am feeling okay")
  ).toDF("text")

  lazy val batchedDF: DataFrame = Seq(
    (Seq("en", "en", "en"), Seq("I hate the rain.", "I love the sun", "This sucks")),
    (Seq("en"), Seq("I love Vancouver."))
  ).toDF("lang", "text")

  lazy val detector: TextSentimentV4 = new TextSentimentV4(options)
    .setSubscriptionKey(textKey)
    .setEndpoint("https://eastus.api.cognitive.microsoft.com/")
    .setInputCol("text")
    .setOutputCol("output")

  test("Sentiment Analysis - Output Assertion") {
    val replies = detector.transform(batchedDF)
      .select("output")
      .collect()
    assert(replies(0).schema(0).name == "output")
    df.printSchema()
    df.show()
    replies.foreach { row =>
      row.toSeq.foreach { col => println(col) }
    }
  }

  lazy val detector2: TextSentimentV4 = new TextSentimentV4(options)
    .setSubscriptionKey(textKey)
    .setEndpoint("https://eastus.api.cognitive.microsoft.com/")
    .setInputCol("text")
    .setLangCol("lang")
    .setOutputCol("output")

  test("Sentiment Analysis - Basic Usage") {
    val replies = detector2.transform(batchedDF)
      .select("output.result.sentiment")
      .collect()
    val data = replies.map(row => row.getList(0))
    assert(data(0).get(0).toString == "negative" && data(0).get(1).toString == "positive" &&
      data(0).get(2).toString == "negative")
    assert(data(1).get(0).toString == "positive")
  }
}

class KeyPhraseExtractionSuiteV4 extends TestBase with DataFrameEquality with TextKey {
  import spark.implicits._

  lazy val df2: DataFrame = Seq(
    (Seq("en","es"), Seq("Hello world. This is some input text that I love.",
      "La carretera estaba atascada. Había mucho tráfico el día de ayer.")),
    (Seq("fr"), Seq("Bonjour tout le monde")),
  ).toDF("lang","text" )

  val options: Option[TextAnalyticsRequestOptionsV4] = Some(new TextAnalyticsRequestOptionsV4("", true, false))
  df2.printSchema()
  df2.show()
  lazy val extractor: TextAnalyticsKeyphraseExtraction = new TextAnalyticsKeyphraseExtraction(options)
    .setSubscriptionKey(textKey)
    .setEndpoint("https://eastus.api.cognitive.microsoft.com/")
    .setInputCol("text")
    .setLangCol("lang")
    .setOutputCol("output")

  test("KPE - Basic Usage") {
    val replies = extractor.transform(df2)
      .select(explode(col("output.result.keyPhrases")))
      .collect()

    assert(replies(1).getSeq[String](0).toSet == Set("mucho tráfico", "carretera", "ayer"))
    assert(replies(2).getSeq[String](0).toSet == Set("Bonjour", "monde"))
    assert(replies(0).getSeq[String](0).toSet == Set("Hello world", "input text"))
  }

    test("KPE - Output Assertion"){
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
}

class PIISuiteV4 extends TestBase with DataFrameEquality with TextKey {

  import spark.implicits._
  lazy val df: DataFrame = Seq(
    (Seq("en", "en", "en"), Seq("This person is named John Doe", "He lives on 123 main street",
      "His phone number was 12345677")),
    (Seq("en"), Seq("I live in Vancouver."))
  ).toDF("lang", "text")

  val options: Option[TextAnalyticsRequestOptionsV4] = Some(new TextAnalyticsRequestOptionsV4("", true, false))
  df.printSchema()
  df.show(10, false)
  lazy val extractor: PII = new PII(options)
    .setSubscriptionKey(textKey)
    .setEndpoint("https://eastus.api.cognitive.microsoft.com/")
    .setInputCol("text")
    .setLangCol("lang")
    .setOutputCol("output")

  test("PII - Basic Usage") {
    val replies = extractor.transform(df)
      .select("output")
      .show(10, false)
  }
}