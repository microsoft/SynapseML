package com.microsoft.ml.spark.cognitive.split1

import com.microsoft.ml.spark.cognitive._
import com.microsoft.ml.spark.core.test.base.TestBase
import org.apache.spark.SparkException
import org.apache.spark.ml.param.DataFrameEquality
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, explode}

class DetectedLanguageSuitev4 extends TestBase with DataFrameEquality with TextKey {

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    (Seq("us", ""), Seq("Hello World", "La carretera estaba atascada. Había mucho tráfico el día de ayer.")),
    (Seq("fr", ""), Seq("Bonjour tout le monde", "世界您好")),
    (Seq(""), Seq(":) :( :D")),
  ).toDF("lang", "text")

  lazy val invalidDocDf: DataFrame = Seq(
    (Seq("us", ""),Seq("",null))
  ).toDF("lang", "text")

  val options: Option[TextAnalyticsRequestOptionsV4] = Some(new TextAnalyticsRequestOptionsV4("", true, false))

  lazy val detector: TextAnalyticsLanguageDetection = new TextAnalyticsLanguageDetection(options)
    .setSubscriptionKey(textKey)
    .setEndpoint("https://eastus.api.cognitive.microsoft.com/")
    .setInputCol("text")
    .setLangCol("lang")
    .setOutputCol("output")

  lazy val invalidDetector: TextAnalyticsLanguageDetection = new TextAnalyticsLanguageDetection(options)
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
      row.toSeq.foreach { col => println(col) }
    }
  };

  test("Language Detection - Batch Usage") {
    val replies = detector.transform(df)
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
    val replies = detector.transform(df)
      .select("output", "output.result.confidenceScore")
      .collect()
    assert(replies(0).schema(0).name == "output", "output.result.confidenceScore")
    val firstRow = replies(0)
    val secondRow = replies(1)
    assert(replies(0).schema(0).name == "output")
    val fromRow = DetectLanguageResponseV4.makeFromRowConverter
    val resFirstRow = fromRow(firstRow.getAs[GenericRowWithSchema]("output"))
    val resSecondRow = fromRow(secondRow.getAs[GenericRowWithSchema]("output"))
    val modelVersionFirstRow = resFirstRow.modelVersion.get
    val modelVersionSecondRow = resSecondRow.modelVersion.get
    val positiveScoreFirstRow = resFirstRow.result.head.get.confidenceScore
    assert(modelVersionFirstRow.matches("\\d{4}-\\d{2}-\\d{2}"))
    assert(modelVersionFirstRow.matches("\\d{4}-\\d{2}-\\d{2}"))
    assert(positiveScoreFirstRow > 0.5)
  }

  test("Language Detection - Assert Model Version") {
    val replies = detector.transform(df)
      .select("output")
      .collect()
    assert(replies(0).schema(0).name == "output")
    val fromRow = DetectLanguageResponseV4.makeFromRowConverter
    replies.foreach(row => {
      val outResponse = fromRow(row.getAs[GenericRowWithSchema]("output"))
      val modelCheck = outResponse.result.head.get
      modelCheck.toString.matches("\\d{4}-\\d{2}-\\d{2}")
    })
  }

  test("Language Detection - Invalid Document Input"){
    val replies = detector.transform(invalidDocDf)
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
        invalidDetector
          .setEndpoint("https://eastus.api.cognitive.microsoft.com/")
          .setSubscriptionKey(invalidKey)
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
        invalidDetector
          .setEndpoint(invalidEndpoint)
          .setSubscriptionKey(textKey)
          .transform(df)
          .show()
      }
    assert(caught.getMessage.contains("'endpoint' must be a valid URL"))
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

  lazy val invalidDocDf: DataFrame = Seq(
    (Seq("us", ""),Seq("",null))
  ).toDF("lang", "text")

  lazy val invalidLanguageDf: DataFrame = Seq(
    (Seq("abc", "/."),Seq("Today is a wonderful day.","I hate the cold"))
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
  };
  test("Sentiment Analysis - Check Model Version") {
    val replies = detector.transform(batchedDF)
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

  test("Sentiment Analysis - Invalid Document Input"){
    val replies = detector.transform(invalidDocDf)
      .select("output.error.errorMessage", "output.error.errorCode")
      .collect()
    val errors = replies.map(row => row.getList(0))
    val codes = replies.map(row => row.getList(1))

    assert(errors(0).get(0).toString == "Document text is empty.")
    assert(codes(0).get(0).toString == "InvalidDocument")

    assert(errors(0).get(1).toString == "Document text is empty.")
    assert(codes(0).get(1).toString == "InvalidDocument")
  }

  test("Sentiment Analysis - Invalid Language Input"){
    val replies = detector.transform(invalidLanguageDf)
      .select("output.error.errorMessage", "output.error.errorCode")
      .collect()
    val errors = replies.map(row => row.getList(0))
    val codes = replies.map(row => row.getList(1))

    assert(errors(0).get(0).toString.contains("Invalid language code."))
    assert(codes(0).get(0).toString == "UnsupportedLanguageCode")

    assert(errors(0).get(1).toString.contains("Invalid language code."))
    assert(codes(0).get(1).toString == "UnsupportedLanguageCode")
  }

  test("Sentiment Analysis - Assert Confidence Score") {
    val replies = detector.transform(batchedDF)
      .select("output")
      .collect()
    val firstRow = replies(0)
    val secondRow = replies(1)
    assert(replies(0).schema(0).name == "output")
    val fromRow = SentimentResponseV4.makeFromRowConverter
    val resFirstRow = fromRow(firstRow.getAs[GenericRowWithSchema]("output"))
    val resSecondRow = fromRow(secondRow.getAs[GenericRowWithSchema]("output"))
    val modelVersionFirstRow = resFirstRow.modelVersion.get
    val modelVersionSecondRow = resSecondRow.modelVersion.get
    val negativeScoreFirstRow = resFirstRow.result.head.get.confidenceScores.negative
    val positiveScoreSecondRow = resSecondRow.result.head.get.confidenceScores.positive
    assert(modelVersionFirstRow.matches("\\d{4}-\\d{2}-\\d{2}"))
    assert(modelVersionFirstRow.matches("\\d{4}-\\d{2}-\\d{2}"))
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

  lazy val blankLanguageDf: DataFrame = Seq(
    (Seq("",""), Seq("Hello world. This is some input text that I love.",
      "La carretera estaba atascada. Había mucho tráfico el día de ayer.")),
    (Seq(""), Seq("Bonjour tout le monde")),
  ).toDF("lang","text" )

  lazy val invalidDocDf: DataFrame = Seq(
    (Seq("us", ""),Seq("",null))
  ).toDF("lang", "text")

  lazy val invalidLanguageDf: DataFrame = Seq(
    (Seq("abc", "/."),Seq("Today is a wonderful day.","I hate the cold"))
  ).toDF("lang", "text")

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
  test("KPE - Check Model Version") {
    val replies = extractor.transform(df2)
      .select("output")
      .collect()
    assert(replies(0).schema(0).name == "output")
    val fromRow = KeyPhraseResponseV4.makeFromRowConverter
    replies.foreach(row => {
      val outResponse = fromRow(row.getAs[GenericRowWithSchema]("output"))
      val modelCheck = outResponse.result.head.get
      modelCheck.toString.matches("\\d{4}-\\d{2}-\\d{2}")
    })
  }

  test("KPE - Blank Language Input") {
    val replies = extractor.transform(blankLanguageDf)
      .select(explode(col("output.result.keyPhrases")))
      .collect()

    assert(replies(1).getSeq[String](0).toSet == Set("mucho tráfico", "carretera", "ayer"))
    assert(replies(2).getSeq[String](0).toSet == Set("Bonjour", "monde"))
    assert(replies(0).getSeq[String](0).toSet == Set("Hello world", "input text"))
  }

  test("KPE - Invalid Document Input"){
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

  test("KPE - Invalid Language Input"){
    val replies = extractor.transform(invalidLanguageDf)
    val errors = replies
      .select(explode(col("output.error.errorMessage")))
      .collect()
    val codes = replies
      .select(explode(col("output.error.errorCode")))
      .collect()

    assert(errors(0).get(0).toString.contains("Invalid language code."))
    assert(codes(0).get(0).toString == "InvalidDocument")

    assert(errors(1).get(0).toString.contains("Invalid language code."))
    assert(codes(1).get(0).toString == "InvalidDocument")
  }
}
class HealthcareSuiteV4 extends TestBase with DataFrameEquality with TextKey {

  import spark.implicits._

  lazy val df3: DataFrame = Seq(
    (Seq("en", "es"), Seq("Patient's brother died at the age of 64 from lung cancer.",
      "Estoy enfermo")),
    (Seq("fr"), Seq("J’ai mal au ventre")),
  ).toDF("lang", "text")
  val options: Option[TextAnalyticsRequestOptionsV4] = Some(new TextAnalyticsRequestOptionsV4("", true, false))
  df3.printSchema()
  df3.show()
  lazy val extractor: HealthcareResponseV4 = new HealthcareResponseV4(options)
    .setSubscriptionKey(textKey)
    .setEndpoint("https://eastus.api.cognitive.microsoft.com/")
    .setInputCol("text")
    .setLangCol("lang")
    .setOutputCol("output")

  test("Healthcare: Output Assertion") {
    val replies = extractor.transform(df3)
      .select("output")
      .collect()

    assert(replies(0).schema(0).name == "output")

    df3.printSchema()
    df3.show()
    replies.foreach { row =>
      row.toSeq.foreach { col => println(col) }
    }
  }
}