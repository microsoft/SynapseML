package com.microsoft.ml.spark.cognitive.split1

import com.azure.ai.textanalytics.models.TextAnalyticsRequestOptions
import com.microsoft.ml.spark.Secrets
import com.microsoft.ml.spark.cognitive._
import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.core.test.fuzzing.TransformerFuzzing
import org.apache.spark.ml.param.DataFrameEquality
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import com.microsoft.ml.spark.stages.FixedMiniBatchTransformer



class DetectedLanguageSuiteV4 extends TestBase with DataFrameEquality with TextKey {
  import spark.implicits._
  lazy val df: DataFrame = Seq(
    "Hello World",
    "Bonjour tout le monde",
    "La carretera estaba atascada. Había mucho tráfico el día de ayer.",
    "世界您好",
    ":) :( :D",
  ).toDF("text2")

  val options: Option[TextAnalyticsRequestOptions] = Some(new TextAnalyticsRequestOptions()
    .setIncludeStatistics(true))

  lazy val detector: TextAnalyticsLanguageDetection = new TextAnalyticsLanguageDetection(options)
    .setSubscriptionKey(textKey)
    .setEndpoint("https://eastus.api.cognitive.microsoft.com/")
    .setInputCol("text2")
    .setOutputCol("output")

  test("Language Detection - Basic Usage") {

    val replies = detector.transform(df)
    .select("name", "iso6391Name")
    .collect()
//    assert(replies(0).getString(0) == "English" && replies(2).getString(0) == "Spanish" )
//    assert(replies(3).getString(0) == "Chinese_Traditional")
//    assert(replies(0).getString(1) == "en" && replies(2).getString(1) == "es")


  }

  test("Language Detection - Print Schema") {

    detector.transform(df).printSchema()
  }
}

class TextSentimentSuiteV4 extends TestBase with DataFrameEquality with TextKey {

  import spark.implicits._

  val options: Option[TextAnalyticsRequestOptions] = Some(new TextAnalyticsRequestOptions()
    .setIncludeStatistics(true))

  lazy val df: DataFrame = Seq(
    "Hello world. This is some input text that I love.",
    "I am sad",
    "I am feeling okay"
  ).toDF("text")

  lazy val detector: TextSentimentV4 = new TextSentimentV4(options)
    .setSubscriptionKey(textKey)
    .setEndpoint("https://eastus.api.cognitive.microsoft.com/")
    .setInputCol("text")
    .setOutputCol("output")

  test("Sentiment Analysis - Basic Usage") {
    val replies = detector.transform(df)
      .select("sentiment", "confidenceScores", "sentences", "warnings", "output")
      .collect()
//    assert(replies(0).getString(0) == "positive" && replies(1).getString(0) == "negative"
//      && replies(2).getString(0) == "neutral")
    assert(replies == "output")
  }
}

  class KeyPhraseExtractionSuiteV4 extends TestBase with DataFrameEquality with TextKey {
    import spark.implicits._

    lazy val df2: DataFrame = Seq(
      "Hello world. This is some input text that I love.",
      "Glaciers are huge rivers of ice that ooze their way over land, powered by gravity and their own sheer weight.",
      "Hello"
    ).toDF("text2")

    val options: Option[TextAnalyticsRequestOptions] = Some(new TextAnalyticsRequestOptions()
      .setIncludeStatistics(true))

    lazy val extractor: TextAnalyticsKeyphraseExtraction = new TextAnalyticsKeyphraseExtraction(options)
      .setSubscriptionKey(textKey)
      .setEndpoint("https://eastus.api.cognitive.microsoft.com/")
      .setInputCol("text2")

    test("KPE - Basic Usage") {
      val replies = extractor.transform(df2)
        .select("keyPhrases")
        .collect()
      assert(replies(0).getSeq[String](0).toSet === Set("Hello world", "input text"))
      assert(replies(1).getSeq[String](0).toSet === Set("land", "sheer weight",
        "gravity", "way", "Glaciers", "ice", "huge rivers"))
    }
  }

