package com.microsoft.ml.spark.cognitive.split1

import com.azure.ai.textanalytics.models.TextAnalyticsRequestOptions
import com.microsoft.ml.spark.Secrets
import com.microsoft.ml.spark.cognitive._
import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.core.test.fuzzing.TransformerFuzzing
import org.apache.spark.ml.param.DataFrameEquality
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col

class TextAnalyticsSDKSuite extends TestBase with DataFrameEquality with TextKey {
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
    .setEndpoint("https://ta-internshipconnector.cognitiveservices.azure.com/")
    .setInputCol("text2")

  test("Basic Usage") {
    val replies = detector.transform(df)
      .select("name", "iso6391Name")
      .collect()
//      .select(detector.getPrimaryDetectedLanguageNameCol)
//      .collect().toList

    assert(replies(0).getString(0) == "English" && replies(2).getString(0) == "Spanish" )
    assert(replies(3).getString(0) == "Chinese_Traditional")
    assert(replies(0).getString(1) == "en" && replies(2).getString(1) == "es")

  }
}

class TextSentimentSuiteV4 extends TestBase with DataFrameEquality with TextKey {
  import spark.implicits._

  lazy val df: DataFrame = Seq(
    "Hello world. This is some input text that I love.",
    "I am sad",
    "I am feeling okay"
  ).toDF( "text")

  val options: Option[TextAnalyticsRequestOptions] = Some(new TextAnalyticsRequestOptions()
    .setIncludeStatistics(true))

  lazy val detector: TextSentimentV4 = new TextSentimentV4(options)
    .setSubscriptionKey(textKey)
    .setEndpoint("https://ta-internshipconnector.cognitiveservices.azure.com/")
    .setInputCol("text")

  test("foo"){
    detector.transform(df).printSchema()
  }
  test("Basic Usage") {
    val replies = detector.transform(df)
      .select("sentiment", "confidenceScores", "sentences", "warnings")
      .collect()
    assert(replies(0).getString(0) == "positive" && replies(1).getString(0) == "negative"
      && replies(2).getString(0) == "neutral")
  }


}
