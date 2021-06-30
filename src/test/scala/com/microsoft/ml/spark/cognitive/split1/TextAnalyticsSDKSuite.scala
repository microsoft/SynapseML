package com.microsoft.ml.spark.cognitive.split1

import com.azure.ai.textanalytics.models.TextAnalyticsRequestOptions
import com.microsoft.ml.spark.Secrets
import com.microsoft.ml.spark.cognitive._
import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.core.test.fuzzing.TransformerFuzzing
import com.microsoft.ml.spark.stages.FixedMiniBatchTransformer
import org.apache.spark.ml.param.DataFrameEquality
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col

class DetectedLanguageSuitev4 extends TestBase with DataFrameEquality with TextKey {

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

  test("Language Detection - Basic Usage") {
    val replies = detector.transform(df)
      .select("name", "iso6391Name")
      .collect()

    assert(replies(0).getString(0) == "English" && replies(2).getString(0) == "Spanish")
    assert(replies(3).getString(0) == "Chinese_Traditional")
    assert(replies(0).getString(1) == "en" && replies(2).getString(1) == "es")

  }

  test("Language Detection - Batch Usage") {
    val batchedDF = new FixedMiniBatchTransformer().setBatchSize(10).transform(df.coalesce(1))
    val tdf = detector.transform(batchedDF)
    val replies = tdf.collect().head.getAs[Seq[Row]]("replies")
    assert(replies.length == 4)
    val languages = replies.map(_.getAs[Seq[Row]]("detectedLanguages").head.getAs[String]("name")).toSet
    assert(languages("Spanish") && languages("English"))
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
    .setEndpoint("endpoint")
    .setInputCol("text")

  test("Sentiment Analysis - Basic Usage") {
    val replies = detector.transform(df)
      .select("sentiment", "confidenceScores", "sentences", "warnings")
      .collect()
    assert(replies(0).getString(0) == "positive" && replies(1).getString(0) == "negative"
      && replies(2).getString(0) == "neutral")
  }


}
