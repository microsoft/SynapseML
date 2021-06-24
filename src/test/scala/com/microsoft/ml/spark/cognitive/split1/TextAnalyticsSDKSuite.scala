package com.microsoft.ml.spark.cognitive.split1

import com.azure.ai.textanalytics.models.TextAnalyticsRequestOptions
import com.microsoft.ml.spark.cognitive.{CognitiveServicesBase, _}
import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.stages.FixedMiniBatchTransformer
import org.apache.spark.ml.param.DataFrameEquality
import org.apache.spark.sql.{DataFrame, Row}

class TextAnalyticsSDKSuite extends TestBase with DataFrameEquality with TextKey {

  import spark.implicits._

  lazy val df: DataFrame = Seq(
    "Hello World",
    "Bonjour tout le monde",
    "La carretera estaba atascada. Había mucho tráfico el día de ayer.",
    ":) :( :D"
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
    assert(replies(0).getString(0) == "English" && replies(2).getString(0) == "Spanish")
    assert(replies(0).getString(1) == "en" && replies(2).getString(1) == "es")
  }

  lazy val df2: DataFrame = Seq(
    ("en","Hello world. This is some input text that I love."),
    ("en","Glaciers are huge rivers of ice that ooze their way over land," +
      "powered by gravity and their own sheer weight."),
    ("es", "La carretera estaba atascada. Había mucho tráfico el día de ayer.")
  ).toDF("langCol","text2")

  lazy val extractor: TextAnalyticsKeyphraseExtraction = new TextAnalyticsKeyphraseExtraction(options)
    .setSubscriptionKey(textKey)
    .setEndpoint("https://ta-internshipconnector.cognitiveservices.azure.com/")
    .setInputCol("text2")

  test("Basic KPE Usage") {
    val replies = extractor.transform(df2)
      .collect()
//      .select("keyPhrases")
//      .collect()
    assert(replies(0).getSeq[String](0).toSet === Set("Hello world", "input text"))
    assert(replies(1).getSeq[String](0).toSet === Set("land", "sheer weight",
      "gravity", "way", "Glaciers", "ice", "huge rivers"))
    assert(replies(2).getSeq[String](0).toSet === Set("carretera", "tráfico", "día"))
  }
}
