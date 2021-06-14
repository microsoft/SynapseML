package com.microsoft.ml.spark.cognitive.split1

import com.azure.ai.textanalytics.models.TextAnalyticsRequestOptions
import com.microsoft.ml.spark.cognitive._
import com.microsoft.ml.spark.core.test.base.TestBase
import org.apache.spark.ml.param.DataFrameEquality
import org.apache.spark.sql.DataFrame

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
    .setSubscriptionKey("91475c1eb6e14ede85d6a7bf491dacc3")
    .setEndpoint("https://assafiwus.cognitiveservices.azure.com/")
    .setInputCol("text2")

  test("Basic Usage") {
    val replies = detector.transform(df)
      .select("name", "iso6391Name")
      .collect()
//      .select(detector.getPrimaryDetectedLanguageNameCol)
//      .collect().toList
    assert(replies(0).getString(0) == "English" && replies(2).getString(0) == "Spanish")
    assert(replies(0).getString(1) == "en" && replies(2).getString(1) == "es")
  }
}
