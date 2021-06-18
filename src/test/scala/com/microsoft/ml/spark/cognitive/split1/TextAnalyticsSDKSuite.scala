package com.microsoft.ml.spark.cognitive.split1

import com.azure.ai.textanalytics.models.TextAnalyticsRequestOptions
import com.microsoft.ml.spark.cognitive._
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
  /*test("Batch Usage") {
    val batchedDF = new FixedMiniBatchTransformer().setBatchSize(10).transform(df.coalesce(1))
    val tdf = detector.transform(batchedDF)
    val replies = tdf
    //collect().head.getAs[Seq[Row]]("replies")
    assert(replies.length == 4)
    val languages = replies.map(_.getAs[Seq[Row]]("detectedLanguages").head.getAs[String]("name")).toSet
    assert(languages("Spanish") && languages("English"))


  } */

  lazy val extractor: TextAnalyticsKeyphraseExtraction = new TextAnalyticsKeyphraseExtraction(options)
    .setSubscriptionKey(textKey)
    .setEndpoint("https://ta-internshipconnector.cognitiveservices.azure.com/")
    .setInputCol("text2")

  test("Basic KPE Usage") {
    val replies = extractor.transform(df)
      .select("keyPhrases")
      .collect()
    assert(replies(0).getString(0) == "Hello")
  }
}
