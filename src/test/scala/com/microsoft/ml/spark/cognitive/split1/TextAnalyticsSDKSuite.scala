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
  ).toDF("In")

  val options: Option[TextAnalyticsRequestOptions] = Some(new TextAnalyticsRequestOptions()
    .setIncludeStatistics(true))

  lazy val detector: TextAnalyticsLanguageDetection = new TextAnalyticsLanguageDetection(options)
    .setSubscriptionKey(textKey)
    .setEndpoint("https://eastus.api.cognitive.microsoft.com/")
    .setInputCol("In")
    .setOutputCol("Out")

  test("Language Detection - Basic Usage") {
    //assert(outputCol(0).getString(0) == "English" && outputCol(2).getString(0) == "Spanish")
    //detector.transformSchema(detector.outputSchema)
    val outputCol = detector.transform(df)
      .select("Out")
      .collect()
    assert(outputCol(0).schema(0).name == "Out")
   assert(outputCol.getItem(0))
    //df.printSchema()
    //df.show()
    outputCol.foreach { row =>
      row.toSeq.foreach{col => println(col) }
    }
  }

  test("Language Detection - Print Schema") {
    detector.transform(df).printSchema()
    detector.transform(df).show()
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
  ).toDF("Out")

  lazy val detector: TextSentimentV4 = new TextSentimentV4(options)
    .setSubscriptionKey(textKey)
    .setEndpoint("https://eastus.api.cognitive.microsoft.com/")
    .setInputCol("In - Sentiment")
    .setOutputCol("Out - Sentiment")

  test("Sentiment Analysis - Basic Usage") {
    val outputCol = detector.transform(df)
      .select("In - Sentiment")
      .collect()
  assert(outputCol(0).getString(0) == "positive" && outputCol(1).getString(0) == "negative"
      && outputCol(2).getString(0) == "neutral")
  }
}

  class KeyPhraseExtractionSuiteV4 extends TestBase with DataFrameEquality with TextKey {
    import spark.implicits._

    lazy val df2: DataFrame = Seq(
      ("en","Hello world. This is some input text that I love."),
      ("en","Glaciers are huge rivers of ice that ooze their way over land," +
        "powered by gravity and their own sheer weight."),
      ("es", "La carretera estaba atascada. Había mucho tráfico el día de ayer.")
    ).toDF("Lang", "Input")

    val options: Option[TextAnalyticsRequestOptions] = Some(new TextAnalyticsRequestOptions()
      .setIncludeStatistics(true))

    lazy val extractor: TextAnalyticsKeyphraseExtraction = new TextAnalyticsKeyphraseExtraction(options)
      .setSubscriptionKey(textKey)
      .setEndpoint("https://eastus.api.cognitive.microsoft.com/")
      .setInputCol("Input")
      .setOutputCol("Out")

    test("KPE - Basic Usage") {
      val outputCol = extractor.transform(df2)
        .select("Input", "Out")
        .collect()
      assert(outputCol(0).getSeq[String](0).toSet === Set("Hello world", "input text"))
      assert(outputCol(1).getSeq[String](0).toSet === Set("land", "sheer weight",
        "gravity", "way", "Glaciers", "ice", "huge rivers"))
    }
  }

