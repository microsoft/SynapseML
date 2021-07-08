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
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema



class DetectedLanguageSuiteV4 extends TestBase with DataFrameEquality with TextKey {
  import spark.implicits._
  lazy val df: DataFrame = Seq(
    "Hello World",
    "Bonjour tout le monde",
    "La carretera estaba atascada. Había mucho tráfico el día de ayer.",
    "世界您好",
    ":) :( :D",
  ).toDF("In - Language")

  val options: Option[TextAnalyticsRequestOptions] = Some(new TextAnalyticsRequestOptions()
    .setIncludeStatistics(true))

  lazy val detector: TextAnalyticsLanguageDetection = new TextAnalyticsLanguageDetection(options)
    .setSubscriptionKey(textKey)
    .setEndpoint("https://eastus.api.cognitive.microsoft.com/")
    .setInputCol("In - Language")
    .setOutputCol("Out - Language")

  test("Language Detection - Basic Usage") {
    val outputCol = detector.transform(df)
      .select("Out - Language")
      .collect()
    assert(outputCol(0).schema(0).name == "Out - Language")

    outputCol.foreach(row => {
      var outResponse: GenericRowWithSchema = row.getAs[GenericRowWithSchema]("Out - Language")

      val result = outResponse.getAs[GenericRowWithSchema]("result");
      val error = outResponse.getAs[GenericRowWithSchema]("error");
      val statistics = outResponse.getAs[GenericRowWithSchema]("statistics");
      val modelVersion = outResponse.getAs[String]("modelVersion");
      val language = result.getAs[String]("name")
      val langCode = result.getAs[String]("iso6391Name")
      val confidence = result.getAs[Double]("confidenceScore")

      assert(language == "English")
      assert(langCode == "en")
      assert(modelVersion == "2021-01-05")
      assert(confidence == 0.81)
    });
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
  ).toDF("In - Sentiment")

  lazy val detector: TextSentimentV4 = new TextSentimentV4(options)
    .setSubscriptionKey(textKey)
    .setEndpoint("https://eastus.api.cognitive.microsoft.com/")
    .setInputCol("In - Sentiment")
    .setOutputCol("Out - Sentiment")

  test("Sentiment Analysis - Basic Usage") {
    val outputCol = detector.transform(df)
      .select("Out - Sentiment")
      .collect()
    assert(outputCol(0).schema(0).name == "Out - Sentiment")
    df.printSchema()
    df.show()
    outputCol.foreach { row =>
      row.toSeq.foreach { col => println(col) }
    }
  }
}

  class KeyPhraseExtractionSuiteV4 extends TestBase with DataFrameEquality with TextKey {
    import spark.implicits._

    lazy val df2: DataFrame = Seq(
      ("en","Hello world. This is some input text that I love."),
      ("en","Glaciers are huge rivers of ice that ooze their way over land," +
        "powered by gravity and their own sheer weight."),
      ("es", "La carretera estaba atascada. Había mucho tráfico el día de ayer.")
    ).toDF("In - Key Phrase", "Out - Key Phrase")

    lazy val df3: DataFrame = Seq(
      ("es","Hola, como estas?"),
      ("en","Glaciers are huge rivers of ice that ooze their way over land," +
        "powered are gravity and their own sheer weight."),
      ("en", "Hi, my name is Sally.")
    ).toDF("In - Key Phrase", "Out - Key Phrase")

    val options: Option[TextAnalyticsRequestOptions] = Some(new TextAnalyticsRequestOptions()
      .setIncludeStatistics(true))

    lazy val extractor: TextAnalyticsKeyphraseExtraction = new TextAnalyticsKeyphraseExtraction(options)
      .setSubscriptionKey(textKey)
      .setEndpoint("https://eastus.api.cognitive.microsoft.com/")
      .setInputCol("In - Key Phrase")
      .setOutputCol("Out - Key Phrase")

      test("KPE - Error in Language Test") {
        val outputCol = extractor.transform(df3)
          .select("In - Key Phrase", "Out - Key Phrase")
          .collect()
        assert(outputCol(0).schema(0).name == "In - Key Phrase")
        df3.printSchema()
        df3.show()
        outputCol.foreach(row => {
          var outResponse: GenericRowWithSchema = row.getAs[GenericRowWithSchema]("Out - Key Phrase")
          val result = outResponse.getAs[GenericRowWithSchema]("result");
          val keyPhrases = outResponse.getAs[GenericRowWithSchema]("keyPhrases");
          val kpe = result.getAs[String]("keyPhrases")
          assert(kpe == "are")
        }
        )};
      }