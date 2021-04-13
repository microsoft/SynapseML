// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.logging


import com.microsoft.ml.spark.cognitive._
import com.microsoft.ml.spark.cognitive.split1.{AnomalyKey, CognitiveKey, TextKey}
import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.stages.FixedMiniBatchTransformer
import org.apache.commons.compress.utils.IOUtils
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{col, collect_list, lit, struct}
import org.apache.spark.sql.{DataFrame, Row}

import java.net.URL

class LoggingTest extends TestBase with TextKey with CognitiveKey with AnomalyKey{

  import spark.implicits._

  test("TextSentiment test"){
    val df: DataFrame = Seq(
      ("en", "Hello world. This is some input text that I love."),
      ("fr", "Bonjour tout le monde"),
      ("es", "La carretera estaba atascada. Había mucho tráfico el día de ayer."),
      (null, "ich bin ein berliner"),
      (null, null),
      ("en", null)
    ).toDF("lang", "text")

    val t = new TextSentiment()
      .setSubscriptionKey(textKey)
      .setLocation("eastus")
      .setTextCol("text")
      .setLanguage("en")
      .setOutputCol("score")
    val batchedDF = new FixedMiniBatchTransformer().setBatchSize(10).transform(df.coalesce(1))
    val tdf = t.transform(batchedDF)
    val replies = tdf.collect().head.getAs[Seq[Row]]("score")
  }

  test("TextSentimentV2 test") {
    val df: DataFrame = Seq(
      ("en", "Hello world. This is some input text that I love."),
      ("fr", "Bonjour tout le monde"),
      ("es", "La carretera estaba atascada. Había mucho tráfico el día de ayer."),
      (null, "ich bin ein berliner"),
      (null, null),
      ("en", null)
    ).toDF("lang", "text")

    val t: TextSentimentV2 = new TextSentimentV2()
      .setSubscriptionKey(textKey)
      .setUrl("https://eastus.api.cognitive.microsoft.com/text/analytics/v2.0/sentiment")
      .setLanguageCol("lang")
      .setOutputCol("replies")

    val results = t.transform(df).withColumn("score",
      col("replies").getItem(0).getItem("score"))
      .select("score").collect().toList
  }

  test("LanguageDetectorV2 test") {
    val df: DataFrame = Seq(
      "Hello World",
      "Bonjour tout le monde",
      "La carretera estaba atascada. Había mucho tráfico el día de ayer.",
      ":) :( :D"
    ).toDF("text2")

    val detector: LanguageDetectorV2 = new LanguageDetectorV2()
      .setSubscriptionKey(textKey)
      .setUrl("https://eastus.api.cognitive.microsoft.com/text/analytics/v2.0/languages")
      .setTextCol("text2")
      .setOutputCol("replies")

    val replies = detector.transform(df)
      .withColumn("lang", col("replies").getItem(0)
        .getItem("detectedLanguages").getItem(0)
        .getItem("name"))
      .select("lang")
      .collect().toList
  }

  test("LanguageDetector test") {
    val df: DataFrame = Seq(
      "Hello World",
      "Bonjour tout le monde",
      "La carretera estaba atascada. Había mucho tráfico el día de ayer.",
      ":) :( :D"
    ).toDF("text")

    val detector: LanguageDetector = new LanguageDetector()
      .setSubscriptionKey(textKey)
      .setUrl("https://eastus.api.cognitive.microsoft.com/text/analytics/v3.0/languages")
      .setOutputCol("replies")

    val replies = detector.transform(df)
      .withColumn("lang", col("replies").getItem(0)
        .getItem("detectedLanguage")
        .getItem("name"))
      .select("lang")
      .collect().toList
  }

  test("EntityDetectorV2 test") {
    val df: DataFrame = Seq(
      ("1", "Microsoft released Windows 10"),
      ("2", "In 1975, Bill Gates III and Paul Allen founded the company.")
    ).toDF("id", "text")

    val detector: EntityDetectorV2 = new EntityDetectorV2()
      .setSubscriptionKey(textKey)
      .setUrl("https://eastus.api.cognitive.microsoft.com/text/analytics/v2.0/entities")
      .setLanguage("en")
      .setOutputCol("replies")

    val results = detector.transform(df)
      .withColumn("entities",
        col("replies").getItem(0).getItem("entities").getItem("name"))
      .select("id", "entities").collect().toList
  }

  test("EntityDetector test") {
    val df: DataFrame = Seq(
      ("1", "Microsoft released Windows 10"),
      ("2", "In 1975, Bill Gates III and Paul Allen founded the company.")
    ).toDF("id", "text")

    val detector: EntityDetector = new EntityDetector()
      .setSubscriptionKey(textKey)
      .setUrl("https://eastus.api.cognitive.microsoft.com/text/analytics/v3.0/entities/linking")
      .setLanguage("en")
      .setOutputCol("replies")

    val results = detector.transform(df)
      .withColumn("entities",
        col("replies").getItem(0).getItem("entities").getItem("name"))
      .select("id", "entities").collect().toList
  }

  test("KeyPhraseExtractorV2 test") {
    val df: DataFrame = Seq(
      ("en", "Hello world. This is some input text that I love."),
      ("fr", "Bonjour tout le monde"),
      ("es", "La carretera estaba atascada. Había mucho tráfico el día de ayer."),
      ("en", null)
    ).toDF("lang", "text")

    lazy val t: KeyPhraseExtractorV2 = new KeyPhraseExtractorV2()
      .setSubscriptionKey(textKey)
      .setUrl("https://eastus.api.cognitive.microsoft.com/text/analytics/v2.0/keyPhrases")
      .setLanguageCol("lang")
      .setOutputCol("replies")

    val results = t.transform(df).withColumn("phrases",
      col("replies").getItem(0).getItem("keyPhrases"))
      .select("phrases").collect().toList
  }

  test("KeyPhraseExtractor test") {
    val df: DataFrame = Seq(
      ("en", "Hello world. This is some input text that I love."),
      ("fr", "Bonjour tout le monde"),
      ("es", "La carretera estaba atascada. Había mucho tráfico el día de ayer."),
      ("en", null)
    ).toDF("lang", "text")

    val t: KeyPhraseExtractor = new KeyPhraseExtractor()
      .setSubscriptionKey(textKey)
      .setUrl("https://eastus.api.cognitive.microsoft.com/text/analytics/v3.0/keyPhrases")
      .setLanguageCol("lang")
      .setOutputCol("replies")

    val results = t.transform(df).withColumn("phrases",
      col("replies").getItem(0).getItem("keyPhrases"))
      .select("phrases").collect().toList
  }

  test("NERV2 test") {
    val df: DataFrame = Seq(
      ("1", "en", "Jeff bought three dozen eggs because there was a 50% discount."),
      ("2", "en", "The Great Depression began in 1929. By 1933, the GDP in America fell by 25%.")
    ).toDF("id", "language", "text")

    lazy val n: NERV2 = new NERV2()
      .setSubscriptionKey(textKey)
      .setLocation("eastus")
      .setLanguage("en")
      .setOutputCol("response")

    val results = n.transform(df)
    val matches = results.withColumn("match",
      col("response")
        .getItem(0)
        .getItem("entities")
        .getItem("matches")
        .getItem(0)
        .getItem(0))
      .select("match")

    val testRow = matches.collect().head(0).asInstanceOf[GenericRowWithSchema]
  }

  test("NER test") {
    val df: DataFrame = Seq(
      ("1", "en", "I had a wonderful trip to Seattle last week."),
      ("2", "en", "I visited Space Needle 2 times.")
    ).toDF("id", "language", "text")

    lazy val n: NER = new NER()
      .setSubscriptionKey(textKey)
      .setLocation("eastus")
      .setLanguage("en")
      .setOutputCol("response")

    val results = n.transform(df)
    val matches = results.withColumn("match",
      col("response")
        .getItem(0)
        .getItem("entities")
        .getItem(0))
      .select("match")

    val testRow = matches.collect().head(0).asInstanceOf[GenericRowWithSchema]
  }

  test("SpeechToText test") {
    val stt = new SpeechToText()
      .setSubscriptionKey(cognitiveKey)
      .setLocation("eastus")
      .setOutputCol("text")
      .setAudioDataCol("audio")
      .setLanguage("en-US")

    val audioBytes: Array[Byte] = {
      IOUtils.toByteArray(new URL("https://mmlspark.blob.core.windows.net/datasets/Speech/test1.wav").openStream())
    }
    val df: DataFrame = Seq(
      Tuple1(audioBytes)
    ).toDF("audio")

    val toObj: Row => SpeechResponse = SpeechResponse.makeFromRowConverter
    val result = toObj(stt.setFormat("simple")
      .transform(df).select("text")
      .collect().head.getStruct(0))
  }

  val df: DataFrame = Seq(
    ("1972-01-01T00:00:00Z", 826.0),
    ("1972-02-01T00:00:00Z", 799.0),
    ("1972-03-01T00:00:00Z", 890.0),
    ("1972-04-01T00:00:00Z", 900.0),
    ("1972-05-01T00:00:00Z", 766.0),
    ("1972-06-01T00:00:00Z", 805.0),
    ("1972-07-01T00:00:00Z", 821.0),
    ("1972-08-01T00:00:00Z", 20000.0),
    ("1972-09-01T00:00:00Z", 883.0),
    ("1972-10-01T00:00:00Z", 898.0),
    ("1972-11-01T00:00:00Z", 957.0),
    ("1972-12-01T00:00:00Z", 924.0),
    ("1973-01-01T00:00:00Z", 881.0),
    ("1973-02-01T00:00:00Z", 837.0),
    ("1973-03-01T00:00:00Z", 90000.0)
  ).toDF("timestamp","value")
    .withColumn("group", lit(1))
    .withColumn("inputs", struct(col("timestamp"), col("value")))
    .groupBy(col("group"))
    .agg(collect_list(col("inputs")).alias("inputs"))

  test("DetectLastAnomaly test"){
    val ad = new DetectLastAnomaly()
      .setSubscriptionKey(anomalyKey)
      .setLocation("westus2")
      .setOutputCol("anomalies")
      .setSeriesCol("inputs")
      .setGranularity("monthly")
      .setErrorCol("errors")

    val fromRow = ADLastResponse.makeFromRowConverter
    val result = fromRow(ad.transform(df)
      .select("anomalies")
      .collect()
      .head.getStruct(0))
  }

  test("DetectAnomalies test"){
    val ad = new DetectAnomalies()
      .setSubscriptionKey(anomalyKey)
      .setLocation("westus2")
      .setOutputCol("anomalies")
      .setSeriesCol("inputs")
      .setGranularity("monthly")

    val fromRow = ADEntireResponse.makeFromRowConverter
    val result = fromRow(ad.transform(df)
      .select("anomalies")
      .collect()
      .head.getStruct(0))
  }

  test("SimpleDetectAnomalies test"){
    val baseSeq = Seq(
      ("1972-01-01T00:00:00Z", 826.0),
      ("1972-02-01T00:00:00Z", 799.0),
      ("1972-03-01T00:00:00Z", 890.0),
      ("1972-04-01T00:00:00Z", 900.0),
      ("1972-05-01T00:00:00Z", 766.0),
      ("1972-06-01T00:00:00Z", 805.0),
      ("1972-07-01T00:00:00Z", 821.0),
      ("1972-08-01T00:00:00Z", 20000.0),
      ("1972-09-01T00:00:00Z", 883.0),
      ("1972-10-01T00:00:00Z", 898.0),
      ("1972-11-01T00:00:00Z", 957.0),
      ("1972-12-01T00:00:00Z", 924.0),
      ("1973-01-01T00:00:00Z", 881.0),
      ("1973-02-01T00:00:00Z", 837.0),
      ("1973-03-01T00:00:00Z", 9000.0)
    )

    val sdf: DataFrame = baseSeq.map(p => (p._1,p._2,1.0))
      .++(baseSeq.map(p => (p._1,p._2,2.0)))
      .toDF("timestamp","value","group")

    val sad = new SimpleDetectAnomalies()
      .setSubscriptionKey(anomalyKey)
      .setLocation("westus2")
      .setOutputCol("anomalies")
      .setGroupbyCol("group")
      .setGranularity("monthly")

    val result = sad.transform(sdf)
      .collect().head.getAs[Row]("anomalies")
  }

}
