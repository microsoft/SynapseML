// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive.split1

import com.microsoft.ml.spark.cognitive._
import com.microsoft.ml.spark.core.test.base.{Flaky, TestBase}
import com.microsoft.ml.spark.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, flatten}

trait TranslatorUtils extends TestBase {

  import spark.implicits._

  lazy val textDf1: DataFrame = Seq(List("Hello, what is your name?")).toDF("text")

  lazy val textDf2: DataFrame = Seq(List("Hello, what is your name?", "Bye")).toDF("text")

  lazy val textDf3: DataFrame = Seq(List("This is bullshit.")).toDF("text")

  lazy val textDf4: DataFrame = Seq(List("<div class=\"notranslate\">This will not be translated." +
    "</div><div>This will be translated.</div>")).toDF("text")

  lazy val textDf5: DataFrame = Seq(List("The word <mstrans:dictionary translation=wordomatic>word " +
    "or phrase</mstrans:dictionary> is a dictionary entry.")).toDF("text")

}

class TranslateSuite extends TransformerFuzzing[Translate]
  with CognitiveKey with Flaky with TranslatorUtils {

  lazy val translate: Translate = new Translate()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setApiVersion(3.0)
    .setToLanguage(Seq("zh-Hans"))
    .setTextCol("text")
    .setOutputCol("translation")
    .setConcurrency(5)

  lazy val translate2: Translate = new Translate()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setApiVersion(3.0)
    .setToLanguage(Seq("zh-Hans"))
    .setToScript("Latn")
    .setTextCol("text")
    .setOutputCol("translation")
    .setConcurrency(5)

  lazy val translate3: Translate = new Translate()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setApiVersion(3.0)
    .setFromLanguage("en")
    .setToLanguage(Seq("fr"))
    .setIncludeAlignment(true)
    .setTextCol("text")
    .setOutputCol("translation")
    .setConcurrency(5)

  lazy val translate4: Translate = new Translate()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setApiVersion(3.0)
    .setFromLanguage("en")
    .setToLanguage(Seq("fr"))
    .setIncludeSentenceLength(true)
    .setTextCol("text")
    .setOutputCol("translation")
    .setConcurrency(5)

  test("Translate multiple pieces of text with language autodetection") {
    val results = translate.transform(textDf2)
      .withColumn("translation", flatten(col("translation.translations")))
      .withColumn("translation", col("translation.text"))
      .select("translation").collect()
    val headStr = results.head.getSeq(0).mkString("\n")
    assert(headStr === "你好，你叫什么名字？\n再见")
  }

  test("Translate with transliteration") {
    val results = translate2
      .transform(textDf1)
      .withColumn("translation", flatten(col("translation.translations")))
      .withColumn("transliteration", col("translation.transliteration.text"))
      .withColumn("translation", col("translation.text"))
      .select("translation", "transliteration").collect()
    assert(results.head.getSeq(0).mkString("\n") === "你好，你叫什么名字？")
    assert(results.head.getSeq(1).mkString("\n") === "nǐ hǎo ， nǐ jiào shén me míng zì ？")
  }

  test("Translate to multiple languages") {
    val results = translate
      .setToLanguage(Seq("zh-Hans", "de"))
      .transform(textDf1)
      .withColumn("translation", flatten(col("translation.translations")))
      .withColumn("translation", col("translation.text"))
      .select("translation").collect()
    val headStr = results.head.getSeq(0).mkString("\n")
    assert(headStr === "你好，你叫什么名字？\nHallo, wie heißt du?")
  }

  test("Handle profanity") {
    val results = translate
      .setFromLanguage("en")
      .setToLanguage(Seq("de"))
      .setProfanityAction("Marked")
      .transform(textDf3)
      .withColumn("translation", flatten(col("translation.translations")))
      .withColumn("translation", col("translation.text"))
      .select("translation").collect()
    val headStr = results.head.getSeq(0).mkString("\n")
    assert(headStr === "Das ist ***.") // problem with Rest API "freaking" -> the marker disappears *** no difference
  }

  test("Translate content with markup and decide what's translated") {
    val results = translate
      .setFromLanguage("en")
      .setToLanguage(Seq("zh-Hans"))
      .setTextType("html")
      .transform(textDf4)
      .withColumn("translation", flatten(col("translation.translations")))
      .withColumn("translation", col("translation.text"))
      .select("translation").collect()
    val headStr = results.head.getSeq(0).mkString("\n")
    assert(headStr === "<div class=\"notranslate\">This will not be translated.</div><div>这将被翻译。</div>")
  }

  test("Obtain alignment information") {
    val results = translate3
      .transform(textDf1)
      .withColumn("translation", flatten(col("translation.translations")))
      .withColumn("alignment", col("translation.alignment.proj"))
      .withColumn("translation", col("translation.text"))
      .select("translation", "alignment").collect()
    assert(results.head.getSeq(0).mkString("\n") === "Bonjour, quel est votre nom?")
    assert(results.head.getSeq(1).mkString("\n") === "0:5-0:7 7:10-9:12 12:13-14:16 15:18-18:22 20:24-24:27")
  }

  test("Obtain sentence boundaries") {
    val results = translate4
      .transform(textDf1)
      .withColumn("translation", flatten(col("translation.translations")))
      .withColumn("srcSentLen", flatten(col("translation.sentLen.srcSentLen")))
      .withColumn("transSentLen", flatten(col("translation.sentLen.transSentLen")))
      .withColumn("translation", col("translation.text"))
      .select("translation", "srcSentLen", "transSentLen").collect()
    assert(results.head.getSeq(0).mkString("\n") === "Bonjour, quel est votre nom?")
    assert(results.head.getSeq(1).mkString("\n") === "25")
    assert(results.head.getSeq(2).mkString("\n") === "28")
  }

  test("Translate with dynamic dictionary") {
    val results = translate
      .setToLanguage(Seq("de"))
      .transform(textDf5)
      .withColumn("translation", flatten(col("translation.translations")))
      .withColumn("translation", col("translation.text"))
      .select("translation").collect()
    assert(results.head.getSeq(0).mkString("\n") === "Das Wort wordomatic ist ein Wörterbucheintrag.")
  }

  override def testObjects(): Seq[TestObject[Translate]] =
    Seq(new TestObject(translate, textDf1))

  override def reader: MLReadable[_] = Translate
}

class TransliterateSuite extends TransformerFuzzing[Transliterate]
  with CognitiveKey with Flaky with TranslatorUtils {

  import spark.implicits._

  lazy val transDf: DataFrame = Seq(List("こんにちは", "さようなら")).toDF("text")

  lazy val transliterate: Transliterate = new Transliterate()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setApiVersion(3.0)
    .setLanguage("ja")
    .setFromScript("Jpan")
    .setToScript("Latn")
    .setTextCol("text")
    .setOutputCol("result")

  test("Transliterate") {
    val results = transliterate.transform(transDf)
      .withColumn("text", col("result.text"))
      .withColumn("script", col("result.script"))
      .select("text", "script").collect()
    assert(results.head.getSeq(0).mkString("\n") === "Kon'nichiwa\nsayonara")
    assert(results.head.getSeq(1).mkString("\n") === "Latn\nLatn")
  }

  override def testObjects(): Seq[TestObject[Transliterate]] =
    Seq(new TestObject(transliterate, transDf))

  override def reader: MLReadable[_] = Transliterate
}

class DetectSuite extends TransformerFuzzing[Detect]
  with CognitiveKey with Flaky with TranslatorUtils {

  lazy val detect: Detect = new Detect()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setApiVersion(3.0)
    .setTextCol("text")
    .setOutputCol("result")

  test("Detect language") {
    val results = detect.transform(textDf1)
      .withColumn("language", col("result.language"))
      .select("language").collect()
    val headStr = results.head.getSeq(0).mkString("\n")
    assert(headStr === "en")
  }

  override def testObjects(): Seq[TestObject[Detect]] =
    Seq(new TestObject(detect, textDf1))

  override def reader: MLReadable[_] = Detect
}

class BreakSentenceSuite extends TransformerFuzzing[BreakSentence]
  with CognitiveKey with Flaky with TranslatorUtils {

  lazy val breakSentence: BreakSentence = new BreakSentence()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setApiVersion(3.0)
    .setTextCol("text")
    .setOutputCol("result")

  test("Break sentence ") {
    val results = breakSentence.transform(textDf1)
      .withColumn("sentLen", flatten(col("result.sentLen")))
      .select("sentLen").collect()
    val headStr = results.head.getSeq(0).mkString("\n")
    assert(headStr === "25")
  }

  override def testObjects(): Seq[TestObject[BreakSentence]] =
    Seq(new TestObject(breakSentence, textDf1))

  override def reader: MLReadable[_] = BreakSentence
}

class DictionaryLookupSuite extends TransformerFuzzing[DictionaryLookup]
  with CognitiveKey with Flaky with TranslatorUtils {

  import spark.implicits._

  lazy val dictDf: DataFrame = Seq(List("fly")).toDF("text")

  lazy val dictionaryLookup: DictionaryLookup = new DictionaryLookup()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setApiVersion(3.0)
    .setFrom("en")
    .setTo("es")
    .setTextCol("text")
    .setOutputCol("result")

  test("Break sentence ") {
    val results = dictionaryLookup.transform(dictDf)
      .withColumn("translations", flatten(col("result.translations")))
      .withColumn("normalizedTarget", col("translations.normalizedTarget"))
      .select("normalizedTarget").collect()
    val headStr = results.head.getSeq(0).mkString("\n")
    assert(headStr === "volar\nmosca\noperan\npilotar\nmoscas\nmarcha")
  }

  override def testObjects(): Seq[TestObject[DictionaryLookup]] =
    Seq(new TestObject(dictionaryLookup, dictDf))

  override def reader: MLReadable[_] = DictionaryLookup
}

class DictionaryExamplesSuite extends TransformerFuzzing[DictionaryExamples]
  with CognitiveKey with Flaky with TranslatorUtils {

  import spark.implicits._

  lazy val dictDf: DataFrame = Seq(List(("fly", "volar"))).toDF("textAndTranslation")

  lazy val dictionaryExamples: DictionaryExamples = new DictionaryExamples()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setApiVersion(3.0)
    .setFrom("en")
    .setTo("es")
    .setTextAndTranslationCol("textAndTranslation")
    .setOutputCol("result")

  test("Dictionary Examples") {
    val results = dictionaryExamples.transform(dictDf)
      .withColumn("examples", flatten(col("result.examples")))
      .withColumn("sourceTerm", col("examples.sourceTerm"))
      .withColumn("targetTerm", col("examples.targetTerm"))
      .select("sourceTerm", "targetTerm").collect()
    assert(results.head.getSeq(0).head.toString === "fly")
    assert(results.head.getSeq(1).head.toString === "volar")
  }

  override def testObjects(): Seq[TestObject[DictionaryExamples]] =
    Seq(new TestObject(dictionaryExamples, dictDf))

  override def reader: MLReadable[_] = DictionaryExamples
}
