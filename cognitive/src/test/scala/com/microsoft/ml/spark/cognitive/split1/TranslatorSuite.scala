// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive.split1

import com.microsoft.ml.spark.Secrets
import com.microsoft.ml.spark.cognitive._
import com.microsoft.ml.spark.core.test.base.{Flaky, TestBase}
import com.microsoft.ml.spark.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, flatten}
import org.scalactic.Equality

trait TranslatorKey {
  lazy val translatorKey: String = sys.env.getOrElse("TRANSLATOR_KEY", Secrets.TranslatorKey)

  lazy val translatorName: String = "mmlspark-translator"
}

trait TranslatorUtils extends TestBase {

  import spark.implicits._

  lazy val textDf1: DataFrame = Seq(List("Hello, what is your name?")).toDF("text")

  lazy val textDf2: DataFrame = Seq(List("Hello, what is your name?", "Bye")).toDF("text")

  lazy val textDf3: DataFrame = Seq(List("This is bullshit.")).toDF("text")

  lazy val textDf4: DataFrame = Seq(List("<div class=\"notranslate\">This will not be translated." +
    "</div><div>This will be translated.</div>")).toDF("text")

  lazy val textDf5: DataFrame = Seq(List("The word <mstrans:dictionary translation=wordomatic>word " +
    "or phrase</mstrans:dictionary> is a dictionary entry.")).toDF("text")

  lazy val textDf6: DataFrame = Seq("Hi, this is Synapse!", "Yes!").toDF("text")

  lazy val textDf7: DataFrame = Seq(("Hi, this is Synapse!", "zh-Hans"),
    (null, "zh-Hans"))
    .toDF("text", "language")

  lazy val textDf8: DataFrame = Seq(("test", null)).toDF("text", "language")

}

class TranslateSuite extends TransformerFuzzing[Translate]
  with TranslatorKey with Flaky with TranslatorUtils {

  def translate: Translate = new Translate()
    .setSubscriptionKey(translatorKey)
    .setLocation("eastus")
    .setTextCol("text")
    .setOutputCol("translation")
    .setConcurrency(5)

  def translationTextTest(translator: Translate,
                          df: DataFrame,
                          expectString: String): Boolean = {
    val results = translator
      .transform(df)
      .withColumn("translation", flatten(col("translation.translations")))
      .withColumn("translation", col("translation.text"))
      .select("translation").collect()
    val headStr = results.head.getSeq(0).mkString("\n")
    headStr === expectString
  }

  test("Translate multiple pieces of text with language autodetection") {
    assert(
      translationTextTest(
        translate.setToLanguage(Seq("zh-Hans")), textDf2, "你好，你叫什么名字？\n再见"
      )
    )

    assert(
      translationTextTest(
        translate.setToLanguage("zh-Hans"), textDf6, "嗨， 这是突触！"
      )
    )

    val translate1: Translate = new Translate()
      .setSubscriptionKey(translatorKey)
      .setLocation("eastus")
      .setText("Hi, this is Synapse!")
      .setOutputCol("translation")
      .setConcurrency(5)
    assert(
      translationTextTest(
        translate1.setToLanguage("zh-Hans"), textDf6, "嗨， 这是突触！"
      )
    )

    val translate2: Translate = new Translate()
      .setSubscriptionKey(translatorKey)
      .setLocation("eastus")
      .setTextCol("text")
      .setToLanguageCol("language")
      .setOutputCol("translation")
      .setConcurrency(5)
    val results = translate2.transform(textDf7)
      .withColumn("translation", flatten(col("translation.translations")))
      .withColumn("translation", col("translation.text"))
      .select("translation").collect()
    assert(results.head.getSeq(0).mkString("") == "嗨， 这是突触！")
    assert(results(1).get(0) == null)
  }

  test("Translate triggers errors if required fields not set") {
    try {
      translate.transform(textDf2).collect()
    } catch {
      case e: Exception => assert(e.getCause.getMessage.contains("required param undefined"))
    }
    try {
      translate.setToLanguageCol("language").transform(textDf8).collect()
    } catch {
      case e: Exception => assert(e.getCause.getMessage.contains("required param undefined"))
    }
  }

  test("Translate with transliteration") {
    val results = translate
      .setToLanguage(Seq("zh-Hans"))
      .setToScript("Latn")
      .transform(textDf1)
      .withColumn("translation", flatten(col("translation.translations")))
      .withColumn("transliteration", col("translation.transliteration.text"))
      .withColumn("translation", col("translation.text"))
      .select("translation", "transliteration").collect()
    assert(results.head.getSeq(0).mkString("\n") === "你好，你叫什么名字？")
    assert(results.head.getSeq(1).mkString("\n") === "nǐ hǎo ， nǐ jiào shén me míng zì ？")
  }

  test("Translate to multiple languages") {
    assert(
      translationTextTest(
        translate.setToLanguage(Seq("zh-Hans", "de")), textDf1, "你好，你叫什么名字？\nHallo, wie heißt du?"
      )
    )
  }

  test("Handle profanity") {
    assert(
      translationTextTest(
        translate.setFromLanguage("en").setToLanguage(Seq("de")).setProfanityAction("Marked"),
        textDf3,
        "Das ist ***." // problem with Rest API "freaking" -> the marker disappears *** no difference
      )
    )
  }

  test("Translate content with markup and decide what's translated") {
    assert(
      translationTextTest(
        translate.setFromLanguage("en").setToLanguage(Seq("zh-Hans")).setTextType("html"),
        textDf4,
        "<div class=\"notranslate\">This will not be translated.</div><div>这将被翻译。</div>"
      )
    )
  }

  test("Obtain alignment information") {
    val results = translate
      .setFromLanguage("en")
      .setToLanguage(Seq("fr"))
      .setIncludeAlignment(true)
      .transform(textDf1)
      .withColumn("translation", flatten(col("translation.translations")))
      .withColumn("alignment", col("translation.alignment.proj"))
      .withColumn("translation", col("translation.text"))
      .select("translation", "alignment").collect()
    assert(results.head.getSeq(0).mkString("\n") === "Bonjour, quel est votre nom?")
    assert(results.head.getSeq(1).mkString("\n") === "0:5-0:7 7:10-9:12 12:13-14:16 15:18-18:22 20:24-24:27")
  }

  test("Obtain sentence boundaries") {
    val results = translate
      .setFromLanguage("en")
      .setToLanguage(Seq("fr"))
      .setIncludeSentenceLength(true)
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
    assert(
      translationTextTest(
        translate.setToLanguage(Seq("de")), textDf5, "Das Wort wordomatic ist ein Wörterbucheintrag."
      )
    )
  }

  override def testObjects(): Seq[TestObject[Translate]] =
    Seq(new TestObject(translate.setToLanguage(Seq("zh-Hans")), textDf1))

  override def reader: MLReadable[_] = Translate
}

class TransliterateSuite extends TransformerFuzzing[Transliterate]
  with TranslatorKey with Flaky with TranslatorUtils {

  import spark.implicits._

  lazy val transDf: DataFrame = Seq(List("こんにちは", "さようなら")).toDF("text")

  def transliterate: Transliterate = new Transliterate()
    .setSubscriptionKey(translatorKey)
    .setLocation("eastus")
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
  with TranslatorKey with Flaky with TranslatorUtils {

  def detect: Detect = new Detect()
    .setSubscriptionKey(translatorKey)
    .setLocation("eastus")
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
  with TranslatorKey with Flaky with TranslatorUtils {

  def breakSentence: BreakSentence = new BreakSentence()
    .setSubscriptionKey(translatorKey)
    .setLocation("eastus")
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
  with TranslatorKey with Flaky with TranslatorUtils {

  import spark.implicits._

  lazy val dictDf: DataFrame = Seq(List("fly")).toDF("text")

  def dictionaryLookup: DictionaryLookup = new DictionaryLookup()
    .setSubscriptionKey(translatorKey)
    .setLocation("eastus")
    .setFromLanguage("en")
    .setToLanguage("es")
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
  with TranslatorKey with Flaky with TranslatorUtils {

  import spark.implicits._

  lazy val dictDf: DataFrame = Seq(List(("fly", "volar"))).toDF("textAndTranslation")

  def dictionaryExamples: DictionaryExamples = new DictionaryExamples()
    .setSubscriptionKey(translatorKey)
    .setLocation("eastus")
    .setFromLanguage("en")
    .setToLanguage("es")
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

// TODO add this test back in when fixed
//class DocumentTranslatorSuite extends TransformerFuzzing[DocumentTranslator]
//  with TranslatorKey with Flaky {
//
//  import spark.implicits._
//
//  // TODO: Replace containerSasToken after 2022-07-13
//  lazy val containerSasToken: String = "?sp=rwl&st=2021-07-12T03:27:50Z&se=2022-07-13T03:27:00Z" +
//    "&sv=2020-08-04&sr=c&sig=lQdMII5ZgiBNXGJk77PWwye27sR6XpP4RhPgmkhUnG0%3D"
//
//  lazy val urlRoot: String = "https://mmlspark.blob.core.windows.net/datasets"
//
//  lazy val sourceUrl: String = urlRoot + containerSasToken
//
//  lazy val fileSourceUrl: String = urlRoot + "/Translator/source/document-translation-sample.pdf" + containerSasToken
//
//  lazy val targetUrl: String = urlRoot + "/Translator/test-zh-Hans-" + documentTranslator.uid + containerSasToken
//
//  lazy val targetUrl2: String = urlRoot + "/Translator/test-zh-Hans-" +
//    documentTranslator.uid + "-2" + containerSasToken
//
//  lazy val targetUrl3: String = urlRoot + "/Translator/test-zh-Hans-" +
//    documentTranslator.uid + "-3" + containerSasToken
//
//  lazy val targetFileUrl1: String = urlRoot + "/Translator/translator-target/" +
//    "document-translation-sample-zh-Hans-" + documentTranslator.uid + ".pdf" + containerSasToken
//
//  lazy val targetFileUrl2: String = urlRoot + "/Translator/translator-target/" +
//    "document-translation-sample-de-" + documentTranslator.uid + ".pdf" + containerSasToken
//
//  lazy val glossaryUrl: String = urlRoot + "/Translator/glossary/glossary.tsv" + containerSasToken
//
//  lazy val docTranslationDf: DataFrame = Seq((sourceUrl,
//    "Translator/source/",
//    Seq(TargetInput(None, None, targetUrl, "zh-Hans", None))))
//    .toDF("sourceUrl", "filterPrefix", "targets")
//
//  lazy val docTranslationDf2: DataFrame = Seq((sourceUrl,
//    "Translator/source/",
//    Seq(TargetInput(None, None, targetUrl2, "zh-Hans", None))))
//    .toDF("sourceUrl", "filterPrefix", "targets")
//
//  lazy val docTranslationDf3: DataFrame = Seq((sourceUrl,
//    "Translator/source/",
//    Seq(TargetInput(None, Some(Seq(Glossary(
//      "TSV", glossaryUrl, None, None
//    ))), targetUrl3, "zh-Hans", None))))
//    .toDF("sourceUrl", "filterPrefix", "targets")
//
//  lazy val docTranslationDf4: DataFrame = Seq((fileSourceUrl,
//    "File",
//    Seq(TargetInput(None, None, targetFileUrl1, "zh-Hans", None),
//      TargetInput(None, None, targetFileUrl2, "de", None))))
//    .toDF("sourceUrl", "storageType", "targets")
//
//  def documentTranslator: DocumentTranslator = new DocumentTranslator()
//    .setSubscriptionKey(translatorKey)
//    .setServiceName(translatorName)
//    .setSourceUrlCol("sourceUrl")
//    .setTargetsCol("targets")
//    .setOutputCol("translationStatus")
//
//  lazy val documentTranslator1: DocumentTranslator = documentTranslator.setFilterPrefixCol("filterPrefix")
//
//  lazy val documentTranslator2: DocumentTranslator = documentTranslator.setStorageTypeCol("storageType")
//
//  test("Translating all documents under folder in a container") {
//    val result = documentTranslator1
//      .transform(docTranslationDf2)
//      .withColumn("totalNumber", col("translationStatus.summary.total"))
//      .select("totalNumber")
//      .collect()
//    assert(result.head.getInt(0) === 1)
//  }
//
//  test("Translating all documents under folder in a container applying glossaries") {
//    val result = documentTranslator1
//      .transform(docTranslationDf3)
//      .withColumn("totalNumber", col("translationStatus.summary.total"))
//      .select("totalNumber")
//      .collect()
//    assert(result.head.getInt(0) === 1)
//  }
//
//  test("Translating specific document in a container") {
//    val result = documentTranslator2
//      .transform(docTranslationDf4)
//      .withColumn("totalNumber", col("translationStatus.summary.total"))
//      .select("totalNumber")
//      .collect()
//    assert(result.head.getInt(0) === 2)
//  }
//
//  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
//    def prep(df: DataFrame) = {
//      df.select("translationStatus.summary.total")
//    }
//
//    super.assertDFEq(prep(df1), prep(df2))(eq)
//  }
//
//  override def testObjects(): Seq[TestObject[DocumentTranslator]] =
//    Seq(new TestObject(documentTranslator1, docTranslationDf))
//
//  override def reader: MLReadable[_] = DocumentTranslator
//}
