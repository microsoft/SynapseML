// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.translate

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.http.client.methods.HttpPost
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StructType

import java.net.URLDecoder

private[translate] class TestableTranslate extends Translate {
  def buildRequest(schema: StructType, row: Row): Option[HttpPost] =
    inputFunc(schema)(row).map(_.asInstanceOf[HttpPost])
}

private[translate] class TestableTransliterate extends Transliterate {
  def buildRequest(schema: StructType, row: Row): Option[HttpPost] =
    inputFunc(schema)(row).map(_.asInstanceOf[HttpPost])
}

private[translate] class TestableDetect extends Detect {
  def buildRequest(schema: StructType, row: Row): Option[HttpPost] =
    inputFunc(schema)(row).map(_.asInstanceOf[HttpPost])
}

private[translate] class TestableBreakSentence extends BreakSentence {
  def buildRequest(schema: StructType, row: Row): Option[HttpPost] =
    inputFunc(schema)(row).map(_.asInstanceOf[HttpPost])
}

private[translate] class TestableDictionaryLookup extends DictionaryLookup {
  def buildRequest(schema: StructType, row: Row): Option[HttpPost] =
    inputFunc(schema)(row).map(_.asInstanceOf[HttpPost])
}

private[translate] class TestableDictionaryExamples extends DictionaryExamples {
  def buildRequest(schema: StructType, row: Row): Option[HttpPost] =
    inputFunc(schema)(row).map(_.asInstanceOf[HttpPost])
}

class TextTranslatorCoreSuite extends TestBase {

  import spark.implicits._

  private def toQueryMap(post: HttpPost): Map[String, String] = {
    Option(post.getURI.getRawQuery).toSeq.flatMap(_.split("&")).map { kv =>
      val pair = kv.split("=", 2)
      val key = URLDecoder.decode(pair(0), "UTF-8")
      val value = if (pair.length > 1) URLDecoder.decode(pair(1), "UTF-8") else ""
      key -> value
    }.toMap
  }

  test("setLocation sets translator endpoint and subscription region") {
    val global = new Translate().setLocation("eastus")
    assert(global.getSubscriptionRegion == "eastus")
    assert(global.getUrl == "https://api.cognitive.microsofttranslator.com/translate")

    val usGov = new Translate().setLocation("usgovarizona")
    assert(usGov.getUrl == "https://api.cognitive.microsofttranslator.us/translate")

    val china = new Translate().setLocation("chinanorth")
    assert(china.getUrl == "https://api.cognitive.microsofttranslator.cn/translate")
  }

  test("translate defaults are deterministic") {
    val t = new Translate()
    assert(t.getOrDefault(t.textType) == Left("plain"))
    assert(t.getOrDefault(t.category) == Left("general"))
    assert(t.getOrDefault(t.profanityAction) == Left("NoAction"))
    assert(t.getOrDefault(t.profanityMarker) == Left("Asterisk"))
    assertResult(Left(false))(t.getOrDefault(t.includeAlignment))
    assertResult(Left(false))(t.getOrDefault(t.includeSentenceLength))
    assertResult(Left(true))(t.getOrDefault(t.allowFallback))
  }

  test("translate rejects invalid enum parameters") {
    intercept[IllegalArgumentException] {
      new Translate().setTextType("markdown")
    }
    intercept[IllegalArgumentException] {
      new Translate().setProfanityAction("Mask")
    }
    intercept[IllegalArgumentException] {
      new Translate().setProfanityMarker("Bracket")
    }
  }

  test("translate request building maps query params and body deterministically") {
    val df = Seq((Seq("hello", "world"), Seq("de", "fr"), "en"))
      .toDF("text", "toLanguage", "fromLanguage")

    val t = new TestableTranslate()
      .setSubscriptionKey("fake-key")
      .setLocation("eastus")
      .setTextCol("text")
      .setToLanguageCol("toLanguage")
      .setFromLanguageCol("fromLanguage")

    val request = t.buildRequest(df.schema, df.head()).get
    val query = toQueryMap(request)
    assert(query("api-version") == "3.0")
    assert(query("from") == "en")
    assert(query("to") == "de,fr")
    assert(query("textType") == "plain")
    assert(query("category") == "general")
    assert(query("profanityAction") == "NoAction")
    assert(query("profanityMarker") == "Asterisk")
    assert(query("includeAlignment") == "false")
    assert(query("includeSentenceLength") == "false")
    assert(query("allowFallback") == "true")
    assert(request.getFirstHeader("Ocp-Apim-Subscription-Key").getValue == "fake-key")
    assert(request.getFirstHeader("Ocp-Apim-Subscription-Region").getValue == "eastus")
    assert(request.getFirstHeader("Content-Type").getValue == "application/json; charset=UTF-8")
    assert(EntityUtils.toString(request.getEntity, "UTF-8") == """[{"Text":"hello"},{"Text":"world"}]""")
  }

  test("translate request building skips empty or missing text and targets") {
    val t = new TestableTranslate()
      .setLocation("eastus")
      .setTextCol("text")
      .setToLanguageCol("toLanguage")

    val emptyTextDf = Seq((Seq.empty[String], Seq("de"))).toDF("text", "toLanguage")
    assert(t.buildRequest(emptyTextDf.schema, emptyTextDf.head()).isEmpty)

    val emptyToDf = Seq((Seq("hello"), Seq.empty[String])).toDF("text", "toLanguage")
    assert(t.buildRequest(emptyToDf.schema, emptyToDf.head()).isEmpty)

    val nullToDf = Seq((Seq("hello"), Option.empty[Seq[String]])).toDF("text", "toLanguage")
    assert(t.buildRequest(nullToDf.schema, nullToDf.head()).isEmpty)
  }

  test("translate transformSchema adds output and error columns without temp columns") {
    val input = Seq(("hello", "de")).toDF("text", "toLanguage")
    val t = new Translate()
      .setTextCol("text")
      .setToLanguageCol("toLanguage")
      .setOutputCol("translation")
      .setErrorCol("translationError")

    val schema = t.transformSchema(input.schema)
    assert(schema.fieldNames.toSet == Set("text", "toLanguage", "translation", "translationError"))
    assert(schema("translation").dataType == ArrayType(TranslateResponse.schema))
  }

  test("translate validates required parameters during schema creation") {
    val textOnly = Seq("hello").toDF("text")
    val err = intercept[AssertionError] {
      new Translate().setTextCol("text").transformSchema(textOnly.schema)
    }
    assert(err.getMessage.contains("Missing required params"))
    assert(err.getMessage.contains("toLanguage"))
  }

  test("transliterate request building maps required params and body deterministically") {
    val df = Seq((Seq("こんにちは"), "ja", "Jpan", "Latn")).toDF("text", "language", "fromScript", "toScript")

    val t = new TestableTransliterate()
      .setSubscriptionKey("fake-key")
      .setLocation("eastus")
      .setTextCol("text")
      .setLanguageCol("language")
      .setFromScriptCol("fromScript")
      .setToScriptCol("toScript")

    val request = t.buildRequest(df.schema, df.head()).get
    val query = toQueryMap(request)
    assert(request.getURI.getPath.endsWith("/transliterate"))
    assert(query("api-version") == "3.0")
    assert(query("language") == "ja")
    assert(query("fromScript") == "Jpan")
    assert(query("toScript") == "Latn")
    assert(request.getFirstHeader("Ocp-Apim-Subscription-Key").getValue == "fake-key")
    assert(request.getFirstHeader("Ocp-Apim-Subscription-Region").getValue == "eastus")
    assert(EntityUtils.toString(request.getEntity, "UTF-8") == """[{"Text":"こんにちは"}]""")
  }

  test("detect and breaksentence request building is deterministic offline") {
    val detectDf = Seq(Seq("hello", "world")).toDF("text")
    val detectRequest = new TestableDetect()
      .setLocation("eastus")
      .setTextCol("text")
      .buildRequest(detectDf.schema, detectDf.head())
      .get
    assert(detectRequest.getURI.getPath.endsWith("/detect"))
    assert(toQueryMap(detectRequest) == Map("api-version" -> "3.0"))
    assert(EntityUtils.toString(detectRequest.getEntity, "UTF-8") == """[{"Text":"hello"},{"Text":"world"}]""")

    val breakDf = Seq((Seq("hello"), "en", "Latn")).toDF("text", "language", "script")
    val breakRequest = new TestableBreakSentence()
      .setLocation("eastus")
      .setTextCol("text")
      .setLanguageCol("language")
      .setScriptCol("script")
      .buildRequest(breakDf.schema, breakDf.head())
      .get
    val breakQuery = toQueryMap(breakRequest)
    assert(breakRequest.getURI.getPath.endsWith("/breaksentence"))
    assert(breakQuery("api-version") == "3.0")
    assert(breakQuery("language") == "en")
    assert(breakQuery("script") == "Latn")
    assert(EntityUtils.toString(breakRequest.getEntity, "UTF-8") == """[{"Text":"hello"}]""")
  }

  test("dictionary lookup and examples request building maps query params and body") {
    val lookupDf = Seq((Seq("fly"), "en", "es")).toDF("text", "fromLanguage", "toLanguage")
    val lookupRequest = new TestableDictionaryLookup()
      .setSubscriptionKey("fake-key")
      .setLocation("eastus")
      .setTextCol("text")
      .setFromLanguageCol("fromLanguage")
      .setToLanguageCol("toLanguage")
      .buildRequest(lookupDf.schema, lookupDf.head())
      .get
    val lookupQuery = toQueryMap(lookupRequest)
    assert(lookupRequest.getURI.getPath.endsWith("/dictionary/lookup"))
    assert(lookupQuery("api-version") == "3.0")
    assert(lookupQuery("from") == "en")
    assert(lookupQuery("to") == "es")
    assert(EntityUtils.toString(lookupRequest.getEntity, "UTF-8") == """[{"Text":"fly"}]""")

    val examplesDf = Seq((Seq(TextAndTranslation("fly", "volar")), "en", "es"))
      .toDF("textAndTranslation", "fromLanguage", "toLanguage")
    val examplesRequest = new TestableDictionaryExamples()
      .setLocation("eastus")
      .setTextAndTranslationCol("textAndTranslation")
      .setFromLanguageCol("fromLanguage")
      .setToLanguageCol("toLanguage")
      .buildRequest(examplesDf.schema, examplesDf.head())
      .get
    val examplesQuery = toQueryMap(examplesRequest)
    assert(examplesRequest.getURI.getPath.endsWith("/dictionary/examples"))
    assert(examplesQuery("api-version") == "3.0")
    assert(examplesQuery("from") == "en")
    assert(examplesQuery("to") == "es")
    assert(EntityUtils.toString(examplesRequest.getEntity, "UTF-8") == """[{"Text":"fly","Translation":"volar"}]""")
  }

  test("dictionary examples request building supports scalar text and translation input") {
    val request = new TestableDictionaryExamples()
      .setLocation("eastus")
      .setFromLanguage("en")
      .setToLanguage("es")
      .setTextAndTranslation(TextAndTranslation("fly", "volar"))
      .buildRequest(StructType(Seq.empty), Row.empty)
      .get
    val query = toQueryMap(request)
    assert(request.getURI.getPath.endsWith("/dictionary/examples"))
    assert(query("api-version") == "3.0")
    assert(query("from") == "en")
    assert(query("to") == "es")
    assert(EntityUtils.toString(request.getEntity, "UTF-8") == """[{"Text":"fly","Translation":"volar"}]""")
  }

  test("non-translate transformSchema adds deterministic output and error columns") {
    val textOnly = Seq("hello").toDF("text")
    val textAndTranslationOnly = Seq(Seq(TextAndTranslation("fly", "volar"))).toDF("textAndTranslation")

    val transliterateSchema = new Transliterate()
      .setTextCol("text")
      .setLanguage("ja")
      .setFromScript("Jpan")
      .setToScript("Latn")
      .setOutputCol("transliteration")
      .setErrorCol("transliterationError")
      .transformSchema(textOnly.schema)
    assert(transliterateSchema.fieldNames.toSet == Set("text", "transliteration", "transliterationError"))
    assert(transliterateSchema("transliteration").dataType == ArrayType(TransliterateResponse.schema))

    val detectSchema = new Detect()
      .setTextCol("text")
      .setOutputCol("detection")
      .setErrorCol("detectionError")
      .transformSchema(textOnly.schema)
    assert(detectSchema.fieldNames.toSet == Set("text", "detection", "detectionError"))
    assert(detectSchema("detection").dataType == ArrayType(DetectResponse.schema))

    val breakSentenceSchema = new BreakSentence()
      .setTextCol("text")
      .setOutputCol("sentenceBreaks")
      .setErrorCol("breakError")
      .transformSchema(textOnly.schema)
    assert(breakSentenceSchema.fieldNames.toSet == Set("text", "sentenceBreaks", "breakError"))
    assert(breakSentenceSchema("sentenceBreaks").dataType == ArrayType(BreakSentenceResponse.schema))

    val lookupSchema = new DictionaryLookup()
      .setTextCol("text")
      .setFromLanguage("en")
      .setToLanguage("es")
      .setOutputCol("lookup")
      .setErrorCol("lookupError")
      .transformSchema(textOnly.schema)
    assert(lookupSchema.fieldNames.toSet == Set("text", "lookup", "lookupError"))
    assert(lookupSchema("lookup").dataType == ArrayType(DictionaryLookupResponse.schema))

    val examplesSchema = new DictionaryExamples()
      .setTextAndTranslationCol("textAndTranslation")
      .setFromLanguage("en")
      .setToLanguage("es")
      .setOutputCol("examples")
      .setErrorCol("examplesError")
      .transformSchema(textAndTranslationOnly.schema)
    assert(examplesSchema.fieldNames.toSet == Set("textAndTranslation", "examples", "examplesError"))
    assert(examplesSchema("examples").dataType == ArrayType(DictionaryExamplesResponse.schema))
  }

  test("non-translate classes validate required parameters during schema creation") {
    val textOnly = Seq("hello").toDF("text")
    val textAndTranslationOnly = Seq(Seq(TextAndTranslation("fly", "volar"))).toDF("textAndTranslation")

    val transliterateError = intercept[AssertionError] {
      new Transliterate().setTextCol("text").transformSchema(textOnly.schema)
    }
    assert(transliterateError.getMessage.contains("Missing required params"))
    assert(transliterateError.getMessage.contains("language"))
    assert(transliterateError.getMessage.contains("fromScript"))
    assert(transliterateError.getMessage.contains("toScript"))

    val dictionaryLookupError = intercept[AssertionError] {
      new DictionaryLookup().setTextCol("text").transformSchema(textOnly.schema)
    }
    assert(dictionaryLookupError.getMessage.contains("Missing required params"))
    assert(dictionaryLookupError.getMessage.contains("fromLanguage"))
    assert(dictionaryLookupError.getMessage.contains("toLanguage"))

    val dictionaryExamplesError = intercept[AssertionError] {
      new DictionaryExamples()
        .setTextAndTranslationCol("textAndTranslation")
        .transformSchema(textAndTranslationOnly.schema)
    }
    assert(dictionaryExamplesError.getMessage.contains("Missing required params"))
    assert(dictionaryExamplesError.getMessage.contains("fromLanguage"))
    assert(dictionaryExamplesError.getMessage.contains("toLanguage"))
  }
}
