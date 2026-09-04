// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.translate

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.http.client.methods.{HttpGet, HttpPost, HttpRequestBase}
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StructType
import spray.json._

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

private[translate] class TestableLanguages extends Languages {
  def buildRequest(schema: StructType, row: Row): Option[HttpGet] =
    inputFunc(schema)(row).map(_.asInstanceOf[HttpGet])
}

class TextTranslatorCoreSuite extends TestBase {

  import spark.implicits._

  private def toQueryMap(request: HttpRequestBase): Map[String, String] = {
    Option(request.getURI.getRawQuery).toSeq.flatMap(_.split("&")).map { kv =>
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
    assert(t.getApiVersion == "3.0")
    assert(t.getOrDefault(t.textType) == Left("plain"))
    assert(t.getOrDefault(t.category) == Left("general"))
    assert(t.getOrDefault(t.profanityAction) == Left("NoAction"))
    assert(t.getOrDefault(t.profanityMarker) == Left("Asterisk"))
    assertResult(Left(false))(t.getOrDefault(t.includeAlignment))
    assertResult(Left(false))(t.getOrDefault(t.includeSentenceLength))
    assertResult(Left(true))(t.getOrDefault(t.allowFallback))
  }

  test("translator API version validation is deterministic") {
    val error = intercept[IllegalArgumentException] {
      new Translate().setApiVersion("2025-10-01-preview")
    }
    assert(error.getMessage.contains("Supported versions: 2026-06-06, 3.0"))
    assert(!classOf[Translate].getMethods.exists(_.getName == "setApiVersionCol"))
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

  test("translate builds the 2026 request body and response schema") {
    val df = Seq((Seq("hello", "world"), Seq("de", "fr"), "en"))
      .toDF("text", "toLanguage", "fromLanguage")

    val t = new TestableTranslate()
      .setApiVersion("2026-06-06")
      .setSubscriptionKey("fake-key")
      .setLocation("eastus")
      .setTextCol("text")
      .setToLanguageCol("toLanguage")
      .setFromLanguageCol("fromLanguage")

    val request = t.buildRequest(df.schema, df.head()).get
    assert(toQueryMap(request) == Map("api-version" -> "2026-06-06"))
    val body = EntityUtils.toString(request.getEntity, "UTF-8").parseJson.asJsObject
    val inputs = body.fields("inputs").asInstanceOf[JsArray].elements
    assert(inputs.map(_.asJsObject.fields("text")) == Seq(JsString("hello"), JsString("world")))
    inputs.foreach { input =>
      assert(input.asJsObject.fields("language") == JsString("en"))
      assert(input.asJsObject.fields("targets").asInstanceOf[JsArray].elements ==
        Seq(JsObject("language" -> JsString("de")), JsObject("language" -> JsString("fr"))))
    }
    assert(t.responseDataType == TranslateResponseV2026.schema)
  }

  test("translate filters invalid 2026 text and target array entries") {
    val df = Seq((Seq("hello", null, " "), Seq("de", null, " "))) //scalastyle:ignore null
      .toDF("text", "toLanguage")
    val t = new TestableTranslate()
      .setApiVersion("2026-06-06")
      .setLocation("eastus")
      .setTextCol("text")
      .setToLanguageCol("toLanguage")

    val request = t.buildRequest(df.schema, df.head()).get
    val inputs = EntityUtils.toString(request.getEntity, "UTF-8")
      .parseJson.asJsObject.fields("inputs").asInstanceOf[JsArray].elements
    assert(inputs.map(_.asJsObject.fields("text")) == Seq(JsString("hello")))
    assert(inputs.head.asJsObject.fields("targets").asInstanceOf[JsArray].elements ==
      Seq(JsObject("language" -> JsString("de"))))

    val blankTargets = Seq((Seq("hello"), Seq(null, " "))) //scalastyle:ignore null
      .toDF("text", "toLanguage")
    val error = intercept[IllegalArgumentException] {
      t.buildRequest(blankTargets.schema, blankTargets.head())
    }
    assert(error.getMessage.contains("at least one non-blank target language"))
  }

  test("translate maps compatible 2026 controls and rejects removed controls") {
    val request = new TestableTranslate()
      .setApiVersion("2026-06-06")
      .setLocation("eastus")
      .setText("hello")
      .setToLanguage("es")
      .setFromLanguage("en")
      .setFromScript("Latn")
      .setToScript("Latn")
      .setTextType("html")
      .setCategory("custom-model")
      .setAllowFallback(false)
      .setProfanityAction("Marked")
      .setProfanityMarker("Tag")
      .buildRequest(StructType(Seq.empty), Row.empty)
      .get
    val body = EntityUtils.toString(request.getEntity, "UTF-8")
    assert(body.contains(""""deploymentName":"custom-model""""))
    assert(body.contains(""""allowFallback":false"""))
    assert(body.contains(""""profanityAction":"Marked""""))
    assert(body.contains(""""profanityMarker":"Tag""""))
    assert(body.contains(""""textType":"Html""""))

    val neutralRequest = new TestableTranslate()
      .setApiVersion("2026-06-06")
      .setLocation("eastus")
      .setText("hello")
      .setToLanguage("es")
      .setIncludeAlignment(false)
      .setIncludeSentenceLength(false)
      .setSuggestedFrom(" ")
      .buildRequest(StructType(Seq.empty), Row.empty)
    assert(neutralRequest.nonEmpty)

    val error = intercept[IllegalArgumentException] {
      new TestableTranslate()
        .setApiVersion("2026-06-06")
        .setLocation("eastus")
        .setText("hello")
        .setToLanguage("es")
        .setIncludeAlignment(true)
        .buildRequest(StructType(Seq.empty), Row.empty)
    }
    assert(error.getMessage.contains("only supported by Translator API 3.0"))

    val invalidTextType = Seq((Seq("hello"), Seq("es"), "markdown"))
      .toDF("text", "toLanguage", "textType")
    val textTypeError = intercept[IllegalArgumentException] {
      new TestableTranslate()
        .setApiVersion("2026-06-06")
        .setLocation("eastus")
        .setTextCol("text")
        .setToLanguageCol("toLanguage")
        .setTextTypeCol("textType")
        .buildRequest(invalidTextType.schema, invalidTextType.head())
    }
    assert(textTypeError.getMessage.contains("Invalid textType 'markdown'"))
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

  test("transliterate builds the wrapped 2026 request and response schema") {
    val df = Seq((Seq("пример текста"), "ru", "Cyrl", "Latn"))
      .toDF("text", "language", "fromScript", "toScript")
    val t = new TestableTransliterate()
      .setApiVersion("2026-06-06")
      .setLocation("eastus")
      .setTextCol("text")
      .setLanguageCol("language")
      .setFromScriptCol("fromScript")
      .setToScriptCol("toScript")

    val request = t.buildRequest(df.schema, df.head()).get
    val query = toQueryMap(request)
    assert(query("api-version") == "2026-06-06")
    assert(query("language") == "ru")
    assert(query("fromScript") == "Cyrl")
    assert(query("toScript") == "Latn")
    assert(EntityUtils.toString(request.getEntity, "UTF-8") ==
      """{"inputs":[{"text":"пример текста"}]}""")
    assert(t.responseDataType == TransliterateResponseV2026.schema)
  }

  test("text-only request bodies ignore null entries") {
    val df = Seq(Seq("hello", null)) //scalastyle:ignore null
      .toDF("text")

    val v3Request = new TestableDetect()
      .setLocation("eastus")
      .setTextCol("text")
      .buildRequest(df.schema, df.head())
      .get
    assert(EntityUtils.toString(v3Request.getEntity, "UTF-8") == """[{"Text":"hello"}]""")

    val v2026Request = new TestableTransliterate()
      .setApiVersion("2026-06-06")
      .setLocation("eastus")
      .setTextCol("text")
      .setLanguage("en")
      .setFromScript("Latn")
      .setToScript("Latn")
      .buildRequest(df.schema, df.head())
      .get
    assert(EntityUtils.toString(v2026Request.getEntity, "UTF-8") ==
      """{"inputs":[{"text":"hello"}]}""")
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

  test("v3-only operations reject Translator API 2026-06-06") {
    val textOnly = Seq("hello").toDF("text")
    val removed = Seq(
      new Detect().setTextCol("text"),
      new BreakSentence().setTextCol("text"),
      new DictionaryLookup().setTextCol("text").setFromLanguage("en").setToLanguage("es"))

    removed.foreach { transformer =>
      transformer.setApiVersion("2026-06-06")
      val error = intercept[IllegalArgumentException] {
        transformer.transformSchema(textOnly.schema)
      }
      assert(error.getMessage.contains("is not available in Translator API 2026-06-06"))
    }

    val examples = new DictionaryExamples()
      .setApiVersion("2026-06-06")
      .setTextAndTranslation(TextAndTranslation("fly", "volar"))
      .setFromLanguage("en")
      .setToLanguage("es")
    val error = intercept[IllegalArgumentException] {
      examples.transformSchema(StructType(Seq.empty))
    }
    assert(error.getMessage.contains("is not available in Translator API 2026-06-06"))
  }

  test("languages supports v3 and 2026 requests") {
    val v3 = new TestableLanguages()
      .setLocation("eastus")
      .setScope(Seq("translation", "dictionary"))
      .buildRequest(StructType(Seq.empty), Row.empty)
      .get
    assert(v3.getURI.getPath.endsWith("/languages"))
    assert(toQueryMap(v3) == Map(
      "api-version" -> "3.0",
      "scope" -> "translation,dictionary"))

    val latest = new TestableLanguages()
      .setApiVersion("2026-06-06")
      .setLocation("eastus")
      .setScope("models")
      .buildRequest(StructType(Seq.empty), Row.empty)
      .get
    assert(toQueryMap(latest) == Map("api-version" -> "2026-06-06", "scope" -> "models"))
    assert(latest.getFirstHeader("Ocp-Apim-Subscription-Region").getValue == "eastus")
    assert(new Languages().responseDataType == TranslatorLanguagesResponse.schema)
    assert(TranslatorLanguagesResponse.schema("models").dataType == ArrayType(org.apache.spark.sql.types.StringType))

    val v3Error = intercept[IllegalArgumentException] {
      new Languages().setScope("models").transformSchema(StructType(Seq.empty))
    }
    assert(v3Error.getMessage.contains("dictionary, translation, transliteration"))
    val latestError = intercept[IllegalArgumentException] {
      new Languages()
        .setApiVersion("2026-06-06")
        .setScope("dictionary")
        .transformSchema(StructType(Seq.empty))
    }
    assert(latestError.getMessage.contains("models, translation, transliteration"))
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
