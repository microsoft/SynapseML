// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.param.GlobalParams
import com.microsoft.azure.synapse.ml.services.OpenAIApiVersion
import com.microsoft.azure.synapse.ml.services.openai.OpenAIDecisions
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.lit
import spray.json.{JsNumber, JsObject, JsString}

import scala.util.Using

class AIPromptPersistenceSuite extends AIPromptTestBase {
  test("public requests preserve rendered JSON number kinds") {
    val reply = """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""
    Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
      val data = spark.range(1).select(lit("hello").as("text"))
      val cases: Seq[(Either[String, Double], String)] =
        Seq(Left("1e0") -> "1", Left("3.2e1") -> "32", Right(12345678.0) -> "12345678")
      cases.foreach { case (value, rendered) =>
        val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model")
          .setInputCol("text").setOutputCol("answer").setErrorCol("error")
        value.fold(text => stage.setExtraBody(s"""{"x":$text}"""), number => stage.setExtraBody(Map("x" -> number)))
        assert(stage.transform(data).head().getAs[String]("answer") == "answer")
        assert(AIJson.objectString(server.requests.poll()._2).fields("x").compactPrint == rendered)
      }
      assert(server.requests.isEmpty)
    }
  }

  test("numeric JSON setters reject undecodable rendered exponents without changing parameters") {
    val cases = Seq("100e2147483647" -> false, "-100e2147483647" -> false, "100e2147483646" -> false,
      "100e2147483645" -> true, "-100e2147483645" -> true, "0e2147483647" -> true, "1e-2147483647" -> true)
    cases.foreach { case (token, supported) =>
      val value = BigDecimal(token)
      val typed = JsObject("x" -> JsNumber(value))
      val text = s"""{"x":$token}"""
      val stage = new AIPrompt().setExtraBody("""{"safe":1}""").setQuestions("""{"safe":1}""")
        .setResponseSchema(Map("type" -> "object"))
      val original = stage.extractParamMap().toSeq
      val setters: Seq[() => AIPrompt] = Seq(
        () => stage.setExtraBody(text), () => stage.setExtraBody(typed), () => stage.setExtraBody(Map("x" -> value)),
        () => stage.setQuestions(text), () => stage.setQuestions(typed), () => stage.setQuestions(Map("x" -> value)),
        () => stage.setResponseSchema(Map("type" -> "object", "minimum" -> value)))
      setters.foreach { set =>
        if (supported) set() else {
          intercept[IllegalArgumentException](set())
          assert(stage.extractParamMap().toSeq.toSet == original.toSet)
        }
      }
      if (supported) {
        assert(AIJson.parse(AIJson.parse(token).compactPrint) == JsNumber(value))
        assert(stage.extraBody.jsonDecode(stage.extraBody.jsonEncode(Left(stage.getExtraBody))) ==
          Left(stage.getExtraBody))
        assert(stage.questions.jsonDecode(stage.questions.jsonEncode(Left(stage.getQuestions))) ==
          Left(stage.getQuestions))
        assert(stage.responseFormat.jsonDecode(stage.responseFormat.jsonEncode(Left(stage.getResponseFormat))) ==
          Left(stage.getResponseFormat))
      } else intercept[IllegalArgumentException](AIJson.parse(token))
    }
  }

  test("typed readers do not initialize incompatible metadata-selected classes") {
    import spark.implicits._
    val initialized = "synapseml.aiprompt.test.incompatible.initialized"
    val constructed = "synapseml.aiprompt.test.incompatible.constructed"
    val before = Seq(initialized, constructed).map(key => key -> Option(System.getProperty(key)))
    assert(before.forall(_._2.isEmpty))
    try {
      val saved = path()
      new AIPrompt().write.session(spark).save(saved)
      val metadataPath = new Path(saved, "metadata").toString
      val metadata = AIJson.objectString(spark.read.text(metadataPath).head().getString(0))
      val changed = JsObject(metadata.fields.updated("class",
        JsString("synapsemltest.ai.AIIncompatibleStageProbe"))).compactPrint
      Seq(changed).toDF("value").write.mode("overwrite").text(metadataPath)
      val readers = Seq(() => AIPrompt.load(saved), () => AISystemOne.load(saved), () => OpenAIDecisions.load(saved))
      readers.foreach { load =>
        val error = intercept[IllegalArgumentException](load())
        assert(error.getMessage.contains("incompatible") && error.getCause == null)
      }
      assert(System.getProperty(initialized) == null && System.getProperty(constructed) == null)
    } finally {
      before.foreach {
        case (key, Some(value)) => System.setProperty(key, value)
        case (key, None) => System.clearProperty(key)
      }
    }
  }

  test("dated fallback is an effective default while original and v1 native stages remain unmodified") {
    val previous = GlobalParams.getGlobalParam(OpenAIApiVersion)
    try {
      GlobalParams.resetGlobalParam(OpenAIApiVersion)
      val dated = new AIPrompt().setUrl("https://example.invalid").setDeploymentName("model").setInputCol("text")
      val effective = dated.effectiveCopy()
      assert(!dated.isDefined(dated.apiVersion) && !effective.isSet(effective.apiVersion))
      assert(effective.getDefault(effective.apiVersion).contains(Left("2025-04-01-preview")))
      Seq(new AIPrompt().setUrl("https://example.invalid/v1"),
        new AISystemOne().setUrl("https://example.invalid"),
        new OpenAIDecisions().setUrl("https://example.invalid/v1")).foreach { stage =>
        assert(!stage.effectiveCopy().isDefined(stage.apiVersion))
      }
    } finally {
      previous match {
        case Some(value) => GlobalParams.setGlobalParam(OpenAIApiVersion, value)
        case None => GlobalParams.resetGlobalParam(OpenAIApiVersion)
      }
    }
  }

  test("leaf readers reject incompatible saved classes while the facade accepts its subclasses") {
    Seq(new AIPrompt(), new AISystemOne(), new OpenAIDecisions()).foreach { original =>
      val saved = path()
      original.write.session(spark).save(saved)
      assert(AIPrompt.load(saved).getClass == original.getClass)
      if (!original.isInstanceOf[AISystemOne]) {
        val error = intercept[IllegalArgumentException] { AISystemOne.load(saved); () }
        assert(error.getMessage.contains("incompatible") && error.getCause == null)
      }
      if (!original.isInstanceOf[OpenAIDecisions]) {
        val error = intercept[IllegalArgumentException] { OpenAIDecisions.load(saved); () }
        assert(error.getMessage.contains("incompatible") && error.getCause == null)
      }
    }
  }
}
