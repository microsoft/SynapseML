// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.services.openai.OpenAIDecisions
import org.apache.spark.sql.Row
import spray.json.{JsArray, JsNumber, JsObject, JsString, JsValue, enrichString}

import java.util.Base64
import scala.util.Try
import scala.util.Using

class NativeQuestionsSuite extends TestBase {
  // Public request examples, retrieved 2026-10-13:
  // https://learn.microsoft.com/en-us/azure/foundry/foundry-models/how-to/use-foundry-models-microsoft-decision
  private val systemOne =
    """{
      |"team":{"type":"choice","instructions":"Which team should handle this request?",
      |  "criteria":{"billing":"Charges, invoices, and refunds","engineering":"Bugs, errors, and outages",
      |    "support":"How-to and account questions"}},
      |"severity":{"type":"score","instructions":"How severe is this issue?",
      |  "criteria":["Cosmetic","Minor","Major","Critical"]},
      |"repeat_contact":{"type":"noul","instructions":"Has the customer contacted support before?"}
      |}""".stripMargin.parseJson

  // https://developers.openai.com/api/docs/guides/decisions
  // https://developers.openai.com/api/reference/python/resources/decisions/methods/create
  private val decisions =
    """[
      |{"type":"predicate","name":"visible_damage","instructions":"Does the product have visible damage?"},
      |{"type":"choice","name":"department","instructions":"Which department should handle this complaint?",
      | "choices":[{"value":"billing","description":"Payments, invoices, and refunds."},
      |   {"value":"technical","description":"Problems using the product."}]},
      |{"type":"score","name":"severity","instructions":"How severe is this issue?",
      | "levels":[{"label":"Cosmetic","description":"Appearance only; no lost functionality."},
      |   {"label":"Workaround available","description":"A task fails, but another way works."},
      |   {"label":"Fully blocked","description":"A task fails with no workaround."}]}
      |]""".stripMargin.parseJson

  private def validate(api: String, text: String): Unit = NativeQuestions.validate(api, text.parseJson)

  private def invalid(api: String, text: String): Unit = {
    intercept[IllegalArgumentException](validate(api, text))
  }

  test("System One accepts the public combined question and optional noul criteria examples") {
    NativeQuestions.validate("systemone", systemOne)
    validate("systemone",
      """{"refund":{"type":"noul","instructions":"Does this reply promise the customer a refund?",
        |"criteria":{"true":"Commits to returning money","false":"Makes no commitment about money"}}}""".stripMargin)
  }

  test("System One enforces native named questions instead of a guessed common API shape") {
    Seq(
      "[]", "{}",
      """{"q":{"type":"predicate","instructions":"Test"}}""",
      """{"q":{"type":"noul"}}""",
      """{"q":{"type":"noul","instructions":true}}""",
      """{"q":{"type":"noul","instructions":"Test","criteria":{"yes":"Yes","no":"No"}}}""",
      """{"q":{"type":"choice","instructions":"Test","choices":["one","two"]}}""",
      """{"q":{"type":"choice","instructions":"Test","criteria":{"one":1,"two":"two"}}}""",
      """{"q":{"type":"choice","instructions":"Test","criteria":{"one":"Only one"}}}""",
      """{"q":{"type":"score","instructions":"Test","criteria":[{"label":"first"}]}}""",
      """{"q":{"type":"score","instructions":"Test","criteria":["first",true]}}"""
    ).foreach(invalid("systemone", _))
  }

  test("Decisions accepts the public ordered questions and typed distinct choice values") {
    NativeQuestions.validate("decisions", decisions)
    validate("decisions",
      """[{"type":"choice","instructions":"Pick one","choices":[{"value":true},{"value":"true"}]},
        |{"type":"predicate","name":null,"instructions":"Test"}]""".stripMargin)
  }

  test("Decisions rejects malformed types, duplicate names and ambiguous choice values") {
    Seq(
      "{}", "[]",
      """[{"type":"noul","instructions":"Test"}]""",
      """[{"type":"predicate","instructions":"Test","name":true}]""",
      """[{"type":"predicate","name":"same","instructions":"A"},
        |{"type":"predicate","name":"same","instructions":"B"}]""".stripMargin,
      """[{"type":"choice","instructions":"Pick","choices":[{"value":1},{"value":2}]}]""",
      """[{"type":"choice","instructions":"Pick","choices":[{"value":true},{"value":true}]}]""",
      """[{"type":"choice","instructions":"Pick","choices":[{"value":"a","description":42},{"value":"b"}]}]""",
      """[{"type":"score","instructions":"Rate","levels":["Low","High"]}]""",
      """[{"type":"score","instructions":"Rate","levels":[{"label":"first"},{"description":"No label"}]}]"""
    ).foreach(invalid("decisions", _))
    val choices = (0 until 256).map(i => JsObject("value" -> JsString(i.toString)))
    val tooMany = JsArray(JsObject("type" -> JsString("choice"), "instructions" -> JsString("Pick"),
      "choices" -> JsArray(choices.toVector)))
    intercept[IllegalArgumentException](NativeQuestions.validate("decisions", tooMany))
  }

  private def input(parts: JsValue*): JsValue = JsArray(JsObject(
    "role" -> JsString("user"), "content" -> JsArray(parts.toVector)))

  private def image(url: String): JsValue = JsObject("type" -> JsString("input_image"), "image_url" -> JsString(url))

  test("direct and facade Decisions inputs distinguish missing, blank and unchanged nonblank text") {
    import spark.implicits._
    Seq("text", "json").foreach { mode =>
      Seq(new AIPrompt().setApiType("decisions"), new OpenAIDecisions()).foreach { stage =>
        Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
          val nonblank = "  evidence\t"
          val values = Seq(Option.empty[String].orNull, "", " \t\n", nonblank)
          val data = values.zipWithIndex.map { case (text, id) =>
            (id, if (mode == "json" && text != null && text.nonEmpty) JsString(text).compactPrint else text)
          }.toDF("id", "text")
          stage.setUrl(server.url + "/v1").setModel("model").setInputCol("text").setInputMode(mode)
            .setQuestions(NativeRequestFixtures.DecisionQuestions).setErrorCol("error")
          val result = stage.transform(data)
          assert(result.schema == stage.transformSchema(data.schema))
          val rows = result.collect().sortBy(_.getAs[Int]("id"))
          rows.take(2).foreach { row =>
            val error = AIJson.objectString(row.getAs[Row]("error").getString(0))
            assert(error.fields("code") == JsString("missing_input"))
            assert(error.fields("message") == JsString("input is null or empty"))
          }
          assert(rows(2).getAs[Row]("error").getString(0).contains("invalid_request"))
          assert(rows(3).getAs[Row]("error") == null && server.requests.size() == 1)
          assert(AIJson.objectString(server.requests.peek()._2).fields("input") == JsString(nonblank))
        }
      }
    }
  }

  test("Decisions accepts text and user-message content with inline image data") {
    NativeQuestions.validateDecisionsInput(JsString("Evidence"))
    NativeQuestions.validateDecisionsInput("""[{"role":"user","type":"message","content":"Evidence"}]""".parseJson)
    NativeQuestions.validateDecisionsInput(input(
      JsObject("type" -> JsString("input_text"), "text" -> JsString("Inspect the product.")),
      image("data:image/png;base64,AQID")))
  }

  test("Decisions inline image syntax matches the basic base64 decoder without exposing content") {
    val samples = Seq("", "A", "AQ", "AQI", "AQID", "AQ==", "AQI=", "AR==", "AQJ=",
      "AQ=", "AQI==", "AQID=", "====", "=AQI", "AQ I", "AQ\nI", "AQ-I", "AQ_I", "private@image")
    samples.foreach { encoded =>
      val expected = Try(Base64.getDecoder.decode(encoded)).toOption.exists(_.nonEmpty)
      val actual = Try(NativeQuestions.validateDecisionsInput(input(image(s"data:image/png;base64,$encoded"))))
      assert(actual.isSuccess == expected, s"Base64 syntax acceptance differed for ${encoded.length} characters")
      actual.failed.foreach { error =>
        assert(error.isInstanceOf[IllegalArgumentException] && error.getCause == null)
        assert(!error.getMessage.contains("private@image"))
      }
    }
  }

  test("Decisions rejects hosted images, invalid base64, unsupported roles and modalities") {
    val unsupported = Seq(
      input(image("https://example.com/image.png")),
      input(image("data:image/png;base64,not-base64")),
      input(image("data:image/png;base64,")),
      input(JsObject("type" -> JsString("input_image"), "file_id" -> JsString("file-example"))),
      input(JsObject(image("data:image/png;base64,AQID").asJsObject.fields +
        ("file_id" -> JsString("file-example")))),
      input(JsObject(image("data:image/png;base64,AQID").asJsObject.fields + ("detail" -> JsNumber(42)))),
      input(JsObject("type" -> JsString("input_audio"))),
      """[{"role":"system","content":"Evidence"}]""".parseJson,
      """[{"role":"user","content":null}]""".parseJson,
      """[{"role":"user","content":[]}]""".parseJson,
      """[{"role":"user","type":"function_call","content":"Evidence"}]""".parseJson,
      """[{"role":"user","content":[{"type":"input_text","text":42}]}]""".parseJson
    )
    unsupported.foreach(v => intercept[IllegalArgumentException](NativeQuestions.validateDecisionsInput(v)))
    val inline = image("data:image/png;base64,AQID")
    NativeQuestions.validateDecisionsInput(input(Seq.fill(128)(inline): _*))
    intercept[IllegalArgumentException] {
      NativeQuestions.validateDecisionsInput(input(Seq.fill(129)(inline): _*))
    }
  }

  test("Native validation errors do not disclose supplied values") {
    val error = intercept[IllegalArgumentException] {
      validate("decisions", """[{"type":"private-sensitive-value","instructions":"Secret"}]""")
    }
    assert(!error.getMessage.contains("private-sensitive-value"))
    assert(!error.getMessage.contains("Secret"))
  }
}
