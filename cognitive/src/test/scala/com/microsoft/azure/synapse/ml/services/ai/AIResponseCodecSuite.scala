// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.services.openai.UsageUtils
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import spray.json.{JsArray, JsNumber, JsObject, JsString, JsValue, enrichString}

import java.nio.charset.StandardCharsets
import java.util.Collections
import scala.collection.JavaConverters._
import scala.util.Using

class AIResponseCodecSuite extends TestBase {
  test("JSON postprocessing honors Spark read syntax without false row errors") {
    import spark.implicits._
    val input = Seq("hello").toDF("text")
    val cases = Seq(
      "{'value':1.0}" -> 1.0,
      """{"value":NaN}""" -> Double.NaN,
      """{"value":Infinity}""" -> Double.PositiveInfinity,
      """{"value":1.0,"value":2.0}""" -> 2.0,
      ("""{"value":3.0,"ignored":""" + "[" * 150 + "0" + "]" * 150 + "}") -> 3.0)
    Seq("chat_completions", "responses").foreach { api =>
      cases.foreach { case (text, expected) =>
        val quoted = JsString(text).compactPrint
        val reply = if (api == "chat_completions") {
          s"""{"choices":[{"index":0,"message":{"role":"assistant","content":$quoted},"finish_reason":"stop"}]}"""
        } else {
          s"""{"status":"completed","output":[{"type":"message","role":"assistant",
             |"content":[{"type":"output_text","text":$quoted}]}]}""".stripMargin
        }
        Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
          val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
            .setInputCol("text").setOutputCol("answer").setErrorCol("error").setRawResponseCol("raw")
            .setPostProcessingOptions(Map("jsonSchema" -> "value DOUBLE"))
          val row = stage.transform(input).head()
          val actual = row.getAs[Row]("answer").getDouble(0)
          assert(actual == expected || (actual.isNaN && expected.isNaN))
          assert(row.getAs[Row]("error") == null, s"$api reported an error for $text")
          assert(row.getAs[String]("raw") == reply && server.requests.size() == 1)
        }
      }
    }
  }

  test("malformed answer projections retain independently validated usage and response identity") {
    Seq("chat_completions", "responses", "embeddings", "systemone", "decisions").foreach { api =>
      val raw = """{"id":"response-with-invalid-answer","model":"returned",
        |"choices":42,"output":42,"data":42,"answers":42,"usage":{"total_tokens":7}}""".stripMargin
      val decoded = AIResponseCodec.decode(api, raw)
      assert(decoded.value == None.orNull)
      assert(decoded.diagnostic.exists(_._1 == "parse_error"))
      assert(decoded.usage.getLong(2) == 7L)
      assert(decoded.responseId == "response-with-invalid-answer")
    }
  }

  test("provider failures outrank malformed projections without weakening successful response checks") {
    Seq("chat_completions", "responses").foreach { api =>
      val key = if (api == "chat_completions") "choices" else "output"
      Seq("null", "42", "[null]", """[{"unexpected":true}]""").foreach { projection =>
        val observations = s""""id":"kept-id","usage":{"total_tokens":7},"$key":$projection"""
        val errors = Seq(""""error":{"code":"failed","message":"private provider detail"}""") ++
          (if (api == "responses") Seq(""""status":"failed"""") else Seq.empty)
        errors.foreach { failure =>
          val result = AIResponseCodec.decode(api, s"{$failure,$observations}")
          assert(result.diagnostic.exists(_._1 == "provider_error"))
          assert(result.value == None.orNull && result.usage.getLong(2) == 7L && result.responseId == "kept-id")
          assert(!result.diagnostic.get._2.contains("private"))
        }
        val successful = AIResponseCodec.decode(api, s"{$observations}")
        assert(successful.diagnostic.exists(_._1 == "parse_error") && successful.value == None.orNull)
      }
      val valid = if (api == "chat_completions") {
        """{"index":0,"message":{"role":"assistant","content":"partial"}}"""
      } else {
        """{"type":"message","role":"assistant","content":[{"type":"output_text","text":"partial"}]}"""
      }
      val mixed = AIResponseCodec.decode(api, s"""{"$key":[$valid,null]}""")
      assert(mixed.diagnostic.exists(_._1 == "parse_error") && mixed.value == None.orNull)
    }
  }

  private val missing: Null = None.orNull
  // Answer examples from the public guide, completed with the reference's envelope/usage.
  // https://developers.openai.com/api/docs/guides/decisions
  // https://developers.openai.com/api/reference/python/resources/decisions/methods/create
  private val decisionFixture =
    """{"model":"gpt-6-luna","answers":[
      | {"type":"predicate","name":"visible_damage","probability":0.92},
      | {"type":"choice","name":"department","choice":"billing","confidence":0.93,
      |  "probabilities":[{"value":"billing","probability":0.95},{"value":"technical","probability":0.02},
      |    {"value":"shipping","probability":0.01},{"value":"other","probability":0.02}]},
      | {"type":"score","name":"severity","score":1.1,"confidence":0.55,
      |  "probabilities":[{"value":0,"label":"Cosmetic","probability":0.1},
      |    {"value":1,"label":"Workaround available","probability":0.7},
      |    {"value":2,"label":"Fully blocked","probability":0.2}]}
      |],"usage":{"input_tokens":20,"output_tokens":4,"total_tokens":24,
      |  "input_tokens_details":{"cached_tokens":3,"cache_write_tokens":2},
      |  "output_tokens_details":{"reasoning_tokens":1}}}""".stripMargin

  private def decode(api: String, text: String): AIResponseCodec.Decoded = AIResponseCodec.decode(api, text)

  private def answer(text: String): String = s"""{"model":"model","answers":[$text]}"""

  private def diagnostic(api: String, text: String, code: String): AIResponseCodec.Decoded = {
    val result = decode(api, text)
    assert(result.diagnostic.map(_._1).contains(code))
    result
  }

  private def row(result: AIResponseCodec.Decoded): Row = result.value.asInstanceOf[Row]

  test("Responses permits a bounded echo envelope without relaxing other JSON parsing checks") {
    val replies = Seq(
      "chat_completions" -> """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}""",
      "responses" -> """{"output":[{"type":"message","role":"assistant",
        |"content":[{"type":"output_text","text":"answer"}]}]}""".stripMargin,
      "embeddings" -> """{"data":[{"index":0,"embedding":[1.0]}]}""",
      "systemone" -> """{"model":"model","answers":{"q":true}}""",
      "decisions" -> decisionFixture)
    replies.foreach { case (api, reply) =>
      val limit = AIJson.MaxDepth + (if (api == "responses") 3 else 0)
      def nested(depth: Int): String =
        reply.dropRight(1) + ""","ignored":""" + "[" * (depth - 1) + "0" + "]" * (depth - 1) + "}"
      assert(decode(api, nested(limit)).diagnostic.isEmpty, api)
      diagnostic(api, nested(limit + 1), "parse_error")
    }
    Seq(
      """{"output":[],"text":{"format":{"x":1,"x":2}}}""",
      """{"output":[],"text":{"format":{"x":NaN}}}""",
      ("""{"output":[],"text":{"format":{"x":""" + "1" * 1001 + "}}}"),
      """{"output":[]} {}"""
    ).foreach(diagnostic("responses", _, "parse_error"))
  }

  test("Decisions public fixtures round-trip through the stable Spark schema and normalized usage") {
    val result = decode("decisions", decisionFixture)
    assert(result.diagnostic.isEmpty)
    val schema = AIResponseCodec.outputType("decisions").asInstanceOf[StructType]
    assert(schema("usage").dataType == UsageUtils.UsageStructType)
    val restored = spark.createDataFrame(Collections.singletonList(row(result)), schema).head()
    val answers = restored.getSeq[Row](1)
    assert(answers.map(_.getString(0)) == Seq("predicate", "choice", "score"))
    assert(answers.head.getDouble(2) == 0.92)
    assert(answers(1).getStruct(3) == Row("string", "billing", missing))
    assert(answers(1).getSeq[Row](6).head.getStruct(0) == Row("string", "billing", missing))
    assert(answers(2).getSeq[Row](7)(1) == Row(1, "Workaround available", 0.7))
    assert(restored.getStruct(2) == result.usage)
    assert(result.usage.getLong(0) == 20L)
    assert(result.usage.getMap[String, Long](3) == Map("cached_tokens" -> 3L, "cache_write_tokens" -> 2L))
    assert(result.usage.getMap[String, Long](4) == Map("reasoning_tokens" -> 1L))
  }

  test("Decisions boolean choices are typed and mixed refusals retain ordered siblings") {
    val result = diagnostic("decisions", answer(
      """{"type":"refusal","name":"blocked"},
        |{"type":"choice","name":"typed","choice":true,"confidence":0.8,
        |"probabilities":[{"value":true,"probability":0.8},{"value":"true","probability":0.2}]},
        |{"type":"predicate","name":null,"probability":0.1}""".stripMargin), "refusal")
    val answers = row(result).getSeq[Row](1)
    assert(answers.map(_.getString(0)) == Seq("refusal", "choice", "predicate"))
    assert(answers(1).getStruct(3) == Row("boolean", missing, true))
    assert(answers(1).getSeq[Row](6)(1).getStruct(0) == Row("string", "true", missing))
    assert(answers(2).getDouble(2) == 0.1)
    assert(result.usage == missing)
  }

  test("Decisions validates required fields and finite bounded numeric probabilities") {
    Seq(
      """{"type":"predicate"}""",
      """{"type":"predicate","probability":"0.3"}""",
      """{"type":"predicate","probability":-0.1}""",
      """{"type":"predicate","probability":1.0000000000000000001}""",
      """{"type":"predicate","probability":1e1000}""",
      """{"type":"predicate","probability":0.5,"name":true}""",
      """{"type":"future_answer","probability":0.5}""",
      """{"type":"choice","choice":true,"confidence":0.5,
        |"probabilities":[{"value":"true","probability":0.5},{"value":false,"probability":0.5}]}""".stripMargin,
      """{"type":"choice","choice":"a","confidence":2,
        |"probabilities":[{"value":"a","probability":0.5},{"value":"b","probability":0.5}]}""".stripMargin,
      """{"type":"choice","choice":1,"confidence":0.5,"probabilities":[]}""",
      """{"type":"score","score":1,"confidence":0.5,
        |"probabilities":[{"value":1,"label":"A","probability":0.5},
        |{"value":0,"label":"B","probability":0.5}]}""".stripMargin,
      """{"type":"score","score":2,"confidence":0.5,
        |"probabilities":[{"value":0,"label":"A","probability":1}]}""".stripMargin,
      """{"type":"score","score":0,"confidence":0.5,"probabilities":[{"value":0,"probability":1}]}"""
    ).foreach(a => diagnostic("decisions", answer(a), "parse_error"))
    diagnostic("decisions", """{"answers":[{"type":"refusal"}]}""", "parse_error")
    diagnostic("decisions", """{"model":"model","answers":[]}""", "parse_error")
    diagnostic("decisions", """{"model":"model","answers":{}}""", "parse_error")
  }

  test("Decisions score bounds are checked before Double rounding or underflow") {
    def response(score: String): String =
      NativeRequestFixtures.DecisionResponse.replace("\"score\":1.7", s""""score":$score""")
    Seq("-1e-400", "2.0000000000000000001").foreach { score =>
      diagnostic("decisions", response(score), "parse_error")
    }
    Seq("0", "2").foreach { score =>
      val result = decode("decisions", response(score))
      assert(result.diagnostic.isEmpty)
      assert(row(result).getSeq[Row](1)(2).getDouble(4) == score.toDouble)
    }
  }

  test("System One has lossless JSON answers and does not invent a provider answer schema") {
    // The public guide establishes the envelope, not a complete answer/usage schema:
    // https://learn.microsoft.com/en-us/azure/foundry/foundry-models/how-to/use-foundry-models-microsoft-decision
    // These heterogeneous answer values exercise SynapseML's representation, not guessed provider projections.
    val payload =
      """{"model":"microsoft-decision-1","answers":{"n":0.75,"b":true,"s":"true","nothing":null,
        |"a":[1,false],"o":{"type":"future","unknown":{"confidence":"not a known field"}}},
        |"usage":{"input_tokens":12,"output_tokens":3,"total_tokens":15,
        |"input_tokens_details":{"cache_write_tokens":2},"future_usage":{"unit":"new"}}}""".stripMargin
    val result = decode("systemone", payload)
    assert(result.diagnostic.isEmpty)
    val schema = AIResponseCodec.outputType("systemone").asInstanceOf[StructType]
    val restored = spark.createDataFrame(Collections.singletonList(row(result)), schema).head()
    val expected = payload.parseJson.asJsObject.fields("answers").asJsObject.fields
    val answers = restored.getMap[String, String](1).map { case (k, v) => k -> v.parseJson }
    assert(answers == expected)
    assert(restored.getString(0) == "microsoft-decision-1")
    assert(restored.getString(2).parseJson == payload.parseJson.asJsObject.fields("usage"))
    assert(result.usage.getLong(0) == 12L)
    assert(result.usage.getMap[String, Long](3) == Map("cache_write_tokens" -> 2L))
  }

  test("System One validates only the documented response envelope and known usage") {
    Seq(
      """{"model":false,"answers":{"q":1}}""",
      """{"model":"m","answers":[]}""",
      """{"model":"m","answers":{"q":1},"usage":12}""",
      """{"model":"m","answers":{"q":1},"usage":{"input_tokens":1.5}}"""
    ).foreach(diagnostic("systemone", _, "parse_error"))
    val unobserved = decode("systemone", """{"model":"m","answers":{"q":true}}""")
    assert(unobserved.diagnostic.isEmpty && unobserved.usage == missing && row(unobserved).isNullAt(2))
  }

  private def textPart(text: String): JsValue = JsObject("type" -> JsString("output_text"), "text" -> JsString(text))

  private def message(parts: JsValue*): JsValue =
    JsObject("type" -> JsString("message"), "role" -> JsString("assistant"), "content" -> JsArray(parts.toVector))

  private def response(output: JsValue*): JsObject = JsObject(
    "id" -> JsString("resp-example"), "status" -> JsString("completed"), "output" -> JsArray(output.toVector))

  test("Responses concatenates every assistant text part in order without tool arguments") {
    val raw = response(message(textPart("a"), textPart("b")),
      JsObject("type" -> JsString("function_call"), "arguments" -> JsString("not prose")),
      message(textPart("c"))).compactPrint
    val result = decode("responses", raw)
    assert(result.value == "abc" && result.responseId == "resp-example" && result.diagnostic.isEmpty)
    assert(result.usage == missing)
  }

  test("Responses partial text and usage survive provider failure, refusal and incomplete diagnostics") {
    val base = response(message(textPart("partial")))
    val usage = decisionFixture.parseJson.asJsObject.fields("usage")
    val cases = Seq(
      ("incomplete", JsObject("status" -> JsString("incomplete"))),
      ("provider_error", JsObject("status" -> JsString("failed"),
        "error" -> JsObject("code" -> JsString("server_error"), "message" -> JsString("Sensitive provider text")))),
      ("refusal", JsObject("output" -> JsArray(message(textPart("partial")),
        message(JsObject("type" -> JsString("refusal"), "refusal" -> JsString("Sensitive refusal text"))))))
    )
    cases.foreach { case (code, changes) =>
      val raw = JsObject(base.fields ++ changes.fields + ("usage" -> usage)).compactPrint
      val result = diagnostic("responses", raw, code)
      assert(result.value == "partial" && result.usage.getLong(0) == 20L && result.responseId == "resp-example")
      assert(!result.diagnostic.get._2.contains("Sensitive"))
    }
  }

  test("Responses no-text output is not a refusal and malformed known text is a parse error") {
    diagnostic("responses", response(
      JsObject("type" -> JsString("function_call"), "arguments" -> JsString("{}"))).compactPrint, "no_text")
    val malformed = message(JsObject("type" -> JsString("output_text"), "text" -> JsArray()))
    diagnostic("responses", response(malformed).compactPrint, "parse_error")
    diagnostic("responses", """{"output":[{"type":"message","role":"assistant"}]}""", "parse_error")
    val emptyText = decode("responses", response(message(textPart(""))).compactPrint)
    assert(emptyText.diagnostic.isEmpty && emptyText.value == "")
  }

  private def chat(content: String, finish: String = "stop", refusal: String = "null"): String =
    s"""{"id":"chat-example","choices":[{"index":0,"finish_reason":"$finish",
       |"message":{"role":"assistant","content":$content,"refusal":$refusal}}]}""".stripMargin

  test("Chat empty refusal preserves successful text without masking other diagnostics") {
    Seq("null", "\"\"").foreach { refusal =>
      val decoded = decode("chat_completions", chat("\"ok\"", refusal = refusal))
      assert(decoded.value == "ok" && decoded.diagnostic.isEmpty && decoded.responseId == "chat-example")
      diagnostic("chat_completions", chat("null", refusal = refusal), "no_text")
      diagnostic("chat_completions", chat("\"partial\"", "length", refusal), "incomplete")
      diagnostic("chat_completions", chat("null", "content_filter", refusal), "refusal")
    }
    Seq("\"declined\"", "\" \"").foreach { refusal =>
      diagnostic("chat_completions", chat("\"partial\"", refusal = refusal), "refusal")
    }
  }

  Seq("chat_completions", "responses", "embeddings", "systemone", "decisions").foreach { api =>
    val fixture = api match {
      case "chat_completions" => chat("\"ok\"").parseJson.asJsObject
      case "responses" => response(message(textPart("ok")))
      case "embeddings" => """{"data":[{"index":0,"embedding":[0.5]}]}""".parseJson.asJsObject
      case "systemone" => """{"model":"model","answers":{"q":true}}""".parseJson.asJsObject
      case "decisions" => answer("""{"type":"predicate","name":"q","probability":0.5}""").parseJson.asJsObject
    }
    test(s"$api public HTTP decoding rejects malformed UTF-8 without changing valid Unicode") {
      val cases = Seq("malformed-utf8", "http-error", "unicode", "replacement")
      val payloads = cases.map { name =>
        val text = name match {
          case "unicode" => "caf\u00e9 \u96ea \uD83D\uDE00"
          case "replacement" => "\uFFFD"
          case _ => "private-value~"
        }
        val payload = JsObject(fixture.fields ++ Map("model" -> JsString(text), "id" -> JsString("kept-id"),
          "usage" -> """{"total_tokens":7}""".parseJson, "future" -> JsString(text))).compactPrint
          .replace("\"ok\"", JsString(text).compactPrint)
        name -> payload
      }.toMap
      Using.resource(new AIRecordingServer((_, body) => {
        val name = cases.find(body.contains).get
        (if (name == "http-error") 400 else 200) -> payloads(name)
      }, encodeResponse = text => text.getBytes(StandardCharsets.UTF_8)
        .map(byte => if (byte == '~'.toByte) 0xff.toByte else byte))) { server =>
        val endpoint = if (api == "systemone") server.url else server.url + "/v1"
        val stage = new AIPrompt().setApiType(api).setUrl(endpoint).setModel("model")
          .setInputCol("text").setOutputCol("answer").setErrorCol("error")
          .setUsageCol("usage").setResponseIdCol("id").setRawResponseCol("raw")
        if (api == "systemone") stage.setQuestions(NativeRequestFixtures.SystemQuestions)
        if (api == "decisions") stage.setQuestions(NativeRequestFixtures.DecisionQuestions)
        val data = spark.createDataFrame(cases.map(Row(_)).asJava, new StructType().add("text", "string"))
        val result = stage.transform(data)
        assert(result.schema == stage.transformSchema(data.schema))
        val rows = result.collect()
        assert(rows.length == cases.length && server.requests.size() == cases.length)
        rows.foreach { row =>
          val name = row.getAs[String]("text")
          if (name == "malformed-utf8" || name == "http-error") {
            assert(Seq("answer", "raw", "usage", "id").forall(field => row.isNullAt(row.fieldIndex(field))))
            val error = row.getAs[Row]("error")
            val code = if (name == "http-error") "http_error" else "parse_error"
            assert(error.getString(0).parseJson.asJsObject.fields("code") == JsString(code))
            assert(!error.getString(0).contains("private-value") && !error.getString(0).contains("kept-id"))
            if (name == "http-error") assert(error.getAs[Row]("status").getAs[Int]("statusCode") == 400)
            else assert(error.getAs[Row]("status") == null)
          } else {
            assert(row.getAs[Row]("error") == null)
            assert(row.getAs[String]("raw") == payloads(name))
            assert(row.getAs[Any]("answer") == AIResponseCodec.decode(api, payloads(name)).value)
            assert(row.getAs[String]("id") == "kept-id" && row.getAs[Row]("usage").getLong(2) == 7L)
          }
        }
      }
    }

    test(s"$api JSON surrogate validation preserves escaped Unicode without repairing text") {
      val escape = "\\" + "u"
      val cases = Seq("d800", "dc00", "d800x", s"dc00${escape}d800", s"d83d${escape}de00")
        .map(code => "\"" + escape + code + "\"")
      Using.resource(new AIRecordingServer((_, body) => {
        val index = cases.indices.find(i => body.contains(s"case-$i")).get
        val payload = fixture.compactPrint.dropRight(1) + s""","future":${cases(index)}}"""
        200 -> payload.replace("\"ok\"", cases(index))
      })) { server =>
        val stage = new AIPrompt().setApiType(api)
          .setUrl(if (api == "systemone") server.url else server.url + "/v1").setModel("model")
          .setInputCol("text").setOutputCol("answer").setErrorCol("error")
          .setUsageCol("usage").setResponseIdCol("id").setRawResponseCol("raw")
        if (api == "systemone") stage.setQuestions(NativeRequestFixtures.SystemQuestions)
        if (api == "decisions") stage.setQuestions(NativeRequestFixtures.DecisionQuestions)
        val data = spark.createDataFrame(cases.indices.map(i => Row(s"case-$i")).asJava,
          new StructType().add("text", "string"))
        val rows = stage.transform(data).collect().sortBy(_.getAs[String]("text"))
        rows.dropRight(1).foreach { row =>
          assert(Seq("answer", "usage", "id").forall(field => row.isNullAt(row.fieldIndex(field))))
          assert(row.getAs[Row]("error").getString(0).parseJson.asJsObject.fields("code") == JsString("parse_error"))
          assert(row.getAs[String]("raw").contains("\\ud"))
        }
        val valid = rows.last
        assert(valid.getAs[Row]("error") == null)
        assert(valid.getAs[Any]("answer") == AIResponseCodec.decode(api, valid.getAs[String]("raw")).value)
        if (Set("chat_completions", "responses")(api)) assert(valid.getAs[String]("answer") == "\uD83D\uDE00")
        assert(server.requests.size() == cases.size)
      }
    }

    test(s"$api optional response diagnostics preserve public answers and independent metadata") {
      val fields = fixture.fields ++ Map("id" -> JsString("kept-id"),
        "usage" -> """{"total_tokens":7}""".parseJson)
      val malformedErrors = Seq(JsArray(), JsObject("code" -> JsNumber(42)),
        JsObject("message" -> JsNumber(42))).map(error => fields.updated("error", error) -> "parse_error")
      val protocolCases = api match {
        case "chat_completions" =>
          def choice(finish: JsValue, refusal: JsValue): Map[String, JsValue] =
            fields.updated("choices", JsArray(JsObject("index" -> JsNumber(0), "finish_reason" -> finish,
              "message" -> JsObject("role" -> JsString("assistant"), "content" -> JsString("ok"),
                "refusal" -> refusal))))
          Seq(choice(JsNumber(42), JsString("")) -> "parse_error",
            choice(JsString("stop"), JsNumber(42)) -> "parse_error",
            choice(JsNumber(42), spray.json.JsNull) -> "parse_error",
            choice(JsString("length"), JsNumber(42)) -> "incomplete",
            choice(JsNumber(42), JsString("declined")) -> "refusal",
            choice(JsNumber(42), spray.json.JsNull).updated("error",
              JsObject("code" -> JsString("failed"))) -> "provider_error")
        case "responses" =>
          Seq(fields.updated("status", JsNumber(42)) -> "parse_error",
            fields.updated("incomplete_details", JsNumber(42)) -> "incomplete",
            fields.updated("incomplete_details", JsObject("reason" -> JsNumber(42))) -> "incomplete",
            fields.updated("output", JsArray(JsObject(message(textPart("ok")).asJsObject.fields
              .updated("status", JsNumber(42))))) -> "parse_error",
            fields.updated("status", JsString("failed")).updated("error", JsArray()) -> "provider_error",
            fields.updated("status", JsNumber(42)).updated("output", JsArray(message(textPart("ok"),
              JsObject("type" -> JsString("refusal"), "refusal" -> JsString("declined"))))) -> "refusal")
        case _ => Seq.empty
      }
      val data = spark.createDataFrame(Collections.singletonList(Row("hello")),
        new StructType().add("text", "string"))
      (malformedErrors ++ protocolCases).foreach { case (payloadFields, code) =>
        val payload = JsObject(payloadFields).compactPrint
        Using.resource(new AIRecordingServer((_, _) => 200 -> payload)) { server =>
          val endpoint = if (api == "systemone") server.url else server.url + "/v1"
          val stage = new AIPrompt().setApiType(api).setUrl(endpoint).setModel("model")
            .setInputCol("text").setOutputCol("answer").setErrorCol("error")
            .setUsageCol("usage").setResponseIdCol("id").setRawResponseCol("raw")
          if (api == "systemone") stage.setQuestions(NativeRequestFixtures.SystemQuestions)
          if (api == "decisions") stage.setQuestions(NativeRequestFixtures.DecisionQuestions)
          val output = stage.transform(data)
          assert(output.schema == stage.transformSchema(data.schema))
          val observed = output.head()
          withClue(payload) {
            api match {
              case "embeddings" => assert(observed.getAs[Vector]("answer").toArray.sameElements(Array(0.5)))
              case "systemone" => assert(observed.getAs[Row]("answer").getMap[String, String](1)("q") == "true")
              case "decisions" => assert(observed.getAs[Row]("answer").getSeq[Row](1).head.getDouble(2) == 0.5)
              case _ => assert(observed.getAs[String]("answer") == "ok")
            }
            assert(observed.getAs[String]("id") == "kept-id" && observed.getAs[Row]("usage").getLong(2) == 7L)
            assert(observed.getAs[Row]("error").getString(0).contains(code))
            assert(observed.getAs[String]("raw") == payload && server.requests.size() == 1)
          }
        }
      }
    }

    test(s"$api nullable usage details preserve available counters without a public row error") {
      val chatNames = api == "chat_completions" || api == "embeddings"
      val input = if (chatNames) "prompt_tokens" else "input_tokens"
      val output = if (chatNames) "completion_tokens" else "output_tokens"
      val usage = JsObject(input -> JsNumber(7), output -> JsNumber(3), "total_tokens" -> JsNumber(10),
        s"${input}_details" -> """{"cached_tokens":2,"audio_tokens":null}""".parseJson,
        s"${output}_details" -> """{"reasoning_tokens":1,"audio_tokens":null}""".parseJson)
      val payload = JsObject(fixture.fields ++ Map("usage" -> usage, "id" -> JsString("kept-id"))).compactPrint
      Using.resource(new AIRecordingServer((_, _) => 200 -> payload)) { server =>
        val endpoint = if (api == "systemone") server.url else server.url + "/v1"
        val stage = new AIPrompt().setApiType(api).setUrl(endpoint).setModel("model")
          .setInputCol("text").setOutputCol("answer").setErrorCol("error")
          .setUsageCol("usage").setResponseIdCol("id").setRawResponseCol("raw")
        if (api == "systemone") stage.setQuestions(NativeRequestFixtures.SystemQuestions)
        if (api == "decisions") stage.setQuestions(NativeRequestFixtures.DecisionQuestions)
        val data = spark.createDataFrame(Collections.singletonList(Row("hello")),
          new StructType().add("text", "string"))
        val result = stage.transform(data)
        assert(result.schema == stage.transformSchema(data.schema))
        val observed = result.head()
        val expected = Row(7L, 3L, 10L, Map("cached_tokens" -> 2L), Map("reasoning_tokens" -> 1L))
        assert(!observed.isNullAt(observed.fieldIndex("answer")) && observed.getAs[Row]("error") == null)
        assert(observed.getAs[Row]("usage") == expected && observed.getAs[String]("id") == "kept-id")
        if (api == "decisions") assert(observed.getAs[Row]("answer").getStruct(2) == expected)
        if (api == "systemone") assert(AIJson.parse(observed.getAs[Row]("answer").getString(2)) == usage)
        assert(observed.getAs[String]("raw") == payload && server.requests.size() == 1)
      }
    }
  }

  Seq(
    "chat_completions" -> """{"choices":[{"index":0,"message":{"role":"assistant","content":"ok"}}]}""",
    "responses" -> """{"output":[{"type":"message","role":"assistant",
      |"content":[{"type":"output_text","text":"ok"}]}]}""".stripMargin,
    "embeddings" -> """{"data":[{"index":0,"embedding":[0.5]}]}"""
  ).foreach { case (api, fixture) =>
    test(s"$api malformed optional model metadata retains public answers and independent observations") {
      val payload = JsObject(fixture.parseJson.asJsObject.fields ++ Map(
        "model" -> JsNumber(42), "id" -> JsString("kept-id"),
        "usage" -> """{"total_tokens":7}""".parseJson)).compactPrint
      Using.resource(new AIRecordingServer((_, _) => 200 -> payload)) { server =>
        val data = spark.createDataFrame(Collections.singletonList(Row("hello")),
          new StructType().add("text", "string"))
        val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
          .setInputCol("text").setOutputCol("answer").setErrorCol("error")
          .setUsageCol("usage").setResponseIdCol("id")
        val output = stage.transform(data)
        assert(output.schema == stage.transformSchema(data.schema))
        val observed = output.head()
        if (api == "embeddings") assert(observed.getAs[Vector]("answer").toArray.sameElements(Array(0.5)))
        else assert(observed.getAs[String]("answer") == "ok")
        assert(observed.getAs[String]("id") == "kept-id" && observed.getAs[Row]("usage").getLong(2) == 7L)
        assert(observed.getAs[Row]("error").getString(0).contains("parse_error"))
        assert(server.requests.size() == 1)
      }
    }
  }

  test("malformed optional model metadata does not replace partial-response provider diagnostics") {
    val base = response(message(textPart("partial"))).fields.updated("model", JsNumber(42))
    Seq("incomplete" -> "incomplete", "failed" -> "provider_error").foreach { case (status, code) =>
      val decoded = diagnostic("responses", JsObject(base.updated("status", JsString(status))).compactPrint, code)
      assert(decoded.value == "partial" && decoded.responseId == "resp-example")
    }
    Seq("systemone" -> """{"model":42,"answers":{"q":true}}""",
      "decisions" -> """{"model":42,"answers":[{"type":"predicate","probability":0.5}]}""")
      .foreach { case (api, payload) => assert(diagnostic(api, payload, "parse_error").value == missing) }
  }

  test("Chat deliberately projects the first choice and preserves refusal and truncation") {
    val base = chat("\"first\"").parseJson.asJsObject
    val choices = base.fields("choices").asInstanceOf[JsArray].elements
    val second = chat("\"second\"").parseJson.asJsObject.fields("choices").asInstanceOf[JsArray].elements
    val raw = JsObject(base.fields.updated("choices", JsArray(choices ++ second))).compactPrint
    assert(decode("chat_completions", raw).value == "first")
    assert(diagnostic("chat_completions", chat("\"partial\"", "length"), "incomplete").value == "partial")
    diagnostic("chat_completions", chat("null", refusal = "\"declined\""), "refusal")
    diagnostic("chat_completions", chat("null", "content_filter"), "refusal")
    diagnostic("chat_completions", chat("null", "tool_calls"), "no_text")
    diagnostic("chat_completions", chat("42"), "parse_error")
    diagnostic("chat_completions", """{"choices":[{"message":{"role":"assistant","content":"Missing index"}}]}""",
      "parse_error")
  }

  test("Embeddings accepts exactly one indexed float vector and rejects base64 or malformed dimensions") {
    val raw = """{"object":"list","model":"embedding","data":[
      |{"object":"embedding","index":0,"embedding":[0.25,-0.5,1]}],
      |"usage":{"prompt_tokens":3,"total_tokens":3}}""".stripMargin
    val result = decode("embeddings", raw)
    assert(result.diagnostic.isEmpty && result.value.asInstanceOf[Vector].toArray.sameElements(Array(0.25, -0.5, 1)))
    assert(result.usage.getLong(0) == 3L && result.usage.isNullAt(1))
    Seq(
      """{"data":[{"index":0,"embedding":"AACAPw=="}]}""",
      """{"data":[{"index":1,"embedding":[1]}]}""",
      """{"data":[{"index":0.1,"embedding":[1]}]}""",
      """{"data":[{"embedding":[1]}]}""",
      """{"data":[{"index":0,"embedding":[1e1000]}]}""",
      """{"data":[{"index":0,"embedding":[]}]}""",
      """{"data":[{"index":0,"embedding":[true]}]}""",
      """{"object":42,"data":[{"index":0,"embedding":[1]}]}""",
      """{"object":"wrong","data":[{"index":0,"embedding":[1]}]}""",
      """{"data":[{"object":42,"index":0,"embedding":[1]}]}""",
      """{"data":[{"object":"wrong","index":0,"embedding":[1]}]}""",
      """{"data":[{"index":0,"embedding":[1]},{"index":1,"embedding":[2]}]}""",
      """{"data":[]}"""
    ).foreach(diagnostic("embeddings", _, "parse_error"))
  }

  test("Usage counts reject negative fractional overflow and noninteger known details") {
    val base = response(message(textPart("ok")))
    Seq(
      """{"input_tokens":-1}""",
      """{"input_tokens":0.5}""",
      """{"input_tokens":null}""",
      """{"input_tokens":9223372036854775808}""",
      """{"input_tokens":"1"}""",
      """{"input_tokens_details":{"cache_write_tokens":true}}""",
      """{"output_tokens_details":{"reasoning_tokens":0.5}}""",
      """{"input_tokens_details":[]}"""
    ).foreach { usage =>
      val decoded = diagnostic("responses",
        JsObject(base.fields.updated("usage", usage.parseJson)).compactPrint, "parse_error")
      assert(decoded.value == "ok" && decoded.responseId == "resp-example" && decoded.usage == missing)
    }
    val chatUsage = """{"prompt_tokens":9,"completion_tokens":3,"total_tokens":12,
      |"prompt_tokens_details":{"cached_tokens":2},"completion_tokens_details":{"reasoning_tokens":1}}""".stripMargin
    val chatBase = chat("\"ok\"").parseJson.asJsObject
    val result = decode("chat_completions", JsObject(chatBase.fields + ("usage" -> chatUsage.parseJson)).compactPrint)
    assert(result.usage == Row(9L, 3L, 12L, Map("cached_tokens" -> 2L), Map("reasoning_tokens" -> 1L)))
  }

  test("malformed optional metadata preserves valid answers and independent observations on every API") {
    val fixtures = Seq(
      "chat_completions" -> chat("\"ok\""),
      "responses" -> response(message(textPart("ok"))).compactPrint,
      "embeddings" -> """{"data":[{"index":0,"embedding":[0.5]}]}""",
      "systemone" -> """{"model":"model","answers":{"q":true}}""",
      "decisions" -> answer("""{"type":"predicate","name":"q","probability":0.5}"""))
    fixtures.foreach { case (api, fixture) =>
      val base = fixture.parseJson.asJsObject.fields
      val badId = diagnostic(api, JsObject(base ++ Map(
        "id" -> JsArray(), "usage" -> """{"total_tokens":7}""".parseJson)).compactPrint, "parse_error")
      assert(badId.value != missing && badId.usage.getLong(2) == 7L && badId.responseId == missing)
      val badUsage = diagnostic(api, JsObject(base ++ Map(
        "id" -> JsString("kept-id"), "usage" -> """{"total_tokens":-1}""".parseJson)).compactPrint, "parse_error")
      assert(badUsage.value != missing && badUsage.responseId == "kept-id" && badUsage.usage == missing)
      if (api == "systemone") assert(row(badUsage).getString(2) == """{"total_tokens":-1}""")
      if (api == "decisions") assert(row(badUsage).isNullAt(2))
    }
    val failed = JsObject(response(message(textPart("partial"))).fields ++ Map(
      "status" -> JsString("failed"), "id" -> JsArray()))
    assert(diagnostic("responses", failed.compactPrint, "provider_error").value == "partial")
  }

  test("Provider errors and invalid JSON have distinct safe diagnostics on every API") {
    Seq("chat_completions", "responses", "embeddings", "systemone", "decisions").foreach { api =>
      val failed = diagnostic(api, """{"error":{"code":"rejected","message":"Sensitive provider text"}}""",
        "provider_error")
      assert(failed.value == missing && failed.usage == missing)
      Seq("not-json-private-value", "[]", """{"model":"one","model":"two"}""").foreach { raw =>
        val bad = diagnostic(api, raw, "parse_error")
        assert(!bad.diagnostic.get._2.contains(raw))
      }
    }
  }
}
