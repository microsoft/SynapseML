// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.services.openai.OpenAIDecisions
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row
import spray.json.{JsArray, JsNumber, JsObject, JsString}

import java.nio.file.Files
import scala.collection.JavaConverters._
import scala.util.Using

class AIPromptUnicodeSuite extends TestBase {
  test("native metadata rejects malformed explicit and default string Params before saving") {
    import spark.implicits._
    val data = Seq("hello").toDF("text")
    val saved = Files.createTempDirectory(tmpDir, "unicode-metadata").resolve("model").toString
    new AIPrompt().setSystemPrompt("keep").write.session(spark).save(saved)
    val reply = """{"choices":[{"index":0,"message":{"role":"assistant","content":"ok"}}]}"""
    Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
      for (name <- Seq("systemPrompt", "promptTemplate"); binding <- Seq("explicit", "default", "shadowed");
           malformed <- Seq("private-\uD800", "private-\uDC00")) {
        val stage = new TestableAIMutableJsonDefaultPrompt("unicode-metadata")
          .setUrl(server.url + "/v1").setModel("model").setInputCol("text").setErrorCol("error")
        val param = if (name == "systemPrompt") stage.systemPrompt else stage.promptTemplate
        if (binding == "explicit") stage.set(param, malformed)
        else {
          stage.replaceJsonDefault(param, malformed)
          if (binding == "shadowed") stage.set(param, "valid")
        }
        val writer = stage.write.session(spark).overwrite()
        val error = intercept[IllegalArgumentException](writer.save(saved))
        assert(error.getMessage.contains("UTF-8") && !error.getMessage.contains("private-"))
        assert(error.getCause == null)
        assert(AIPrompt.read.session(spark).load(saved).getSystemPrompt == "keep")
        val fresh = Files.createTempDirectory(tmpDir, "unicode-rejected").resolve("model")
        intercept[IllegalArgumentException](writer.save(fresh.toString))
        assert(!Files.exists(fresh))
        if (name == "systemPrompt" && binding != "shadowed") {
          val row = stage.transform(data).head()
          assert(row.getAs[Row]("error").getString(0).contains("invalid_request"))
        }
      }
      assert(server.requests.isEmpty)
    }
  }

  test("native metadata preserves valid Unicode and literal escape text through explicit and default Params") {
    import spark.implicits._
    val data = Seq("hello").toDF("text")
    val reply = """{"choices":[{"index":0,"message":{"role":"assistant","content":"ok"}}]}"""
    Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
      for (value <- Seq("\\" + "ud800", "caf\u00e9 \u96ea \uD83D\uDE00 \uFFFD"); explicit <- Seq(false, true)) {
        val stage = new TestableAIMutableJsonDefaultPrompt("valid-unicode-metadata")
          .setUrl(server.url + "/v1").setModel("model").setOutputCol("answer").setErrorCol("error")
        if (explicit) stage.setSystemPrompt(value).setPromptTemplate(value)
        else {
          stage.replaceJsonDefault(stage.systemPrompt, value).replaceJsonDefault(stage.promptTemplate, value)
          stage.setInputCol("text")
        }
        val saved = Files.createTempDirectory(tmpDir, "valid-unicode-metadata").resolve("model").toString
        stage.write.session(spark).save(saved)
        val loaded = AIPrompt.read.session(spark).load(saved)
        Seq(stage, loaded).foreach { candidate =>
          assert(candidate.getSystemPrompt == value && candidate.getPromptTemplate == value)
          assert(candidate.isSet(candidate.systemPrompt) == explicit)
          assert(candidate.isSet(candidate.promptTemplate) == explicit)
          val row = candidate.transform(data).head()
          assert(row.getAs[Row]("error") == null && row.getAs[String]("answer") == "ok")
        }
        val requests = server.requests.asScala.map(request => AIJson.objectString(request._2)).toVector.takeRight(2)
        assert(requests.length == 2 && requests.forall { body =>
          body.fields("messages") match {
            case JsArray(messages) => messages.head.asJsObject.fields("content") == JsString(value)
            case _ => fail("Expected a request message array")
          }
        })
      }
      assert(server.requests.size() == 8)
    }
  }

  test("prompt template Unicode is validated before Spark literal encoding") {
    import spark.implicits._
    val data = Seq("row").toDF("text")
    val unicode = "caf\u00e9 \u96ea \uD83D\uDE00 \uFFFD"
    Seq(new AIPrompt(), new AIPrompt().setApiType("responses"),
      new AISystemOne(), new OpenAIDecisions()).foreach { stage =>
      val reply = stage.getApiType match {
        case "systemone" => NativeRequestFixtures.SystemResponse
        case "decisions" => NativeRequestFixtures.DecisionResponse
        case "responses" =>
          """{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]}"""
        case _ => """{"choices":[{"index":0,"message":{"role":"assistant","content":"ok"}}]}"""
      }
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val url = if (stage.getApiType == "systemone") server.url else server.url + "/v1"
        stage.setUrl(url).setModel("model").setOutputCol("answer").setErrorCol("error")
        if (stage.getApiType == "systemone") {
          stage.setQuestions(NativeRequestFixtures.SystemQuestions).setInputMode("json")
        } else if (stage.getApiType == "decisions") {
          stage.setQuestions(NativeRequestFixtures.DecisionQuestions)
        }
        Seq("\uD800", "\uDC00").foreach { malformed =>
          stage.setPromptTemplate("\"private-template-" + malformed + "\"")
          Seq[() => Any](() => stage.transformSchema(data.schema), () => stage.transform(data).collect())
            .foreach { evaluate =>
              val error = intercept[IllegalArgumentException](evaluate())
              assert(error.getMessage.contains("template") && !error.getMessage.contains("private-template"))
              assert(error.getCause == null)
            }
        }
        assert(server.requests.isEmpty)
        val result = stage.setPromptTemplate(JsString(unicode).compactPrint).transform(data).head()
        assert(result.getAs[Row]("error") == null && !result.isNullAt(result.fieldIndex("answer")))
        assert(server.requests.size() == 1 && server.requests.peek()._2.contains(unicode))
        if (stage.getApiType == "systemone") {
          assert(AIJson.objectString(server.requests.peek()._2).fields("state") == JsString(unicode))
        }
      }
    }
  }

  test("JSON surrogate validation rejects unpaired strings and keys across native setters and persistence") {
    val invalid = Seq("\uD800", "\uDC00", "\uDC00\uD800", "\uD800x", "x\uDC00")
    invalid.foreach { text =>
      val documents = Seq(JsObject("private-field" -> JsString(text)), JsObject(text -> JsString("private-value")))
      documents.foreach { document =>
        val values = Seq[Any](document, AIJson.AnyFormat.read(document),
          AIJson.AnyFormat.read(document).asInstanceOf[Map[String, Any]].asJava)
        values.foreach { value =>
          val error = intercept[IllegalArgumentException](AIJson.fromAny(value))
          assert(error.getMessage.contains("surrogate") && !error.getMessage.contains("private-"))
        }
        Seq(new AIPrompt(), new AISystemOne(), new OpenAIDecisions()).foreach { stage =>
          Seq[() => AIPrompt](
            () => stage.setExtraBody(document), () => stage.setQuestions(document),
            () => stage.setResponseFormat(AIJson.AnyFormat.read(document).asInstanceOf[Map[String, Any]])
          ).foreach { setter =>
            assert(intercept[IllegalArgumentException](setter()).getMessage.contains("surrogate"))
          }
        }
      }
    }
    val high = "\\" + "ud800"
    val low = "\\" + "udc00"
    Seq(s"""{"private-field":"$high"}""", s"""{"private-field":"$low"}""",
      s"""{"$high":1,"?":2}""", s"""{"$low":1,"?":2}""").foreach { json =>
      assert(intercept[IllegalArgumentException](AIJson.objectString(json)).getMessage.contains("surrogate"))
      val stage = new AIPrompt()
      assert(intercept[IllegalArgumentException](stage.extraBody.jsonDecode(s"""{"left":$json}"""))
        .getMessage.contains("surrogate"))
    }
    val unicode = "caf\u00e9 \u96ea \uD83D\uDE00 \uFFFD"
    val stage = new AIPrompt().setExtraBody(Map(unicode -> Seq(unicode)))
    val saved = Files.createTempDirectory(tmpDir, "aiprompt-unicode").resolve("model").toString
    stage.write.session(spark).save(saved)
    Seq(stage, stage.copy(ParamMap.empty), AIPrompt.load(saved)).foreach { candidate =>
      assert(candidate.getExtraBody == Map(unicode -> Seq(unicode)))
    }
    val escaped = """"caf""" + "\\" + "u00e9 " + "\\" + "u96ea " + "\\" + "ud83d" + "\\" + "ude00 " +
      "\\" + "ufffd\""
    assert(AIJson.parse(escaped) == JsString(unicode))
  }

  test("JSON surrogate validation rejects malformed per-row bodies without colliding wire keys") {
    import spark.implicits._
    val high = "\\" + "ud800"
    val low = "\\" + "udc00"
    val pair = "\\" + "ud83d" + "\\" + "ude00"
    val invalid = Seq(s"""{"private-field":"$high"}""", s"""{"private-field":"$low"}""",
      s"""{"$high":1,"?":2}""", s"""{"$low":1,"?":2}""")
    val valid = s"""{"$pair":"$pair","?":2}"""
    val data = (invalid :+ valid).zipWithIndex.map { case (body, id) => (id, "hello", body) }
      .toDF("id", "text", "body")
    val reply = """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""
    Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
      val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setInputCol("text")
        .setExtraBodyCol("body").setOutputCol("answer").setErrorCol("error")
      val rows = stage.transform(data).collect().sortBy(_.getAs[Int]("id"))
      rows.take(invalid.size).foreach { row =>
        val diagnostic = row.getAs[Row]("error").getString(0)
        assert(diagnostic.contains("invalid_request") && diagnostic.contains("surrogate"))
        assert(!diagnostic.contains("private-field") && row.getAs[String]("answer") == null)
      }
      assert(rows.last.getAs[Row]("error") == null && server.requests.size() == 1)
      val sent = AIJson.objectString(server.requests.peek()._2)
      assert(sent.fields("\uD83D\uDE00") == JsString("\uD83D\uDE00") && sent.fields("?") == JsNumber(2))
      stage.clear(stage.extraBody)
      Seq("\uD800", "\uDC00").foreach { malformed =>
        val row = stage.setSystemPrompt(malformed).transform(data.limit(1)).head()
        assert(row.getAs[Row]("error").getString(0).contains("invalid_request"))
      }
      assert(server.requests.size() == 1)
    }
  }

  test("structured response JSON surrogate diagnostics cover decoded values and keys in both generative APIs") {
    import spark.implicits._
    val high = "\\" + "ud800"
    val low = "\\" + "udc00"
    val pair = "\\" + "ud83d" + "\\" + "ude00"
    val documents = Map(
      "value-high" -> s"""{"value":"$high"}""",
      "value-low" -> s"""{"value":"$low"}""",
      "key-high" -> s"""{"$high":"private-value","?":"other"}""",
      "key-low" -> s"""{"$low":"private-value","?":"other"}""",
      "valid" -> s"""{'value':'$pair','$pair':'unchanged'}""")
    Seq("chat_completions", "responses").foreach { api =>
      Using.resource(new AIRecordingServer((_, body) => {
        val document = documents.find { case (name, _) => body.contains(name) }.get._2
        val text = JsString(document).compactPrint
        200 -> (if (api == "chat_completions") {
          s"""{"choices":[{"index":0,"message":{"role":"assistant","content":$text}}]}"""
        } else {
          s"""{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":$text}]}]}"""
        })
      })) { server =>
        val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
          .setInputCol("text").setOutputCol("answer").setErrorCol("error").setRawResponseCol("raw")
          .setPostProcessingOptions(Map("jsonSchema" -> "MAP<STRING,STRING>"))
        val rows = stage.transform(documents.keys.toSeq.toDF("text")).collect()
        rows.filter(_.getAs[String]("text") != "valid").foreach { row =>
          assert(row.getAs[Row]("error") != null)
          val diagnostic = row.getAs[Row]("error").getString(0)
          assert(diagnostic.contains("parse_error") && !diagnostic.contains("private-value"))
          assert(row.getAs[String]("raw").contains("\\\\u"))
        }
        val valid = rows.find(_.getAs[String]("text") == "valid").get
        assert(valid.getAs[Row]("error") == null)
        assert(valid.getAs[Map[String, String]]("answer") ==
          Map("value" -> "\uD83D\uDE00", "\uD83D\uDE00" -> "unchanged"))
        assert(server.requests.size() == documents.size)
      }
    }
  }
}
