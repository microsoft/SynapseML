// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.io.File
import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._
import scala.util.Using

class AIPromptDurableRequestSuite extends AIPromptOutputTestBase {
  test("durable prompt template Unicode rejects malformed literals before creating output") {
    delta()
    import spark.implicits._
    val data = Seq("row").toDF("text")
    Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.SystemResponse)) { server =>
      val root = path()
      val stage = new AISystemOne().setUrl(server.url).setModel("model")
        .setQuestions(NativeRequestFixtures.SystemQuestions).setInputMode("json")
        .setOutputCol("answer").setErrorCol("error").setOutputPath(root)
      Seq("\uD800", "\uDC00").foreach { malformed =>
        stage.setPromptTemplate("\"private-template-" + malformed + "\"")
        Seq[() => Any](() => stage.transformSchema(data.schema), () => stage.transform(data).collect())
          .foreach { evaluate =>
            val error = intercept[IllegalArgumentException](evaluate())
            assert(error.getMessage.contains("template") && !error.getMessage.contains("private-template"))
            assert(error.getCause == null)
          }
        assert(!new File(root).exists() && server.requests.isEmpty)
      }
      val unicode = "caf\u00e9 \u96ea \uD83D\uDE00 \uFFFD"
      val rows = stage.setPromptTemplate(JsString(unicode).compactPrint).transform(data).collect()
      assert(rows.length == 1 && rows.head.getAs[Row]("error") == null)
      assert(AIJson.objectString(server.requests.peek()._2).fields("state") == JsString(unicode))
      assert(AIPrompt.getOutputStatus(spark, root).allSucceeded)
      assert(stage.resume(spark).collect().toSet == rows.toSet)
      assert(AIPrompt.loadOutput(spark, root).collect().toSet == rows.toSet && server.requests.size() == 1)
    }
  }

  test("durable fingerprints distinguish malformed UTF-16 across interrupted recovery") {
    delta()
    import spark.implicits._
    val data = Seq("one", "two").toDF("text")
    val reply = """{"choices":[{"index":0,"message":{"role":"assistant","content":"ok"}}]}"""
    Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
      val root = path()
      val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setInputCol("text")
        .setSystemPrompt("\uD800").setOutputCol("answer").setErrorCol("error")
        .setOutputPath(root).setOutputBatchSize(1)
      intercept[IllegalStateException] {
        AIPromptOutput.transformObserved(stage, data, event =>
          if (event == "batch_committed") throw new IllegalStateException("stop after invalid request"))
      }
      val status = AIPrompt.getOutputStatus(spark, root)
      assert(status.committedCount == 1 && status.pendingCount == 1 && status.rowErrorCount == 1)
      assert(server.requests.isEmpty)
      val committed = AIPrompt.loadOutput(spark, root).head()
      assert(committed.getAs[Row]("error").getString(0).contains("invalid_request"))
      AIPrompt.releaseOutputOwner(spark, root, status.ownerToken.get, confirmStopped = true)
      Seq("?", "\uDC00", "\\" + "ud800", "\uFFFD", "\uD83D\uDE00").foreach { changed =>
        val error = intercept[IllegalArgumentException] {
          stage.copy(ParamMap.empty).setSystemPrompt(changed).resume(spark)
        }
        assert(error.getMessage.contains("configuration") && server.requests.isEmpty)
        val unchanged = AIPrompt.getOutputStatus(spark, root)
        assert(unchanged.ownerToken.isEmpty && unchanged.committedCount == 1 && unchanged.pendingCount == 1)
      }
      val rows = stage.resume(spark).collect()
      assert(rows.length == 2 && rows.contains(committed))
      assert(rows.forall(_.getAs[Row]("error").getString(0).contains("invalid_request")))
      val complete = AIPrompt.getOutputStatus(spark, root)
      assert(complete.isComplete && complete.rowErrorCount == 2 && !complete.allSucceeded)
      assert(AIPrompt.loadOutput(spark, root).collect().toSet == rows.toSet && server.requests.isEmpty)
    }
  }

  test("durable structured JSON surrogate errors remain terminal across resume and reload") {
    delta()
    import spark.implicits._
    val escape = "\\" + "u"
    Seq("chat_completions", "responses").foreach { api =>
      Using.resource(new AIRecordingServer((_, body) => {
        val document = if (body.contains("invalid-value")) s"""{"value":"${escape}d800"}"""
        else if (body.contains("invalid-key")) s"""{"${escape}dc00":"private-value","?":"other"}"""
        else s"""{"value":"${escape}d83d${escape}de00"}"""
        val text = JsString(document).compactPrint
        200 -> (if (api == "chat_completions") {
          s"""{"choices":[{"index":0,"message":{"role":"assistant","content":$text}}]}"""
        } else {
          s"""{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":$text}]}]}"""
        })
      })) { server =>
        val root = path()
        val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
          .setInputCol("text").setOutputCol("answer").setErrorCol("error").setRawResponseCol("raw")
          .setPostProcessingOptions(Map("jsonSchema" -> "MAP<STRING,STRING>")).setOutputPath(root)
        val rows = stage.transform(Seq("invalid-value", "invalid-key", "valid").toDF("text")).collect()
        rows.filter(_.getAs[String]("text") != "valid").foreach { row =>
          assert(row.getAs[Row]("error") != null)
          assert(row.getAs[Row]("error").getString(0).contains("parse_error"))
          assert(row.getAs[String]("raw").contains("\\\\u"))
        }
        val valid = rows.find(_.getAs[String]("text") == "valid").get
        assert(valid.getAs[Row]("error") == null &&
          valid.getAs[Map[String, String]]("answer")("value") == "\uD83D\uDE00")
        val status = AIPrompt.getOutputStatus(spark, root)
        assert(status.isComplete && !status.allSucceeded && status.rowErrorCount == 2)
        assert(stage.resume(spark).collect().toSet == rows.toSet)
        assert(AIPrompt.loadOutput(spark, root).collect().toSet == rows.toSet && server.requests.size() == 3)
      }
    }
  }

  test("durable JSON surrogate validation stores parse errors rather than corrupted successful answers") {
    delta()
    import spark.implicits._
    Using.resource(new AIRecordingServer((_, body) => {
      val escape = "\\" + "u"
      val content = "\"" + escape + (if (body.contains("invalid")) "d800" else s"d83d${escape}de00") + "\""
      200 -> s"""{"choices":[{"index":0,"message":{"role":"assistant","content":$content}}]}"""
    })) { server =>
      val root = path()
      val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setInputCol("text")
        .setOutputCol("answer").setErrorCol("error").setRawResponseCol("raw").setOutputPath(root)
      val result = stage.transform(Seq("invalid", "valid").toDF("text")).collect()
      val failed = result.find(_.getAs[String]("text") == "invalid").get
      assert(failed.getAs[String]("answer") == null)
      assert(failed.getAs[Row]("error").getString(0).contains("parse_error"))
      assert(failed.getAs[String]("raw").contains("\\ud800"))
      val valid = result.find(_.getAs[String]("text") == "valid").get
      assert(valid.getAs[Row]("error") == null && valid.getAs[String]("answer") == "\uD83D\uDE00")
      val status = AIPrompt.getOutputStatus(spark, root)
      assert(status.isComplete && !status.allSucceeded && status.rowErrorCount == 1)
      assert(stage.resume(spark).collect().toSet == result.toSet)
      assert(AIPrompt.loadOutput(spark, root).collect().toSet == result.toSet && server.requests.size() == 2)
    }
  }

  test("durable native UTF-8 failures remain terminal across interrupted recovery") {
    delta()
    import spark.implicits._
    val unicode = "caf\u00e9 \u96ea \uFFFD"
    val valid = s"""{"model":"model","answers":{"q":${JsString(unicode).compactPrint}}}"""
    Using.resource(new AIRecordingServer((count, _) =>
      200 -> (if (count == 1) """{"model":"model","answers":{"q":"private-value~"}}""" else valid),
      encodeResponse = text => text.getBytes(StandardCharsets.UTF_8)
        .map(byte => if (byte == '~'.toByte) 0xff.toByte else byte))) { server =>
      val root = path()
      val stage = new AISystemOne().setUrl(server.url).setModel("model").setStateCol("text")
        .setQuestions("""{"q":{"type":"noul","instructions":"Return an answer."}}""")
        .setSubscriptionKey("synthetic-key").setOutputCol("answer").setErrorCol("error")
        .setRawResponseCol("raw").setUsageCol("usage").setResponseIdCol("id")
        .setOutputPath(root).setOutputBatchSize(1)
      val data = Seq("one", "two", "three").toDF("text")
      intercept[IllegalStateException] {
        AIPromptOutput.transformObserved(stage, data, event =>
          if (event == "batch_committed") throw new IllegalStateException("stop after committed UTF-8 failure"))
      }
      val status = AIPrompt.getOutputStatus(spark, root)
      assert(status.committedCount == 1 && status.pendingCount == 2 && status.rowErrorCount == 1)
      assert(!status.allSucceeded && server.requests.size() == 1)
      val failed = AIPrompt.loadOutput(spark, root).head()
      assert(Seq("answer", "raw", "usage", "id").forall(field => failed.isNullAt(failed.fieldIndex(field))))
      assert(failed.getAs[Row]("error").getString(0).parseJson.asJsObject.fields("code") == JsString("parse_error"))
      AIPrompt.releaseOutputOwner(spark, root, status.ownerToken.get, confirmStopped = true)
      val resumed = stage.resume(spark)
      assert(resumed.schema == stage.transformSchema(data.schema))
      val rows = resumed.collect()
      assert(rows.length == 3 && rows.contains(failed))
      rows.filter(_.getAs[Row]("error") == null).foreach { row =>
        assert(row.getAs[String]("raw") == valid)
        assert(row.getAs[Row]("answer").getMap[String, String](1)("q") == JsString(unicode).compactPrint)
      }
      val complete = AIPrompt.getOutputStatus(spark, root)
      assert(complete.isComplete && !complete.allSucceeded && complete.rowErrorCount == 1)
      assert(complete.ownerToken.isEmpty && complete.pendingCount == 0)
      assert(stage.resume(spark).collect().toSet == rows.toSet)
      assert(AIPrompt.loadOutput(spark, root).collect().toSet == rows.toSet && server.requests.size() == 3)
    }
  }

  test("durable Decisions preserves exact score bounds in committed and resumed results") {
    delta()
    val session = spark
    import session.implicits._
    val scores = Seq("0", "2", "-1e-400", "2.0000000000000000001")
    def response(score: String): String =
      NativeRequestFixtures.DecisionResponse.replace("\"score\":1.7", s""""score":$score""")
    Using.resource(new AIRecordingServer((_, body) =>
      200 -> response(AIJson.objectString(body).fields("input").convertTo[String]))) { server =>
      val root = path()
      val stage = new AIPrompt().setApiType("decisions").setUrl(server.url + "/v1").setModel("model")
        .setInputCol("input").setQuestions(NativeRequestFixtures.DecisionQuestions)
        .setOutputCol("answer").setErrorCol("error").setRawResponseCol("raw")
        .setOutputPath(root).setOutputBatchSize(4)
      val result = stage.transform(scores.toDF("input"))
      val rows = result.collect()
      assert(rows.length == 4 && server.requests.size() == 4)
      rows.foreach { row =>
        val score = row.getAs[String]("input")
        assert(row.getAs[String]("raw") == response(score))
        if (score == "0" || score == "2") {
          assert(row.getAs[Row]("error") == null)
          assert(row.getAs[Row]("answer").getAs[Seq[Row]]("answers")(2).getAs[Double]("score") == score.toDouble)
        } else {
          assert(row.getAs[Row]("answer") == null)
          assert(row.getAs[Row]("error").getString(0).parseJson.asJsObject.fields("code") == JsString("parse_error"))
        }
      }
      val status = AIPrompt.getOutputStatus(spark, root)
      assert(status.rowErrorCount == 2 && !status.allSucceeded)
      assert(stage.resume(spark).collect().toSet == rows.toSet)
      assert(AIPrompt.loadOutput(spark, root).collect().toSet == rows.toSet)
      assert(server.requests.size() == 4)
    }
  }

  test("durable Chat empty refusal commits successful answers across interrupted recovery") {
    delta()
    import spark.implicits._
    val data = Seq("one", "two", "three").toDF("text")
    val reply =
      """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer","refusal":""},
        |"finish_reason":"stop"}]}""".stripMargin
    Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
      val root = path()
      val stage = AIPromptStorageTestConfig.configure(new AIPrompt(), root, server.url + "/v1/chat/completions")
      intercept[IllegalStateException] {
        AIPromptOutput.transformObserved(stage, data, event =>
          if (event == "batch_committed") throw new IllegalStateException("stop after committed answer"))
      }
      val status = AIPrompt.getOutputStatus(spark, root)
      assert(status.committedCount == 2 && status.pendingCount == 1 && status.rowErrorCount == 0)
      assert(server.requests.size() == 2)
      AIPrompt.releaseOutputOwner(spark, root, status.ownerToken.get, confirmStopped = true)
      val result = stage.resume(spark)
      assert(result.schema == stage.transformSchema(data.schema))
      val rows = result.collect()
      assert(rows.length == 3 && rows.forall(row =>
        row.getAs[Row]("error") == null && row.getAs[String]("answer") == "answer" &&
          row.getAs[String]("raw") == reply))
      assert(AIPrompt.getOutputStatus(spark, root).allSucceeded)
      assert(stage.resume(spark).count() == 3 && server.requests.size() == 3)
    }
  }

  test("durable templates require materialized encryption while lazy random IV encryption remains supported") {
    delta()
    import spark.implicits._
    val data = Seq.fill(2)("hello").toDF("text").withColumn("iv", lit("abcdefghijkl"))
    val encrypted = "base64(aes_encrypt(text, '0000111122223333'))"
    val reply =
      """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"},"finish_reason":"stop"}]}"""
    Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
      Seq(encrypted,
        "base64(aes_encrypt(text, '0000111122223333', 'CBC', 'DEFAULT', ''))",
        "base64(aes_encrypt(text, '0000111122223333', 'GCM', 'DEFAULT', iv))",
        "base64(aes_encrypt(text, '0000111122223333', 'GCM', 'DEFAULT', 'abcdefghijkl'))",
        "base64(aes_encrypt(text, '0000111122223333', 'ECB'))").foreach { expression =>
        val root = path()
        val stage = AIPromptStorageTestConfig.configure(new AIPrompt(), root,
          server.url + "/v1/chat/completions").setPromptTemplate("{" + expression + "}")
        val schemaError = intercept[IllegalArgumentException](stage.transformSchema(data.schema))
        val transformError = intercept[IllegalArgumentException](stage.transform(data))
        Seq(schemaError, transformError).foreach { error =>
          assert(error.getMessage.contains("stable built-in expressions") && error.getCause == null)
        }
        assert(!new File(root).exists() && server.requests.isEmpty)
      }
      val lazyStage = AIPromptStorageTestConfig.configure(new AIPrompt(), path(),
        server.url + "/v1/chat/completions").setPromptTemplate("{" + encrypted + "}")
      lazyStage.clear(lazyStage.outputPath)
      val result = lazyStage.transform(data)
      assert(result.schema == lazyStage.transformSchema(data.schema))
      assert(result.collect().forall(_.getAs[Row]("error") == null))
      val ciphertexts = server.requests.asScala.map { case (_, body) =>
        AIJson.objectString(body).fields("messages").asInstanceOf[JsArray].elements.last
          .asJsObject.fields("content").asInstanceOf[JsString].value
      }.toVector
      assert(ciphertexts.size == 2 && ciphertexts.distinct.size == 2)
    }
  }

  test("durable recovery retains materialized random IV ciphertext without resending committed rows") {
    delta()
    import spark.implicits._
    val data = Seq.fill(3)("hello").toDF("text")
      .withColumn("ciphertext", expr("base64(aes_encrypt(text, '0000111122223333'))"))
    val reply =
      """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"},"finish_reason":"stop"}]}"""
    Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
      val root = path()
      val stage = AIPromptStorageTestConfig.configure(new AIPrompt(), root,
        server.url + "/v1/chat/completions").setPromptTemplate("{ciphertext}")
      intercept[IllegalStateException] {
        AIPromptOutput.transformObserved(stage, data, event =>
          if (event == "batch_committed") throw new IllegalStateException("stop after committed ciphertext"))
      }
      val status = AIPrompt.getOutputStatus(spark, root)
      assert(status.committedCount == 2 && status.pendingCount == 1 && server.requests.size() == 2)
      AIPrompt.releaseOutputOwner(spark, root, status.ownerToken.get, confirmStopped = true)
      val result = stage.resume(spark)
      assert(result.schema == stage.transformSchema(data.schema))
      val rows = result.collect()
      assert(rows.length == 3 && rows.forall(_.getAs[Row]("error") == null))
      val ciphertexts = server.requests.asScala.map { case (_, body) =>
        AIJson.objectString(body).fields("messages").asInstanceOf[JsArray].elements.last
          .asJsObject.fields("content").asInstanceOf[JsString].value
      }.toVector
      assert(ciphertexts.size == 3 && ciphertexts.distinct.size == 3)
      assert(ciphertexts.sorted == rows.map(_.getAs[String]("ciphertext")).sorted.toVector)
      assert(stage.resume(spark).count() == 3 && server.requests.size() == 3)
      assert(AIPrompt.getOutputStatus(spark, root).allSucceeded)
    }
  }

  Seq(false, true).foreach { perRow =>
    test(s"durable Responses output-only include survives interrupted recovery with perRow=$perRow") {
      delta()
      import spark.implicits._
      val requested = Seq("message.output_text.logprobs", "reasoning.encrypted_content")
      val extension = JsObject("include" -> JsArray(requested.map(JsString(_)).toVector))
      val data = Seq("one", "two", "three").toDF("text").withColumn("body", lit(extension.compactPrint))
      val reply =
        """{"status":"completed","output":[{"type":"message","role":"assistant",
          |"content":[{"type":"output_text","text":"answer",
          |"logprobs":[{"token":"answer","logprob":-0.1}]}]},
          |{"type":"reasoning","encrypted_content":"synthetic-encrypted-reasoning"}],
          |"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}""".stripMargin
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val root = path()
        val stage = AIPromptStorageTestConfig.configure(new AIPrompt(), root, server.url + "/v1/responses")
          .setApiType("responses")
        if (perRow) stage.setExtraBodyCol("body") else stage.setExtraBody(Map("include" -> requested))
        intercept[IllegalStateException] {
          AIPromptOutput.transformObserved(stage, data, event =>
            if (event == "batch_committed") throw new IllegalStateException("stop after committed include output"))
        }
        val status = AIPrompt.getOutputStatus(spark, root)
        assert(status.committedCount == 2 && status.pendingCount == 1 && server.requests.size() == 2)
        AIPrompt.releaseOutputOwner(spark, root, status.ownerToken.get, confirmStopped = true)
        val result = stage.resume(spark)
        assert(result.schema == stage.transformSchema(data.schema))
        val rows = result.collect()
        assert(rows.length == 3 && rows.forall(row =>
          row.getAs[Row]("error") == null && row.getAs[String]("answer") == "answer" &&
            row.getAs[String]("raw") == reply))
        assert(server.requests.size() == 3)
        server.requests.asScala.foreach { case (url, body) =>
          assert(url == "/v1/responses")
          assert(AIJson.objectString(body).fields("include") == extension.fields("include"))
        }
        assert(stage.resume(spark).count() == 3 && server.requests.size() == 3)
        assert(AIPrompt.getOutputStatus(spark, root).allSucceeded)
      }
    }
  }
}
