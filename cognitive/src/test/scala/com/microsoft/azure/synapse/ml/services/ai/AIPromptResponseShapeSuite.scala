// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import spray.json._

import java.util.Collections
import scala.collection.JavaConverters._
import scala.util.Using

class AIPromptResponseShapeSuite extends AIPromptTestBase {
  Seq("chat_completions", "responses").foreach { api =>
    test(s"$api JSON NOT NULL fields have nullable output schemas") {
      val text = """{"value":7,"nested":{},"items":[{},null],"lookup":{"key":{}}}"""
      val encoded = JsString(text).compactPrint
      val reply = if (api == "chat_completions") {
        s"""{"choices":[{"index":0,"message":{"role":"assistant","content":$encoded}}]}"""
      } else {
        s"""{"status":"completed","output":[{"type":"message","role":"assistant",
           |"content":[{"type":"output_text","text":$encoded}]}]}""".stripMargin
      }
      val nested = new StructType().add("n", IntegerType)
      val expected = new StructType().add("value", IntegerType).add("nested", nested)
        .add("items", ArrayType(nested)).add("lookup", MapType(StringType, nested))
      val data = spark.createDataFrame(Collections.singletonList(Row("hello")),
        new StructType().add("text", StringType))
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
          .setInputCol("text").setOutputCol("answer").setErrorCol("error")
          .setPostProcessingOptions(Map("jsonSchema" ->
            """value INT NOT NULL, nested STRUCT<n: INT NOT NULL> NOT NULL,
              |items ARRAY<STRUCT<n: INT NOT NULL>>, lookup MAP<STRING, STRUCT<n: INT NOT NULL>>""".stripMargin))
        assert(stage.transformSchema(data.schema)("answer").dataType == expected)
        val result = stage.transform(data)
        assert(result.schema == stage.transformSchema(data.schema))
        val row = result.head()
        val answer = row.getAs[Row]("answer")
        assert(row.getAs[Row]("error") == null && answer.getInt(0) == 7)
        assert(answer.getStruct(1).isNullAt(0))
        assert(answer.getSeq[Row](2).head.isNullAt(0) && answer.getSeq[Row](2).last == null)
        assert(answer.getMap[String, Row](3)("key").isNullAt(0))
        assert(server.requests.size() == 1)
      }
    }

    test(s"$api failed projections retain partial observations and provider diagnostics") {
      import spark.implicits._
      val key = if (api == "chat_completions") "choices" else "output"
      val valid = if (api == "chat_completions") {
        """{"index":0,"message":{"role":"assistant","content":"partial"}}"""
      } else {
        """{"type":"message","role":"assistant","content":[{"type":"output_text","text":"partial"},
          |{"type":"output_text","text":42},{"type":"output_text","text":" tail"}]}""".stripMargin
      }
      val projections = Map("NULL_CASE" -> "null", "WRONG_CASE" -> "42",
        "MIXED_CASE" -> s"[$valid,null]", "FIRST_BAD_CASE" -> s"[null,$valid]")
      val input = projections.keys.toSeq.toDF("text")
      Using.resource(new AIRecordingServer((_, body) => {
        val name = projections.keys.find(body.contains).get
        200 -> s"""{"id":"failed-id","error":{"code":"failed","message":"private provider detail"},
          |"$key":${projections(name)},"usage":{"total_tokens":7}}""".stripMargin
      })) { server =>
        val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
          .setInputCol("text").setOutputCol("answer").setErrorCol("error").setUsageCol("usage")
          .setResponseIdCol("response_id").setRawResponseCol("raw")
        val output = stage.transform(input)
        assert(output.schema == stage.transformSchema(input.schema))
        val rows = output.collect()
        assert(rows.length == projections.size)
        rows.foreach { row =>
          val name = row.getAs[String]("text")
          val expected = if (name == "MIXED_CASE" || (api == "responses" && name == "FIRST_BAD_CASE")) {
            if (api == "chat_completions") "partial" else "partial tail"
          } else None.orNull
          assert(row.getAs[String]("answer") == expected)
          assert(row.getAs[String]("response_id") == "failed-id" && row.getAs[Row]("usage").getLong(2) == 7L)
          val error = row.getAs[Row]("error")
          assert(error != null && error.getString(0).contains("provider_error"))
          assert(!error.getString(0).contains("private provider detail"))
          assert(row.getAs[String]("raw").contains("private provider detail"))
        }
        assert(server.requests.size() == 4)
      }
    }
  }

  Seq("responses", "decisions").foreach { api =>
    test(s"$api message type tags validate before HTTP") {
      val reply = if (api == "decisions") NativeRequestFixtures.DecisionResponse else
        """{"status":"completed","output":[{"type":"message","role":"assistant",
          |"content":[{"type":"output_text","text":"answer"}]}]}""".stripMargin
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val message = new StructType().add("role", StringType).add("content", StringType).add("type", StringType)
        val schema = new StructType().add("tag", StringType).add("messages", ArrayType(message))
        val data = spark.createDataFrame(Seq("message", "unsupported", "", Option.empty[String].orNull)
          .map(tag => Row(tag, Seq(Row("user", "hello", tag)))).asJava, schema)
        val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
          .setMessagesCol("messages").setOutputCol("answer").setErrorCol("error")
        if (api == "decisions") stage.setQuestions(NativeRequestFixtures.DecisionQuestions)
        val result = stage.transform(data)
        assert(result.schema == stage.transformSchema(data.schema))
        result.collect().foreach { row =>
          if (row.isNullAt(0) || row.getString(0) == "message") {
            assert(row.getAs[Row]("error") == null && !row.isNullAt(row.fieldIndex("answer")))
          } else assert(row.getAs[Row]("error").getString(0).contains("invalid_request"))
        }
        val invalidType = spark.createDataFrame(Collections.singletonList(Row(Seq(Row("user", "hello", 1)))),
          new StructType().add("messages", ArrayType(
            new StructType().add("role", StringType).add("content", StringType).add("type", IntegerType))))
        assert(stage.transform(invalidType).head().getAs[Row]("error").getString(0).contains("invalid_request"))
        assert(server.requests.size() == 2)
      }
    }
  }

  test("nullable embedding object tags preserve vectors and diagnostics") {
    import spark.implicits._
    val input = Seq("hello").toDF("text")
    val reply = """{"id":"embedding-id","object":null,"data":[{"object":null,"index":0,"embedding":[0.25,-0.5]}],
      |"usage":{"prompt_tokens":2,"total_tokens":2}}""".stripMargin
    Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
      val stage = new AIPrompt().setApiType("embeddings").setUrl(server.url + "/v1").setModel("model")
        .setInputCol("text").setOutputCol("answer").setErrorCol("error").setUsageCol("usage")
        .setResponseIdCol("response_id").setRawResponseCol("raw")
      val output = stage.transform(input)
      assert(output.schema == stage.transformSchema(input.schema))
      val row = output.head()
      assert(row.getAs[org.apache.spark.ml.linalg.Vector]("answer") == Vectors.dense(0.25, -0.5))
      assert(row.getAs[Row]("error") == null && row.getAs[Row]("usage").getLong(2) == 2L)
      assert(row.getAs[String]("response_id") == "embedding-id" && row.getAs[String]("raw") == reply)
      assert(server.requests.size() == 1)
    }
  }

  test("Responses in-progress messages retain partial observations and incomplete errors") {
    import spark.implicits._
    val input = Seq("absent", "completed").toDF("text")
    Using.resource(new AIRecordingServer((_, body) => {
      val status = if (body.contains("completed")) """"status":"completed",""" else ""
      200 -> s"""{$status"id":"partial-id","output":[{"type":"message","role":"assistant",
        |"status":"in_progress","content":[{"type":"output_text","text":"partial"}]}],
        |"usage":{"input_tokens":2,"output_tokens":1,"total_tokens":3}}""".stripMargin
    })) { server =>
      val stage = new AIPrompt().setApiType("responses").setUrl(server.url + "/v1").setModel("model")
        .setInputCol("text").setOutputCol("answer").setErrorCol("error").setUsageCol("usage")
        .setResponseIdCol("response_id").setRawResponseCol("raw")
      val output = stage.transform(input)
      assert(output.schema == stage.transformSchema(input.schema))
      val rows = output.collect()
      assert(rows.length == 2)
      rows.foreach { row =>
        assert(row.getAs[String]("answer") == "partial" && row.getAs[String]("response_id") == "partial-id")
        assert(row.getAs[Row]("usage").getLong(2) == 3L && row.getAs[String]("raw").contains("in_progress"))
        assert(Option(row.getAs[Row]("error")).exists(_.getString(0).contains("incomplete")))
      }
      assert(server.requests.size() == 2)
    }
  }
}
