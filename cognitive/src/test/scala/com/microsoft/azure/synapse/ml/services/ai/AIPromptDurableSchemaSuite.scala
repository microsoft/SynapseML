// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.io.http.HandlingUtils
import com.microsoft.azure.synapse.ml.services.openai.OpenAIDecisions
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.linalg.{SQLDataTypes, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{ArrayType, CalendarIntervalType, IntegerType,
  MapType, NullType, StringType, StructType}
import spray.json.{JsArray, JsNull, JsObject, JsString}

import java.io.File
import scala.collection.JavaConverters._
import scala.util.Using

class AIPromptDurableSchemaSuite extends AIPromptOutputTestBase {
  private val inputSchema = new StructType().add("text", StringType)
    .add("credential", StringType).add("headers", MapType(StringType, StringType))

  Seq(false, true).foreach { execution =>
    test(s"durable preflight rejects custom default handlers with execution=$execution") {
      if (execution) delta()
      withServer { server =>
        val root = path()
        val stage = new TestableAIAlternateDefaultHandlerPrompt("alternate")
          .setUrl(server.url).setModel("model").setInputCol("text").setOutputPath(root)
        val data = spark.range(1).select(lit("hello").as("text"))
        assert(!stage.isSet(stage.handler))
        val failure = intercept[IllegalArgumentException] {
          if (execution) stage.transform(data) else stage.transformSchema(data.schema)
        }
        assert(failure.getMessage.contains("default HTTP handler"))
        intercept[IllegalArgumentException] {
          AIPromptOutput.fingerprint(stage, data.schema, data.schema, spark)
        }
        assert(!new File(root).exists() && server.requests.get() == 0)
      }
    }
  }

  Seq(true).foreach { durable =>
    test(s"non-row-local templates are rejected before inference or storage with durable=$durable") {
      val data = spark.range(2).selectExpr("id",
        "CASE WHEN id = 0 THEN CAST('2020-01-01 00:00:00' AS TIMESTAMP) END AS ts")
      withServer { server =>
        Seq("{cast(window(ts, '1 hour').start as string)}",
          "{cast(window(ts, '1 hour', '30 minutes').start as string)}",
          "{cast(session_window(ts, '1 hour').start as string)}",
          "{first(cast(id as string))}", "{row_number() over (order by id)}").foreach { template =>
          val root = path()
          val stage = prompt(root, server).setPromptTemplate(template)
          if (!durable) stage.clear(stage.outputPath)
          val schemaError = intercept[IllegalArgumentException](stage.transformSchema(data.schema))
          val transformError = intercept[IllegalArgumentException](stage.transform(data))
          Seq(schemaError, transformError).foreach { error =>
            assert(error.getMessage.contains("row-local") && Option(error.getCause).isEmpty)
          }
          assert(!new File(root).exists() && server.requests.get() == 0)
        }
      }
    }
  }

  Seq(
    ("to_json(map(*))", Seq("concat('key-', id) AS first", "cast(id as string) AS second")),
    ("nvl(*)", Seq("CASE WHEN id = 0 THEN 'first' END AS first", "cast(id as string) AS second")),
    ("cast(coalesce(*) as string)", Seq(
      "CASE WHEN id = 0 THEN DATE '2020-01-01' END AS first", "DATE '2020-01-02' AS second"))
  ).foreach { case (expression, columns) =>
    Seq("complete", "resume").foreach { mode =>
      test(s"wildcard template $expression validates only public columns in $mode mode") {
        if (mode != "lazy") delta()
        val data = spark.range(3).selectExpr(columns: _*)
        val expected = data.selectExpr(expression).collect().map(_.getString(0)).toVector.sorted
        val reply = """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""
        Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
          val root = path()
          val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model")
            .setPromptTemplate(s"{$expression}").setOutputCol("answer").setErrorCol("error")
          if (mode != "lazy") stage.setOutputPath(root).setOutputBatchSize(1)
          val expectedSchema = stage.transformSchema(data.schema)
          if (mode == "resume") {
            intercept[IllegalStateException] {
              AIPromptOutput.transformObserved(stage, data, event =>
                if (event == "batch_committed") throw new IllegalStateException("stop after committed batch"))
            }
            val status = AIPrompt.getOutputStatus(spark, root)
            assert(status.committedCount == 1 && status.pendingCount == 2 && server.requests.size() == 1)
            AIPrompt.releaseOutputOwner(spark, root, status.ownerToken.get, confirmStopped = true)
          }
          val output = if (mode == "resume") stage.resume(spark) else stage.transform(data)
          val rows = output.collect()
          assert(output.schema == expectedSchema && rows.length == 3)
          assert(rows.forall(row => row.getAs[String]("answer") == "answer" && row.getAs[Row]("error") == null))
          assert(rows.map(row => row.toSeq.take(2)).toSet == data.collect().map(_.toSeq).toSet)
          val contents = server.requests.asScala.toVector.map { case (_, body) =>
            AIJson.objectString(body).fields("messages").asInstanceOf[JsArray].elements.last
              .asJsObject.fields("content").asInstanceOf[JsString].value
          }.sorted
          assert(contents == expected && server.requests.size() == 3)
          if (mode != "lazy") {
            assert(AIPrompt.getOutputStatus(spark, root).allSucceeded)
            assert(stage.resume(spark).collect().toSet == rows.toSet && server.requests.size() == 3)
          }
        }
      }
    }
  }

  test("lazy and durable templates preserve precomputed window columns and null input rows") {
    delta()
    val data = spark.range(2).selectExpr("id", "row_number() over (order by id) AS ordinal",
      "CASE WHEN id = 0 THEN CAST('2020-01-01 00:00:00' AS TIMESTAMP) END AS ts")
    withServer { server =>
      Seq(true).foreach { durable =>
        val stage = prompt(path(), server)
          .setPromptTemplate("{coalesce(cast(ts as string), 'missing')}:{cast(ordinal as string)}")
        if (!durable) stage.clear(stage.outputPath)
        val output = stage.transform(data)
        assert(output.schema == stage.transformSchema(data.schema))
        val rows = output.collect().sortBy(_.getAs[Long]("id"))
        assert(rows.map(_.getAs[Int]("ordinal")).toSeq == Seq(1, 2))
        assert(rows(1).isNullAt(rows(1).fieldIndex("ts")))
        assert(rows.forall(row => row.getAs[String]("answer") == "answer" && row.getAs[Row]("error") == null))
      }
      assert(server.requests.get() == 4)
    }
  }

  test("fused JVM message arrays preserve lazy and durable outcomes") {
    delta()
    val messageType = new StructType().add("role", StringType)
      .add("content", ArrayType(MapType(StringType, StringType)))
    val schema = new StructType().add("id", "long").add("messages", ArrayType(messageType))
    val input = spark.range(2).map { id =>
      val message = if (id == 0) Row("user", Array(Map("type" -> "text", "text" -> "hello")))
      else Option.empty[Row].orNull
      Row(id, Array(message))
    }(Encoders.row(schema))
    val expected = input.repartition(1).collect().map(row => row.getLong(0) -> row).toMap
    val reply = """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""
    Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
      val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setMessagesCol("messages")
        .setOutputCol("answer").setErrorCol("error")
      Seq(true).foreach { durable =>
        if (durable) stage.setOutputPath(path())
        val output = stage.transform(input)
        assert(output.schema == stage.transformSchema(schema))
        val rows = output.collect().map(row => row.getLong(0) -> row).toMap
        assert(rows(0L).getAs[String]("answer") == "answer" && rows(0L).getAs[Row]("error") == null)
        assert(rows(1L).getAs[Row]("error").getString(0).contains("invalid_request"))
        rows.foreach { case (id, row) =>
          assert(row.getAs[Seq[Row]]("messages") == expected(id).getAs[Seq[Row]]("messages"))
        }
        assert(server.requests.size() == 1)
        val body = AIJson.objectString(server.requests.remove()._2)
        assert(body.fields("messages").asInstanceOf[JsArray].elements.head.asJsObject
          .fields("content") == JsString("hello"))
        if (durable) {
          val status = AIPrompt.getOutputStatus(spark, stage.getOutputPath)
          assert(status.isComplete && status.rowErrorCount == 1)
          assert(stage.resume(spark).count() == 2 && server.requests.isEmpty)
        }
      }
    }
  }

  test("fused JVM storage arrays preserve vectors nulls and nested values") {
    val schema = new StructType().add("vectors", ArrayType(SQLDataTypes.VectorType))
      .add("nested", MapType(StringType, ArrayType(StringType))).add("numbers", ArrayType(IntegerType))
    val input = spark.range(1).map { _ =>
      Row(Array(Vectors.sparse(3, Array(1), Array(0.5)), Option.empty[org.apache.spark.ml.linalg.Vector].orNull),
        Map("key" -> Array("value", Option.empty[String].orNull)), Array(1, 2))
    }(Encoders.row(schema))
    val expected = input.repartition(1).collect().toSeq
    val output = AIPromptStorageCodec.decode(AIPromptStorageCodec.encode(input, schema), schema)
    assert(output.schema == schema && output.collect().toSeq == expected)
  }

  Seq("chat_completions", "responses").foreach { api =>
    test(s"$api failed projections retain partial observations and terminal durable provider errors") {
      delta()
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
        Seq(true).foreach { durable =>
          if (durable) stage.setOutputPath(path())
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
          assert(server.requests.size() == (if (durable) 8 else 4))
          if (durable) {
            val status = AIPrompt.getOutputStatus(spark, stage.getOutputPath)
            assert(status.isComplete && !status.allSucceeded && status.rowErrorCount == 4)
            assert(stage.resume(spark).collect().toSet == rows.toSet && server.requests.size() == 8)
          }
        }
      }
    }
  }

  test("nullable embedding object tags preserve lazy and durable vectors without replay") {
    delta()
    import spark.implicits._
    val input = Seq("hello").toDF("text")
    val reply = """{"id":"embedding-id","object":null,"data":[{"object":null,"index":0,"embedding":[0.25,-0.5]}],
      |"usage":{"prompt_tokens":2,"total_tokens":2}}""".stripMargin
    Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
      val stage = new AIPrompt().setApiType("embeddings").setUrl(server.url + "/v1").setModel("model")
        .setInputCol("text").setOutputCol("answer").setErrorCol("error").setUsageCol("usage")
        .setResponseIdCol("response_id").setRawResponseCol("raw")
      Seq(true).foreach { durable =>
        if (durable) stage.setOutputPath(path())
        val output = stage.transform(input)
        assert(output.schema == stage.transformSchema(input.schema))
        val row = output.head()
        assert(row.getAs[org.apache.spark.ml.linalg.Vector]("answer") == Vectors.dense(0.25, -0.5))
        assert(row.getAs[Row]("error") == null && row.getAs[Row]("usage").getLong(2) == 2L)
        assert(row.getAs[String]("response_id") == "embedding-id" && row.getAs[String]("raw") == reply)
        assert(server.requests.size() == (if (durable) 2 else 1))
        if (durable) {
          assert(AIPrompt.getOutputStatus(spark, stage.getOutputPath).allSucceeded)
          assert(stage.resume(spark).head() == row && server.requests.size() == 2)
        }
      }
    }
  }

  test("Responses in-progress messages retain partial observations and terminal durable errors") {
    delta()
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
      Seq(true).foreach { durable =>
        if (durable) stage.setOutputPath(path())
        val output = stage.transform(input)
        assert(output.schema == stage.transformSchema(input.schema))
        val rows = output.collect()
        assert(rows.length == 2)
        rows.foreach { row =>
          assert(row.getAs[String]("answer") == "partial" && row.getAs[String]("response_id") == "partial-id")
          assert(row.getAs[Row]("usage").getLong(2) == 3L && row.getAs[String]("raw").contains("in_progress"))
          assert(Option(row.getAs[Row]("error")).exists(_.getString(0).contains("incomplete")))
        }
        assert(server.requests.size() == (if (durable) 4 else 2))
        if (durable) {
          val status = AIPrompt.getOutputStatus(spark, stage.getOutputPath)
          assert(status.isComplete && !status.allSucceeded && status.rowErrorCount == 2)
          assert(stage.resume(spark).collect().toSet == rows.toSet && server.requests.size() == 4)
        }
      }
    }
  }

  test("durable Responses preserves literal file_data and never replays committed provider errors") {
    delta()
    val raw = "JVBERi0xLjQK"
    val inline = "data:application/pdf;base64," + raw
    val contents = Seq(raw, inline, "not base64", "https://example.invalid/not-a-location")
    val messageType = new StructType().add("role", StringType)
      .add("content", ArrayType(MapType(StringType, StringType)))
    val input = spark.createDataFrame(contents.map { text =>
      Row(text, Seq(Row("user", Seq(Map("type" -> "input_file", "filename" -> "note.pdf", "file_data" -> text)))))
    }.asJava, new StructType().add("text", StringType).add("messages", ArrayType(messageType))).coalesce(1)
    def fileData(body: String): String = {
      val message = AIJson.objectString(body).fields("input").asInstanceOf[JsArray].elements.head.asJsObject
      message.fields("content").asInstanceOf[JsArray].elements.head.asJsObject
        .fields("file_data").asInstanceOf[JsString].value
    }
    val reply = """{"status":"completed","output":[{"type":"message","role":"assistant",
      |"content":[{"type":"output_text","text":"answer"}]}]}""".stripMargin
    Using.resource(new AIRecordingServer((_, body) => {
      if (Set(raw, inline)(fileData(body))) 200 -> reply
      else 400 -> """{"error":{"code":"invalid_file","message":"Unsupported inline file content"}}"""
    })) { server =>
      val stage = new AIPrompt().setApiType("responses").setUrl(server.url + "/v1").setModel("model")
        .setMessagesCol("messages").setOutputCol("answer").setErrorCol("error")
      val lazyRows = stage.transform(input).collect().map(row => row.getAs[String]("text") -> row).toMap
      assert(lazyRows.size == 4 && lazyRows.values.count(_.getAs[Row]("error") != null) == 2)
      assert(server.requests.asScala.map(entry => fileData(entry._2)).toSeq.sorted == contents.sorted)
      server.requests.clear()
      val root = path()
      stage.setOutputPath(root).setOutputBatchSize(2)
      intercept[IllegalStateException] {
        AIPromptOutput.transformObserved(stage, input, event =>
          if (event == "batch_committed") throw new IllegalStateException("stop after committed batch"))
      }
      val partial = AIPrompt.getOutputStatus(spark, root)
      assert(partial.committedCount == 2 && partial.pendingCount == 2 && server.requests.size() == 2)
      AIPrompt.releaseOutputOwner(spark, root, partial.ownerToken.get, confirmStopped = true)
      val output = stage.resume(spark)
      assert(output.schema == stage.transformSchema(input.schema))
      val rows = output.collect().map(row => row.getAs[String]("text") -> row).toMap
      assert(rows.keySet == lazyRows.keySet)
      rows.foreach { case (text, row) =>
        assert(row.getAs[String]("answer") == lazyRows(text).getAs[String]("answer"))
        assert(row.getAs[Row]("error") == lazyRows(text).getAs[Row]("error"))
      }
      val complete = AIPrompt.getOutputStatus(spark, root)
      assert(complete.isComplete && complete.rowErrorCount == 2 && complete.ownerToken.isEmpty)
      assert(stage.resume(spark).count() == 4 && server.requests.size() == 4)
      assert(server.requests.asScala.map(entry => fileData(entry._2)).toSeq.sorted == contents.sorted)
    }
  }

  Seq(false, true).foreach { perRow =>
    test(s"durable body extensions preserve literal file_data with perRow=$perRow") {
      delta()
      import spark.implicits._
      val extension = Map("extension" -> Map("file_data" -> "JVBERi0xLjQK"))
      val input = Seq("one", "two").toDF("text").withColumn("body", lit(AIJson.fromAny(extension).compactPrint))
      val reply = """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val root = path()
        val stage = AIPromptStorageTestConfig.configure(new AIPrompt(), root, server.url + "/v1")
        if (perRow) stage.setExtraBodyCol("body") else stage.setExtraBody(extension)
        val output = stage.transform(input)
        assert(output.collect().forall(_.getAs[Row]("error") == null))
        assert(AIPrompt.getOutputStatus(spark, root).allSucceeded)
        assert(stage.resume(spark).count() == 2 && server.requests.size() == 2)
        server.requests.asScala.foreach { case (_, body) =>
          assert(AIJson.objectString(body).fields("extension") == AIJson.fromAny(extension("extension")))
        }
      }
    }
  }

  test("durable container JSON conversion failures remain terminal row errors on resume") {
    delta()
    import spark.implicits._
    Seq("chat_completions", "responses").foreach { api =>
      Using.resource(new AIRecordingServer((_, body) => {
        val text = if (body.contains("incompatible")) """[{"value":"wrong"}]"""
        else if (body.contains("bad-singleton")) """{"value":"wrong"}"""
        else if (body.contains("singleton")) """{"value":1}"""
        else """[{"value":null}]"""
        val quoted = JsString(text).compactPrint
        val reply = if (api == "chat_completions") {
          s"""{"choices":[{"index":0,"message":{"role":"assistant","content":$quoted},"finish_reason":"stop"}]}"""
        } else {
          s"""{"status":"completed","output":[{"type":"message","role":"assistant",
             |"content":[{"type":"output_text","text":$quoted}]}]}""".stripMargin
        }
        200 -> reply
      })) { server =>
        val root = path()
        val stage = AIPromptStorageTestConfig.configure(new AIPrompt(), root, server.url + "/v1")
          .setApiType(api).setPostProcessingOptions(Map("jsonSchema" -> "ARRAY<STRUCT<value: INT>>"))
        val data = Seq("incompatible", "valid", "singleton", "bad-singleton").toDF("text")
        val output = stage.transform(data)
        assert(output.schema == stage.transformSchema(data.schema))
        val rows = output.collect().map(row => row.getAs[String]("text") -> row).toMap
        Seq("incompatible", "bad-singleton").foreach { name =>
          assert(rows(name).getAs[Row]("error").getString(0).contains("parse_error"))
        }
        Seq("valid", "singleton").foreach(name => assert(rows(name).getAs[Row]("error") == null))
        assert(rows("valid").getAs[Seq[Row]]("answer").head.isNullAt(0))
        assert(rows("singleton").getAs[Seq[Row]]("answer") == Seq(Row(1)))
        val status = AIPrompt.getOutputStatus(spark, root)
        assert(status.rowErrorCount == 2 && !status.allSucceeded && status.pendingCount == 0)
        val resumed = stage.resume(spark).collect().map(row => row.getAs[String]("text") -> row).toMap
        assert(resumed == rows && server.requests.size() == 4)
      }
    }
  }

  test("Decisions null image options normalize through lazy and durable direct and facade stages") {
    delta()
    import spark.implicits._
    Seq(new AIPrompt().setApiType("decisions"), new OpenAIDecisions()).foreach { stage =>
      Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
        val image = Map("type" -> JsString("input_image"), "image_url" -> JsString("data:image/png;base64,AQID"),
          "provider_extension" -> JsNull)
        val variants = Seq(Map("file_id" -> JsNull), Map("detail" -> JsNull),
          Map("file_id" -> JsNull, "detail" -> JsNull))
        val data = variants.map { fields =>
          JsArray(JsObject("role" -> JsString("user"),
            "content" -> JsArray(JsObject(image ++ fields)))).compactPrint
        }.toDF("text")
        stage.setUrl(server.url + "/v1").setModel("model").setInputCol("text").setInputMode("json")
          .setQuestions(NativeRequestFixtures.DecisionQuestions).setOutputCol("answer").setErrorCol("error")
        Seq(true).foreach { durable =>
          if (durable) stage.setOutputPath(path()).setOutputBatchSize(3)
          val output = stage.transform(data)
          assert(output.schema == stage.transformSchema(data.schema))
          val rows = output.collect()
          assert(rows.length == 3 && rows.forall(_.getAs[Row]("error") == null))
          assert(rows.forall(_.getAs[Row]("answer") != null))
          val sends = if (durable) 6 else 3
          assert(server.requests.size() == sends)
          server.requests.asScala.foreach { case (_, body) =>
            val message = AIJson.objectString(body).fields("input").asInstanceOf[JsArray].elements.head.asJsObject
            val part = message.fields("content").asInstanceOf[JsArray].elements.head.asJsObject
            assert(!part.fields.contains("file_id") && !part.fields.contains("detail"))
            assert(part.fields("provider_extension") == JsNull)
          }
          if (durable) {
            val status = AIPrompt.getOutputStatus(spark, stage.getOutputPath)
            assert(status.allSucceeded && status.ownerToken.isEmpty)
            assert(stage.resume(spark).count() == 3 && server.requests.size() == sends)
          }
        }
      }
    }
  }

  test("durable recovery rotates custom authentication hooks but fingerprints ordinary header values") {
    delta()
    val data = spark.createDataFrame(Seq(Row("first"), Row("last")).asJava,
      new StructType().add("text", StringType))
    val reply = """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""
    Seq("x-model-key", "x-model-token").foreach { authName =>
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val root = path()
        val first = new TestableAIHeaderPrompt("durable-custom-auth").setUrl(server.url + "/v1")
          .setModel("model").setInputCol("text").setOutputCol("answer").setErrorCol("error")
          .setOutputPath(root).setOutputBatchSize(1)
          .setCustomHeaders(Map(authName -> "synthetic-first", "api-key" -> "ordinary"))
          .setTelemHeaders(Map(authName -> "ignored-first", "Authorization" -> "ordinary"))
        intercept[IllegalStateException] {
          AIPromptOutput.transformObserved(first, data, event =>
            if (event == "batch_committed") throw new IllegalStateException("stop after committed batch"))
        }
        val status = AIPrompt.getOutputStatus(spark, root)
        assert(status.committedCount == 1 && status.pendingCount == 1 && server.requests.size() == 1)
        AIPrompt.releaseOutputOwner(spark, root, status.ownerToken.get, confirmStopped = true)
        val second = first.copy(ParamMap.empty)
          .setCustomHeaders(Map(authName.toUpperCase(java.util.Locale.ROOT) -> "synthetic-rotated",
            "api-key" -> "ordinary"))
          .setTelemHeaders(Map(authName -> "ignored-rotated", "Authorization" -> "ordinary"))
        val result = second.resume(spark)
        assert(result.collect().forall(_.getAs[Row]("error") == null))
        assert(result.count() == 2 && server.requests.size() == 2)
        assert(server.headers.asScala.last(authName) == "synthetic-rotated")
        assert(second.resume(spark).count() == 2 && server.requests.size() == 2)
        second.setCustomHeaders(Map(authName -> "synthetic-rotated", "api-key" -> "changed"))
        intercept[IllegalArgumentException](second.resume(spark))
        second.setCustomHeaders(Map(authName -> "synthetic-rotated", "api-key" -> "ordinary"))
          .setTelemHeaders(Map("Authorization" -> "changed"))
        intercept[IllegalArgumentException](second.resume(spark))
        assert(server.requests.size() == 2 && AIPrompt.getOutputStatus(spark, root).ownerToken.isEmpty)
      }
    }
  }

  test("durable scalar header preflight rejects invalid configuration before storage and allows correction") {
    delta()
    val data = spark.createDataFrame(Seq(Row("hello")).asJava, new StructType().add("text", StringType))
    val reply = """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""
    Seq("ap\u0131-key" -> "api-key", "author\u0131zation" -> "Authorization").foreach { case (alias, canonical) =>
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model")
          .setInputCol("text").setOutputCol("answer").setErrorCol("error").setOutputPath(path())
          .setCustomHeaders(Map(alias -> "synthetic-first"))
        val invalid: Seq[AIPrompt => AIPrompt] = Seq(
          _.setCustomHeaders(Map(alias -> "synthetic-private")),
          _.setTelemHeaders(Map(alias -> "synthetic-private")),
          _.setCustomHeaders(Map("X-Caller" -> "synthetic-private\n")),
          _.setTelemHeaders(Map("X-Caller" -> "synthetic-private\n")),
          _.setSubscriptionKey("synthetic-private\n"), _.setAADToken("synthetic-private\n"),
          _.setCustomAuthHeader("synthetic-private\n"))
        stage.clear(stage.customHeaders)
        invalid.foreach { configure =>
          val candidate = configure(stage.copy(ParamMap.empty))
          Seq(
            intercept[IllegalArgumentException](candidate.transformSchema(data.schema)),
            intercept[IllegalArgumentException](candidate.transform(data))
          ).foreach { error =>
            assert(error.getMessage.contains("Invalid HTTP header") && !error.getMessage.contains("synthetic-private"))
          }
          assert(!new File(stage.getOutputPath).exists() && server.requests.isEmpty)
        }
        val result = stage.setCustomHeaders(Map(canonical -> "synthetic-corrected")).transform(data).head()
        assert(result.getAs[Row]("error") == null && server.requests.size() == 1)
        assert(stage.resume(spark).head() == result && server.requests.size() == 1)
        assert(AIPrompt.getOutputStatus(spark, stage.getOutputPath).allSucceeded)
      }
    }
  }

  test("durable schema rejects nonresumable settings without jobs, storage or requests") {
    withServer { server =>
      val settings: Seq[AIPrompt => AIPrompt] = Seq(
        _.setSubscriptionKeyCol("credential"),
        _.setAADTokenCol("credential"),
        _.setCustomAuthHeaderCol("credential"),
        stage => stage.setVectorParam(stage.customHeaders, "headers"),
        stage => stage.setVectorParam(stage.telemHeaders, "headers"),
        stage => stage.setHandler(stage.getHandler),
        _.setPreviousResponseId("provider-response"),
        _.setPreviousResponseIdCol("credential"),
        _.setExtraBody(Map("file_id" -> "provider-file")),
        _.setExtraBody(Map("extension" -> Map("file_url" -> "https://example.invalid/media"))))
      val group = java.util.UUID.randomUUID().toString
      spark.sparkContext.setJobGroup(group, "Durable schema-only configuration validation")
      try {
        settings.zipWithIndex.foreach { case (configure, index) =>
          withClue(s"configuration $index: ") {
            val root = path()
            val stage = configure(prompt(root, server).setApiType("responses")
              .setUrl(server.url.stripSuffix("/chat/completions")))
            val lazyStage = stage.copy(ParamMap.empty)
            lazyStage.clear(lazyStage.outputPath)
            lazyStage.transformSchema(inputSchema)
            intercept[IllegalArgumentException](stage.transformSchema(inputSchema))
            intercept[IllegalArgumentException] {
              new Pipeline().setStages(Array(stage)).transformSchema(inputSchema)
            }
            assert(!new File(root).exists())
          }
        }
        assert(spark.sparkContext.statusTracker.getJobIdsForGroup(group).isEmpty)
        assert(server.requests.get() == 0)
      } finally spark.sparkContext.clearJobGroup()
    }
  }

  test("durable schema rejects external native questions through direct and facade stages") {
    withServer { server =>
      val cases = Seq(
        (new AIPrompt().setApiType("systemone"), new AISystemOne(),
          """{"urgent":{"type":"noul","instructions":"Is it urgent?","file_id":"provider-file"}}"""),
        (new AIPrompt().setApiType("decisions"), new OpenAIDecisions(),
          """[{"type":"predicate","instructions":"Is it urgent?","file_id":"provider-file"}]"""))
      cases.foreach { case (facade, direct, questions) =>
        Seq(facade, direct).foreach { stage =>
          val root = path()
          stage.setInputCol("text").setModel("model").setQuestions(questions)
            .setUrl(if (stage.getApiType == "systemone") server.url.stripSuffix("/v1/chat/completions")
              else server.url.stripSuffix("/chat/completions"))
          stage.transformSchema(inputSchema)
          stage.setOutputPath(root)
          val error = intercept[IllegalArgumentException](stage.transformSchema(inputSchema))
          assert(error.getMessage.contains("external resources") && !new File(root).exists())
        }
      }
      assert(server.requests.get() == 0)
    }
  }

  test("durable schema analyses stable templates without evaluating input or requiring Delta") {
    val session = spark.newSession()
    session.conf.set("spark.sql.ansi.enabled", "true")
    session.udf.register("aiprompt_schema_udf", (value: String) => value)
    AIPrompt.withSession(session) {
      withServer { server =>
        val group = java.util.UUID.randomUUID().toString
        session.sparkContext.setJobGroup(group, "Durable schema-only template analysis")
        try {
          Seq("{current_timestamp()}", "{aiprompt_schema_udf(text)}", "{(select 'external')}")
            .foreach { template =>
              val root = path()
              val stage = prompt(root, server).setPromptTemplate(template)
              val lazyStage = stage.copy(ParamMap.empty)
              lazyStage.clear(lazyStage.outputPath)
              lazyStage.transformSchema(inputSchema)
              intercept[IllegalArgumentException](stage.transformSchema(inputSchema))
              assert(!new File(root).exists())
            }
          val root = path()
          val valid = prompt(root, server).setPromptTemplate("{upper(text)}:{1 / 0}")
          val lazyStage = valid.copy(ParamMap.empty)
          lazyStage.clear(lazyStage.outputPath)
          assert(valid.transformSchema(inputSchema) == AIPromptOutput.storageSchema(
            lazyStage.transformSchema(inputSchema)))
          assert(!new File(root).exists() && server.requests.get() == 0)
          assert(session.sparkContext.statusTracker.getJobIdsForGroup(group).isEmpty)
        } finally session.sparkContext.clearJobGroup()
      }
    }
  }

  test("durable schema validates input types even when an output role replaces the field") {
    withServer { server =>
      val roles: Seq[AIPrompt => AIPrompt] = Seq(
        _.setOutputCol("replaced"), _.setErrorCol("replaced"), _.setUsageCol("replaced"),
        _.setRawResponseCol("replaced"), _.setResponseIdCol("replaced"))
      val group = java.util.UUID.randomUUID().toString
      spark.sparkContext.setJobGroup(group, "Durable input type preflight")
      try {
        Seq(CalendarIntervalType, NullType, new StructType().add("empty", new StructType()))
          .foreach { dataType =>
            roles.zipWithIndex.foreach { case (configure, index) =>
              withClue(s"${dataType.typeName}, role $index: ") {
                val root = path()
                val schema = inputSchema.add("replaced", dataType)
                val stage = configure(prompt(root, server))
                val lazyStage = stage.copy(ParamMap.empty)
                lazyStage.clear(lazyStage.outputPath)
                AIPromptOutput.storageSchema(lazyStage.transformSchema(schema))
                Seq(stage, new Pipeline().setStages(Array(stage))).foreach { transformer =>
                  val error = intercept[IllegalArgumentException](transformer.transformSchema(schema))
                  assert(error.getMessage.contains("AIPrompt durable storage"))
                }
                assert(!new File(root).exists())
              }
            }
          }
        assert(spark.sparkContext.statusTracker.getJobIdsForGroup(group).isEmpty)
        assert(server.requests.get() == 0)
      } finally spark.sparkContext.clearJobGroup()
    }
  }
}

class AIPromptSessionTeardownSuite extends AIPromptOutputTestBase {
  test("durable fixture teardown clears stopped session aliases before other stages are constructed") {
    val alias = spark.newSession()
    SparkSession.setActiveSession(alias)
    SparkSession.setDefaultSession(alias)
    AIPromptDeltaTestSession.stopSparkSession()
    assert(alias.sparkContext.isStopped)
    assert(!SparkSession.getActiveSession.exists(_.sparkContext.isStopped))
    assert(!SparkSession.getDefaultSession.exists(_.sparkContext.isStopped))
    new AIPrompt().setInputCol("text").setUrl("https://example.invalid/v1").setModel("model")
      .transformSchema(new StructType().add("text", StringType))
  }
}
