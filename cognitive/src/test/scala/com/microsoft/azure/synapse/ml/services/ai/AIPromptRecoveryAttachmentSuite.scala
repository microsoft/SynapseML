// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.io.http.HTTPTransformer
import com.microsoft.azure.synapse.ml.services.openai.{OpenAIDecisions, OpenAIPrompt}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSInputStream, FileSystem, Path, RawLocalFileSystem}
import org.apache.spark.SparkException
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}
import spray.json._

import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths}
import java.util.concurrent.{CancellationException, ConcurrentLinkedQueue}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.util.Using

class AIPromptRecoveryAttachmentSuite extends AIPromptOutputTestBase {
  test("Decisions path columns retain inline images across durable output and resume") {
    delta()
    import spark.implicits._
    val inline = "data:image/png;base64,AQID"
    val data = Seq(inline).toDF("input")
    Seq(new AIPrompt().setApiType("decisions"), new OpenAIDecisions()).foreach { stage =>
      Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
        stage.setUrl(server.url + "/v1").setModel("model").setSubscriptionKey("unused")
          .setPromptTemplate("Inspect {input}").setColumnTypes(Map("input" -> "path"))
          .setQuestions(NativeRequestFixtures.DecisionQuestions).setFileSizeLimitMB(1)
          .setOutputPath(path()).setOutputCol("answer").setErrorCol("error")
        val row = stage.transform(data).head()
        assert(row.getAs[Row]("error") == null && row.getAs[Row]("answer") != null)
        assert(stage.resume(spark).count() == 1 && server.requests.size() == 1)
        val content = AIJson.objectString(server.requests.peek()._2).fields("input")
          .asInstanceOf[JsArray].elements.head.asJsObject.fields("content").asInstanceOf[JsArray].elements
        assert(content == Vector(
          JsObject("type" -> JsString("input_text"), "text" -> JsString("Inspect attachment.png")),
          JsObject("type" -> JsString("input_image"), "image_url" -> JsString(inline))))
        val status = AIPrompt.getOutputStatus(spark, stage.getOutputPath)
        assert(status.isComplete && status.rowErrorCount == 0 && status.ownerToken.isEmpty)
        Seq("file:///private-image.png", "https://example.invalid/photo.png").foreach { external =>
          val rejected = stage.copy(ParamMap.empty).setOutputPath(path())
          val failure = intercept[SparkException](rejected.transform(Seq(external).toDF("input")))
          assert(failure.getMessage.contains("Durable media must be inline base64"))
          assert(!new File(rejected.getOutputPath, "results").exists() && server.requests.size() == 1)
        }
      }
    }
  }

  test("Decisions durable question images reject hosted URLs and resume inline instructions") {
    delta()
    import spark.implicits._
    val inline = "data:image/png;base64,AQID"
    def questions(url: String): String = JsArray(JsObject(
      "type" -> JsString("predicate"), "name" -> JsString("urgent"),
      "instructions" -> JsArray(
        JsObject("type" -> JsString("input_text"), "text" -> JsString("Does this image require attention?")),
        JsObject("type" -> JsString("input_image"), "image_url" -> JsString(url))))).compactPrint
    val data = Seq("Inspect the image").toDF("text")
    Seq(new AIPrompt().setApiType("decisions"), new OpenAIDecisions()).foreach { stage =>
      Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
        stage.setUrl(server.url + "/v1").setModel("model").setSubscriptionKey("unused")
          .setInputCol("text").setOutputCol("answer").setErrorCol("error")
        val hosted = questions(server.url + "/candidate.png")
        val scalarPath = path()
        val scalar = stage.copy(ParamMap.empty).setQuestions(hosted).setOutputPath(scalarPath)
        Seq[() => Any](() => scalar.transformSchema(data.schema), () => scalar.transform(data)).foreach { call =>
          assert(intercept[IllegalArgumentException](call()).getMessage.contains("Durable media must be inline"))
          assert(!new File(scalarPath).exists() && server.requests.isEmpty)
        }
        val perRow = stage.copy(ParamMap.empty).setQuestionsCol("questions").setOutputPath(path())
        val invalidData = Seq(("Inspect the image", hosted)).toDF("text", "questions")
        assert(intercept[SparkException](perRow.transform(invalidData))
          .getMessage.contains("Durable media must be inline"))
        assert(server.requests.isEmpty)
        Seq(false, true).foreach { fromColumn =>
          val configured = stage.copy(ParamMap.empty).setOutputPath(path())
          val safe = questions(inline)
          val input = if (fromColumn) {
            configured.setQuestionsCol("questions")
            Seq(("Inspect the image", safe)).toDF("text", "questions")
          } else {
            configured.setQuestions(safe)
            data
          }
          val before = server.requests.size()
          val row = configured.transform(input).head()
          assert(row.getAs[Row]("error") == null && row.getAs[Row]("answer") != null)
          assert(configured.resume(spark).count() == 1 && server.requests.size() == before + 1)
          val status = AIPrompt.getOutputStatus(spark, configured.getOutputPath)
          assert(status.isComplete && status.rowErrorCount == 0 && status.ownerToken.isEmpty)
        }
        assert(server.requests.size() == 2)
        server.requests.asScala.foreach { case (route, body) =>
          assert(route == "/v1/decisions")
          assert(AIJson.objectString(body).fields("questions") == questions(inline).parseJson)
        }
      }
    }
  }

  test("Decisions durable explicit image inputs reject hosted media and resume inline messages and JSON") {
    delta()
    import spark.implicits._
    val inline = "data:image/png;base64,AQID"
    for (json <- Seq(false, true); stage <- Seq(new AIPrompt().setApiType("decisions"), new OpenAIDecisions())) {
      def input(url: String): DataFrame = {
        val messages = Seq(url).toDF("asset").select(array(struct(lit("user").as("role"), array(
          map(lit("type"), lit("input_text"), lit("text"), lit("Inspect the image")),
          map(lit("type"), lit("input_image"), lit("image_url"), col("asset"))).as("content"))).as("messages"))
        if (json) messages.select(to_json(col("messages")).as("input")) else messages
      }
      Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
        stage.setUrl(server.url + "/v1").setModel("model").setSubscriptionKey("unused")
          .setQuestions(NativeRequestFixtures.DecisionQuestions).setOutputCol("answer").setErrorCol("error")
        if (json) stage.setInputCol("input").setInputMode("json") else stage.setMessagesCol("messages")
        val rejected = stage.copy(ParamMap.empty).setOutputPath(path())
        val failure = intercept[SparkException](rejected.transform(input(server.url + "/photo.png")))
        assert(failure.getMessage.contains("Durable media must be inline base64"))
        assert(server.requests.isEmpty && !new File(rejected.getOutputPath, "results").exists())
        val data = input(inline)
        stage.setOutputPath(path())
        val result = stage.transform(data)
        assert(result.schema == stage.transformSchema(data.schema))
        val row = result.head()
        assert(row.getAs[Row]("error") == null && row.getAs[Row]("answer") != null)
        assert(stage.resume(spark).head() == row && server.requests.size() == 1)
        val status = AIPrompt.getOutputStatus(spark, stage.getOutputPath)
        assert(status.allSucceeded && status.committedCount == 1 && status.ownerToken.isEmpty)
        val request = server.requests.peek()
        assert(request._1 == "/v1/decisions")
        assert(AIJson.objectString(request._2).fields("input") == JsArray(JsObject(
          "role" -> JsString("user"), "content" -> JsArray(
            JsObject("type" -> JsString("input_text"), "text" -> JsString("Inspect the image")),
            JsObject("type" -> JsString("input_image"), "image_url" -> JsString(inline))))))
      }
    }
  }

  test("Decisions path images retain order through copied and loaded partial durable resumes") {
    delta()
    import spark.implicits._
    val left = "data:image/png;base64,AQID"
    val right = "data:image/png;base64,BAUG"
    val data = Seq.fill(3)((left, right)).toDF("left", "right").select("right", "left")
    Seq(new AIPrompt().setApiType("decisions"), new OpenAIDecisions()).foreach { stage =>
      Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
        val root = path()
        stage.setUrl(server.url + "/v1").setModel("model").setSubscriptionKey("unused")
          .setQuestions(NativeRequestFixtures.DecisionQuestions).setOutputCol("answer").setErrorCol("error")
          .setOutputPath(root).setOutputBatchSize(1).setFileSizeLimitMB(1)
          .setPromptTemplate("Compare {left} and {`RiGhT`}; literal {{LEFT}}")
          .setColumnTypes(ListMap("LEFT" -> "path", "RIGHT" -> "path"))
        def stopAfterBatch(event: String): Unit =
          if (event == "batch_committed") throw new IllegalStateException("stop after committed batch")
        intercept[IllegalStateException](AIPromptOutput.transformObserved(stage, data, stopAfterBatch))
        val first = AIPrompt.getOutputStatus(spark, root)
        assert(first.state == "running" && first.committedCount == 1 && first.pendingCount == 2)
        assert(server.requests.size() == 1)
        AIPrompt.releaseOutputOwner(spark, root, first.ownerToken.get, confirmStopped = true)
        val reordered = stage.copy(ParamMap.empty).setColumnTypes(ListMap("RIGHT" -> "path", "LEFT" -> "path"))
        intercept[IllegalStateException](AIPromptOutput.resumeObserved(reordered, spark, stopAfterBatch))
        val second = AIPrompt.getOutputStatus(spark, root)
        assert(second.state == "running" && second.committedCount == 2 && second.pendingCount == 1)
        assert(server.requests.size() == 2)
        AIPrompt.releaseOutputOwner(spark, root, second.ownerToken.get, confirmStopped = true)
        val saved = path()
        reordered.write.save(saved)
        val loaded = stage match {
          case _: OpenAIDecisions => OpenAIDecisions.load(saved)
          case _ => AIPrompt.load(saved)
        }
        val resumed = loaded.resume(spark)
        assert(resumed.schema == stage.transformSchema(data.schema))
        val rows = resumed.collect()
        assert(rows.length == 3 && rows.forall(row =>
          row.getAs[Row]("error") == null && row.getAs[Row]("answer") != null))
        assert(loaded.resume(spark).count() == 3 && server.requests.size() == 3)
        val complete = AIPrompt.getOutputStatus(spark, root)
        assert(complete.allSucceeded && complete.committedCount == 3 && complete.ownerToken.isEmpty)
        server.requests.asScala.foreach { case (route, body) =>
          assert(route == "/v1/decisions")
          assert(AIJson.objectString(body).fields("input") == JsArray(JsObject(
            "role" -> JsString("user"), "content" -> JsArray(
              JsObject("type" -> JsString("input_text"),
                "text" -> JsString("Compare attachment.png and attachment.png; literal {LEFT}")),
              JsObject("type" -> JsString("input_image"), "image_url" -> JsString(right)),
              JsObject("type" -> JsString("input_image"), "image_url" -> JsString(left))))))
        }
      }
    }
  }

  Seq("chat_completions", "responses").foreach { api =>
    test(s"$api attachment size limits reject invalid Param bindings before HTTP or durable storage") {
      val reply = if (api == "chat_completions")
        """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""
      else """{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}]}"""
      val data = spark.createDataFrame(Seq(Row("data:text/plain;base64,aGVsbG8=")).asJava,
        new StructType().add("asset", StringType))
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val root = Files.createTempDirectory("aiprompt-file-limits-")
        try {
          val base = new TestableAIPromptAttachmentDefaults()
            .setApiType(api).setUrl(server.url + "/v1").setModel("model")
            .setPromptTemplate("{asset}").setColumnTypes(Map("asset" -> "path")).setErrorCol("error")
          for {
            limit <- Seq(0.0, -1.0, Double.NaN)
            binding <- Seq("direct", "copy", "default")
            durable <- Seq(true)
          } {
            val stage = binding match {
              case "copy" => base.copy(ParamMap(base.fileSizeLimitMB -> limit))
              case "default" => base.defaultLimit(limit).copy(ParamMap.empty)
              case "direct" =>
                val direct = base.copy(ParamMap.empty)
                direct.set(direct.fileSizeLimitMB, limit)
            }
            if (durable) stage.setOutputPath(root.resolve("run").toString)
            assert(intercept[IllegalArgumentException](stage.transformSchema(data.schema))
              .getMessage.contains("fileSizeLimitMB must be positive"))
            assert(intercept[IllegalArgumentException](stage.transform(data))
              .getMessage.contains("fileSizeLimitMB must be positive"))
            assert(server.requests.isEmpty && !Files.exists(root.resolve("run")))
          }
          assert(base.setFileSizeLimitMB(0.001).transform(data).head().getAs[Row]("error") == null)
          val oversized = base.setFileSizeLimitMB(0.000001).transform(data).head().getAs[Row]("error")
          assert(AIJson.objectString(oversized.getString(0)).fields("code") == JsString("invalid_request"))
          assert(server.requests.size() == 1)
        } finally org.apache.commons.io.FileUtils.deleteDirectory(root.toFile)
      }
    }

    Seq(true).foreach { durable =>
      test(s"$api applies default attachment limits without making them explicit with durable=$durable") {
        if (durable) delta()
        import spark.implicits._
        val data = Seq("data:text/plain;base64,aGVsbG8=").toDF("asset")
        val reply = if (api == "chat_completions")
          """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""
        else """{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}]}"""
        Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
          val stage = new TestableAIPromptAttachmentDefaults().defaultLimit(0.000001)
            .setApiType(api).setUrl(server.url + "/v1").setModel("model")
            .setPromptTemplate("{asset}").setColumnTypes(Map("asset" -> "path")).setErrorCol("error")
          if (durable) stage.setOutputPath(path())
          val error = stage.transform(data).head().getAs[Row]("error")
          assert(error != null && AIJson.objectString(error.getString(0)).fields("code") == JsString("invalid_request"))
          assert(!stage.isSet(stage.fileSizeLimitMB) && server.requests.isEmpty)
          if (durable) {
            assert(stage.resume(spark).count() == 1 && server.requests.isEmpty)
            val status = AIPrompt.getOutputStatus(spark, stage.getOutputPath)
            assert(status.isComplete && status.rowErrorCount == 1 && status.ownerToken.isEmpty)
          } else {
            val explicit = stage.copy(ParamMap(stage.fileSizeLimitMB -> 0.001))
            assert(explicit.transform(data).head().getAs[Row]("error") == null)
            assert(stage.defaultLimit(Double.PositiveInfinity).transform(data).head().getAs[Row]("error") == null)
            val legacy = new TestableLegacyAttachmentDefaults().setApiType(api).setUrl(server.url + "/v1")
              .setDeploymentName("model").setSubscriptionKey("unused")
              .setPromptTemplate("{asset}").setColumnTypes(Map("asset" -> "path")).setErrorCol("error")
            assert(legacy.transform(data).head().getAs[Row]("error") == null && server.requests.size() == 3)
          }
        }
      }
    }

    test(s"$api rejects resolver-equivalent columnTypes keys before HTTP or durable storage") {
      val caseSensitive = spark.conf.get("spark.sql.caseSensitive")
      val schema = new StructType().add("asset", StringType)
      val data = spark.createDataFrame(Seq(Row("data:image/png;base64,AQID")).asJava, schema)
      val output = path()
      try {
        spark.conf.set("spark.sql.caseSensitive", "false")
        Using.resource(new AIRecordingServer((_, _) => 200 -> "{}")) { server =>
          val base = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
            .setPromptTemplate("Review {asset}").setOutputCol("answer").setErrorCol("error")
          for {
            kind <- Seq("path", "text")
            durable <- Seq(true)
          } {
            val stage = base.copy(ParamMap.empty).setColumnTypes(Map("asset" -> kind, "ASSET" -> "path"))
            if (durable) stage.setOutputPath(output)
            Seq[() => Any](() => stage.transformSchema(schema), () => stage.transform(data)).foreach { call =>
              val error = intercept[IllegalArgumentException](call())
              assert(error.getMessage.contains("Ambiguous columnTypes keys"))
              assert(server.requests.isEmpty && !Files.exists(Paths.get(output)))
            }
          }
        }
      } finally spark.conf.set("spark.sql.caseSensitive", caseSensitive)
    }
  }
  Seq(true).foreach { durable =>
    Seq("chat_completions", "responses").foreach { api =>
      test(s"missing attachment placeholders preserve usable $api inputs with durable=$durable") {
        if (durable) delta()
        val inline = "data:image/png;base64,AQID"
        val absent = Option.empty[String].orNull
        val rows = Seq(absent, "", " \t ").flatMap { missing =>
          Seq(Row("hello", inline, missing), Row("hello", missing, missing))
        } :+ Row(absent, inline, absent)
        val schema = new StructType().add("text", StringType).add("asset", StringType)
          .add("optional", StringType)
        val data = spark.createDataFrame(rows.asJava, schema)
        val reply = if (api == "chat_completions")
          """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""
        else """{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}]}"""
        Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
          val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
            .setPromptTemplate("Review {asset}; optional={optional}; text={text}")
            .setColumnTypes(Map("asset" -> "path", "optional" -> "path"))
            .setOutputCol("answer").setErrorCol("error")
          if (durable) stage.setOutputPath(path()).setOutputBatchSize(2)
          val result = stage.transform(data).collect()
          assert(result.length == 7 && result.count(_.getAs[String]("answer") == "answer") == 3)
          val errors = result.flatMap(row => Option(row.getAs[Row]("error")))
          assert(errors.length == 4 && errors.forall(error =>
            AIJson.objectString(error.getString(0)).fields("code") == JsString("missing_input")))
          assert(server.requests.size() == 3)
          server.requests.asScala.foreach { case (_, body) =>
            val key = if (api == "chat_completions") "messages" else "input"
            val content = AIJson.objectString(body).fields(key).asInstanceOf[JsArray].elements.last
              .asJsObject.fields("content").asInstanceOf[JsArray].elements
            assert(content.exists(_.asJsObject.fields.get("text").contains(
              JsString("Review attachment.png; optional=; text=hello"))))
          }
          if (durable) {
            assert(stage.resume(spark).count() == 7 && server.requests.size() == 3)
            val status = AIPrompt.getOutputStatus(spark, stage.getOutputPath)
            assert(status.isComplete && status.rowErrorCount == 4 && status.ownerToken.isEmpty)
          }
        }
      }
    }
  }
}
