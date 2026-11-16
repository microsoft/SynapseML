// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.io.http.HTTPTransformer
import com.microsoft.azure.synapse.ml.services.openai.OpenAIPrompt
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSInputStream, FileSystem, Path, RawLocalFileSystem}
import org.apache.spark.SparkException
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{StringType, StructType}
import spray.json._

import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths}
import java.util.concurrent.{CancellationException, ConcurrentLinkedQueue}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._
import scala.util.Using

class AIPromptRecoveryAttachmentSuite extends AIPromptOutputTestBase {
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
