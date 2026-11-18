// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.core.contracts.HasInputCol
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.http.{HTTPSchema, URLKey}
import com.microsoft.azure.synapse.ml.param.GlobalParams
import com.microsoft.azure.synapse.ml.services.OpenAIApiVersion
import com.microsoft.azure.synapse.ml.services.aifoundry.AIFoundryModel
import com.microsoft.azure.synapse.ml.services.openai.{OpenAIDecisions, OpenAIDeploymentNameKey,
  OpenAIPrompt, OpenAITemperatureKey}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import spray.json._

import java.nio.file.Files
import java.util.concurrent.{Callable, CountDownLatch, ExecutionException, Executors, TimeUnit}
import scala.collection.JavaConverters._
import scala.util.Using

class AIPromptRecoveryPreflightSuite extends AIPromptOutputTestBase {
  private val chatReply =
    """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"},"finish_reason":"stop"}]}"""

  private def input: DataFrame = spark.createDataFrame(
    Seq(Row("hello")).asJava, new StructType().add("text", StringType))

  test("Decisions rejects prompt input columns marked as paths before IO") {
    val caseSensitive = spark.conf.get("spark.sql.caseSensitive")
    val root = Files.createTempDirectory("decisions-input-path-")
    val data = input.selectExpr("text AS asset")
    try {
      spark.conf.set("spark.sql.caseSensitive", "false")
      Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
        for (name <- Seq("asset", "ASSET"); durable <- Seq(false, true);
             stage <- Seq(new AIPrompt().setApiType("decisions"), new OpenAIDecisions())) {
          stage.setUrl(server.url + "/v1").setModel("model").setInputCol(name)
            .setColumnTypes(Map("asset" -> "path")).setQuestions(NativeRequestFixtures.DecisionQuestions)
          if (durable) stage.setOutputPath(root.resolve("run").toString)
          Seq[() => Any](() => stage.transformSchema(data.schema), () => stage.transform(data)).foreach { call =>
            assert(intercept[IllegalArgumentException](call()).getMessage ==
              "requirement failed: Decisions inputCol cannot also be a 'path' column; " +
                "use a separate text column or a promptTemplate filename placeholder")
            assert(server.requests.isEmpty && !Files.exists(root.resolve("run")))
          }
        }
      }
    } finally {
      spark.conf.set("spark.sql.caseSensitive", caseSensitive)
      org.apache.commons.io.FileUtils.deleteDirectory(root.toFile)
    }
  }

  test("durable facade rejects inherited typed cache controls before HTTP or storage") {
    val settings: Seq[(String, AIPrompt => AIPrompt)] = Seq(
      "promptCacheOptions" -> (_.setPromptCacheOptions(Map("mode" -> "auto"))),
      "promptCacheKey" -> (_.setPromptCacheKey("cache-key")),
      "promptCacheRetention" -> (_.setPromptCacheRetention("24h")))
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      AIRequestAdapter.SupportedApis.foreach { api =>
        settings.foreach { case (name, configure) =>
          val root = path()
          val stage = configure(new AIPrompt().setApiType(api).setUrl(server.url + "/v1")
            .setModel("model").setInputCol("text").setOutputPath(root))
          Seq(stage, stage.copy(ParamMap.empty).set(stage.getParam(name), Right("text"))).foreach { candidate =>
            val error = intercept[IllegalArgumentException](candidate.transform(input))
            assert(error.getMessage.contains(s"$name is not supported"))
          }
          assert(!Files.exists(java.nio.file.Paths.get(root)) && server.requests.isEmpty)
        }
      }
    }
  }

  test("Decisions accepts distinct case-sensitive prompt and image columns") {
    val caseSensitive = spark.conf.get("spark.sql.caseSensitive")
    val image = "data:image/png;base64,AQID"
    try {
      spark.conf.set("spark.sql.caseSensitive", "true")
      val data = spark.createDataFrame(Seq(Row(image, "Inspect")).asJava,
        new StructType().add("asset", StringType).add("ASSET", StringType))
      Seq(new AIPrompt().setApiType("decisions"), new OpenAIDecisions()).foreach { stage =>
        Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
          stage.setUrl(server.url + "/v1").setModel("model").setInputCol("ASSET")
            .setColumnTypes(Map("asset" -> "path")).setQuestions(NativeRequestFixtures.DecisionQuestions)
            .setOutputCol("answer").setErrorCol("error")
          val result = stage.transform(data)
          assert(result.schema == stage.transformSchema(data.schema))
          val row = result.head()
          assert(row.getAs[Row]("error") == null && row.getAs[Row]("answer") != null)
          assert(server.requests.size() == 1)
          assert(AIJson.objectString(server.requests.peek()._2).fields("input") == JsArray(
            JsObject("role" -> JsString("user"), "content" -> JsArray(
              JsObject("type" -> JsString("input_text"), "text" -> JsString("Inspect")),
              JsObject("type" -> JsString("input_image"), "image_url" -> JsString(image))))))
        }
      }
    } finally spark.conf.set("spark.sql.caseSensitive", caseSensitive)
  }

  Seq("chat_completions", "responses", "decisions").foreach { api =>
    test(s"$api explicit messages reject unused construction settings before HTTP or durable storage") {
      val data = input.selectExpr("text", "array(named_struct('role', 'user', 'content', text)) AS messages")
      val settings = Seq[(String, AIPrompt => AIPrompt)](
        "systemPrompt" -> (_.setSystemPrompt("Do not ignore this")),
        "columnTypes" -> (_.setColumnTypes(Map("text" -> "path"))),
        "columnTypes" -> (_.setColumnTypes(Map("text" -> "text"))),
        "fileSizeLimitMB" -> (_.setFileSizeLimitMB(1.0)),
        "dropPrompt" -> (_.setDropPrompt(false)), "dropPrompt" -> (_.setDropPrompt(true))) ++
        (if (api == "decisions") Seq[(String, AIPrompt => AIPrompt)]("inputMode" -> (_.setInputMode("text")),
          "inputMode" -> (_.setInputMode("json"))) else Seq.empty)
      val stages = if (api == "decisions") Seq(new AIPrompt().setApiType(api), new OpenAIDecisions())
      else Seq(new AIPrompt().setApiType(api))
      val reply = if (api == "responses") {
        """{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}]}"""
      } else if (api == "decisions") NativeRequestFixtures.DecisionResponse else chatReply
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val root = Files.createTempDirectory("aiprompt-message-settings-")
        try {
          stages.foreach { base =>
            base.setUrl(server.url + "/v1").setModel("model").setMessagesCol("messages").setErrorCol("error")
            if (api == "decisions") base.setQuestions(NativeRequestFixtures.DecisionQuestions)
            settings.foreach { case (name, configure) =>
              Seq(true).foreach { durable =>
                val stage = configure(base.copy(ParamMap.empty))
                if (durable) stage.setOutputPath(root.resolve("run").toString)
                Seq[() => Any](() => stage.transformSchema(data.schema), () => stage.transform(data)).foreach { call =>
                  val message = intercept[IllegalArgumentException](call()).getMessage
                  if (name == "fileSizeLimitMB") {
                    assert(message == "fileSizeLimitMB cannot be combined with messagesCol")
                  } else assert(message.contains(name))
                }
                assert(server.requests.isEmpty && !Files.exists(root.resolve("run")))
              }
            }
            assert(base.setColumnTypes(Map.empty[String, String]).transform(data).head().getAs[Row]("error") == null)
            assert(server.requests.size() == 1)
            server.requests.clear()
          }
        } finally org.apache.commons.io.FileUtils.deleteDirectory(root.toFile)
      }
    }
  }
  Seq("systemone", "decisions", "embeddings").foreach { api =>
    test(s"$api ignores inactive global temperature in schema, requests and durable identity") {
      val previous = GlobalParams.getGlobalParam(OpenAITemperatureKey)
      val reply = api match {
        case "systemone" => NativeRequestFixtures.SystemResponse
        case "decisions" => NativeRequestFixtures.DecisionResponse
        case _ => """{"data":[{"index":0,"embedding":[0.25,0.5]}],"model":"model"}"""
      }
      try {
        GlobalParams.resetGlobalParam(OpenAITemperatureKey)
        Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
          val stage = (api match {
            case "systemone" => new AISystemOne().setStateCol("text")
              .setQuestions(NativeRequestFixtures.SystemQuestions)
            case "decisions" => new OpenAIDecisions().setInputCol("text")
              .setQuestions(NativeRequestFixtures.DecisionQuestions)
            case _ => new AIPrompt().setApiType(api).setInputCol("text")
          }).setUrl(if (api == "systemone") server.url else server.url + "/v1")
            .setModel("model").setOutputCol("answer").setErrorCol("error")
          val original = stage.effectiveCopy()
          val expectedSchema = stage.transformSchema(input.schema)
          val fingerprint = AIPromptOutput.fingerprint(original, input.schema, expectedSchema, spark)
          val defaults = Seq[Either[Double, String]](Right("absent-global-column"), Right("text"), Left(0.25))
          defaults.foreach { default =>
            GlobalParams.setGlobalParam(OpenAITemperatureKey, default)
            assert(stage.transformSchema(input.schema) == expectedSchema)
            assert(AIPromptOutput.fingerprint(stage.effectiveCopy(), input.schema, expectedSchema, spark) ==
              fingerprint)
            val row = stage.transform(input).head()
            assert(row.getAs[Row]("error") == null && !row.isNullAt(row.fieldIndex("answer")))
            assert(!AIJson.objectString(server.requests.asScala.last._2).fields.contains("temperature"))
            assert(GlobalParams.getGlobalParam(OpenAITemperatureKey).contains(default))
          }
          val requestsBefore = server.requests.size()
          Seq(stage.copy(ParamMap.empty).setTemperature(0.25),
            stage.copy(ParamMap.empty).setTemperatureCol("text")).foreach { explicit =>
            intercept[IllegalArgumentException](explicit.transformSchema(input.schema))
          }
          assert(server.requests.size() == requestsBefore)
        }
      } finally {
        previous match {
          case Some(value) => GlobalParams.setGlobalParam(OpenAITemperatureKey, value)
          case None => GlobalParams.resetGlobalParam(OpenAITemperatureKey)
        }
      }
    }
  }

  test("native stages require explicit models despite global chat and Foundry defaults") {
    val previousModel = GlobalParams.getGlobalParam(AIFoundryModel)
    val previousDeployment = GlobalParams.getGlobalParam(OpenAIDeploymentNameKey)
    try {
      Seq("systemone", "decisions").foreach { api =>
        val questions = if (api == "systemone") NativeRequestFixtures.SystemQuestions
          else NativeRequestFixtures.DecisionQuestions
        val direct = if (api == "systemone") new AISystemOne() else new OpenAIDecisions()
        Seq(direct, new AIPrompt().setApiType(api)).foreach { stage =>
          stage.setUrl(if (api == "systemone") "https://example.services.ai.azure.com"
            else "https://example.invalid/v1").setInputCol("text").setQuestions(questions)
          GlobalParams.resetGlobalParam(AIFoundryModel)
          GlobalParams.resetGlobalParam(OpenAIDeploymentNameKey)
          val explicit = stage.copy(ParamMap.empty).setModel("native-model")
          val schema = explicit.transformSchema(input.schema)
          val fingerprint = AIPromptOutput.fingerprint(explicit.effectiveCopy(), input.schema, schema, spark)
          Seq[Either[String, String]](Left("chat-model"), Right("absent-model-column")).foreach { value =>
            GlobalParams.setGlobalParam(AIFoundryModel, value)
            GlobalParams.setGlobalParam(OpenAIDeploymentNameKey, value)
            val error = intercept[IllegalArgumentException](stage.transformSchema(input.schema))
            assert(error.getMessage.contains("Set deploymentName/model"))
            assert(explicit.transformSchema(input.schema) == schema)
            assert(AIPromptOutput.fingerprint(explicit.effectiveCopy(), input.schema, schema, spark) == fingerprint)
            assert(GlobalParams.getGlobalParam(AIFoundryModel).contains(value))
          }
        }
      }
    } finally {
      previousModel match {
        case Some(value) => GlobalParams.setGlobalParam(AIFoundryModel, value)
        case None => GlobalParams.resetGlobalParam(AIFoundryModel)
      }
      previousDeployment match {
        case Some(value) => GlobalParams.setGlobalParam(OpenAIDeploymentNameKey, value)
        case None => GlobalParams.resetGlobalParam(OpenAIDeploymentNameKey)
      }
    }
  }
  Seq("systemone", "decisions", "chat_completions", "responses", "embeddings", "custom", "global-v1")
    .foreach { route =>
      test(s"global API versions are inactive for $route in schema, requests and durable identity") {
        val previous = GlobalParams.getGlobalParam(OpenAIApiVersion)
        val previousUrl = GlobalParams.getGlobalParam(URLKey)
        val data = input.withColumn("wrongVersion", org.apache.spark.sql.functions.lit(1))
        val api = if (Set("custom", "global-v1")(route)) "chat_completions" else route
        val reply = api match {
          case "systemone" => NativeRequestFixtures.SystemResponse
          case "decisions" => NativeRequestFixtures.DecisionResponse
          case "embeddings" => """{"data":[{"index":0,"embedding":[0.25,0.5]}],"model":"model"}"""
          case "responses" =>
            """{"output":[{"type":"message","role":"assistant",
              |"content":[{"type":"output_text","text":"answer"}]}]}""".stripMargin
          case _ => chatReply
        }
        try {
          GlobalParams.resetGlobalParam(OpenAIApiVersion)
          Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
            val stage = (api match {
              case "systemone" => new AISystemOne().setStateCol("text")
                .setQuestions(NativeRequestFixtures.SystemQuestions)
              case "decisions" => new OpenAIDecisions().setInputCol("text")
                .setQuestions(NativeRequestFixtures.DecisionQuestions)
              case _ => new AIPrompt().setApiType(api).setInputCol("text")
            }).setModel("model").setOutputCol("answer").setErrorCol("error")
            route match {
              case "systemone" => stage.setUrl(server.url + "/providers/microsoft/v1/systemone")
              case "custom" => stage.setCustomUrlRoot(server.url + "/chat/completions?api-version=owned")
              case "global-v1" => GlobalParams.setGlobalParam(URLKey, server.url + "/v1/chat/completions")
              case _ => stage.setUrl(server.url + "/v1")
            }
            val expectedSchema = stage.transformSchema(data.schema)
            val fingerprint = AIPromptOutput.fingerprint(stage.effectiveCopy(), data.schema, expectedSchema, spark)
            val original = stage.transform(data).head()
            assert(original.getAs[Row]("error") == null && !original.isNullAt(original.fieldIndex("answer")))
            val expectedRequest = server.requests.asScala.last
            val defaults = Seq[Either[String, String]](
              Right("absentVersion"), Right("wrongVersion"), Left("dated-one"), Left("dated-two"))
            defaults.foreach { default =>
              GlobalParams.setGlobalParam(OpenAIApiVersion, default)
              assert(stage.transformSchema(data.schema) == expectedSchema)
              assert(AIPromptOutput.fingerprint(stage.effectiveCopy(), data.schema, expectedSchema, spark) ==
                fingerprint)
              val row = stage.transform(data).head()
              assert(row.getAs[Row]("error") == null && !row.isNullAt(row.fieldIndex("answer")))
              assert(server.requests.asScala.last == expectedRequest)
              assert(GlobalParams.getGlobalParam(OpenAIApiVersion).contains(default))
            }
            val supportsExplicitVersion = Set("decisions", "chat_completions", "responses", "global-v1")(route)
            Seq("explicit" -> stage.copy(ParamMap.empty).setApiVersion("explicit"),
              "hello" -> stage.copy(ParamMap.empty).setApiVersionCol("text")).foreach { case (version, explicit) =>
              if (supportsExplicitVersion) {
                assert(explicit.transformSchema(data.schema) == expectedSchema)
                assert(AIPromptOutput.fingerprint(explicit.effectiveCopy(), data.schema, expectedSchema, spark) !=
                  fingerprint)
                val row = explicit.transform(data).head()
                assert(row.getAs[Row]("error") == null && !row.isNullAt(row.fieldIndex("answer")))
                assert(server.requests.asScala.last._1 == expectedRequest._1 + s"?api-version=$version")
                assert(server.requests.asScala.last._2 == expectedRequest._2)
              } else {
                val error = intercept[IllegalArgumentException](explicit.transformSchema(data.schema))
                assert(error.getMessage.contains("apiVersion is not supported"))
                if (route == "custom") assert(error.getMessage.contains("customUrlRoot"))
                val transformError = intercept[IllegalArgumentException](explicit.transform(data))
                assert(transformError.getMessage == error.getMessage)
              }
            }
            assert(server.requests.size() == (if (supportsExplicitVersion) 7 else 5))
          }
        } finally {
          previous match {
            case Some(value) => GlobalParams.setGlobalParam(OpenAIApiVersion, value)
            case None => GlobalParams.resetGlobalParam(OpenAIApiVersion)
          }
          previousUrl match {
            case Some(value) => GlobalParams.setGlobalParam(URLKey, value)
            case None => GlobalParams.resetGlobalParam(URLKey)
          }
        }
      }
    }

  test("global API versions remain active for dated routes") {
    val previous = GlobalParams.getGlobalParam(OpenAIApiVersion)
    val data = input.withColumn("version", org.apache.spark.sql.functions.lit("dated-column"))
    try {
      Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
        val stage = new AIPrompt().setUrl(server.url).setModel("model").setInputCol("text")
          .setOutputCol("answer").setErrorCol("error")
        GlobalParams.setGlobalParam(OpenAIApiVersion, Right("absentVersion"))
        intercept[IllegalArgumentException](stage.transformSchema(data.schema))
        GlobalParams.setGlobalParam(OpenAIApiVersion, Right("version"))
        val columnVersion = stage.transform(data).head()
        assert(columnVersion.getAs[String]("answer") == "answer" && columnVersion.getAs[Row]("error") == null)
        assert(server.requests.asScala.last._1.endsWith("?api-version=dated-column"))
        GlobalParams.setGlobalParam(OpenAIApiVersion, Left("dated-one"))
        val fingerprint = AIPromptOutput.fingerprint(stage.effectiveCopy(), data.schema,
          stage.transformSchema(data.schema), spark)
        val scalarVersion = stage.transform(data).head()
        assert(scalarVersion.getAs[String]("answer") == "answer" && scalarVersion.getAs[Row]("error") == null)
        assert(server.requests.asScala.last._1.endsWith("?api-version=dated-one"))
        GlobalParams.setGlobalParam(OpenAIApiVersion, Left("dated-two"))
        assert(AIPromptOutput.fingerprint(stage.effectiveCopy(), data.schema,
          stage.transformSchema(data.schema), spark) != fingerprint)
      }
    } finally {
      previous match {
        case Some(value) => GlobalParams.setGlobalParam(OpenAIApiVersion, value)
        case None => GlobalParams.resetGlobalParam(OpenAIApiVersion)
      }
    }
  }
  test("invalid concurrent timeout defaults fail before HTTP or durable storage") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val saved = tmpDir.resolve("invalid-timeout-output")
      Seq(0.0, -1.0, 1e-10, Double.NaN, Double.PositiveInfinity).foreach { timeout =>
        Seq(true).foreach { durable =>
          val stage = new TestableAIDefaultTimeoutPrompt("invalid-default-timeout").setConcurrentTimeoutDefault(timeout)
            .setUrl(server.url + "/v1").setModel("model").setInputCol("text")
          if (durable) stage.setOutputPath(saved.toString)
          assert(intercept[IllegalArgumentException](stage.transformSchema(input.schema))
            .getMessage.contains("concurrentTimeout"))
          assert(intercept[IllegalArgumentException](stage.transform(input)).getMessage.contains("concurrentTimeout"))
          assert(server.requests.isEmpty && !Files.exists(saved))
        }
      }
    }
  }
}
