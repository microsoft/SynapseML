// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.http.HTTPSchema
import com.microsoft.azure.synapse.ml.param.GlobalParams
import com.microsoft.azure.synapse.ml.services.OpenAIApiVersion
import com.microsoft.azure.synapse.ml.services.aifoundry.AIFoundryModel
import com.microsoft.azure.synapse.ml.services.openai.{OpenAIDecisions, OpenAIDeploymentNameKey,
  OpenAIEmbeddingDeploymentNameKey, OpenAITemperatureKey, OpenAITopPKey}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{Encoders, Row}
import org.apache.spark.sql.types.{StringType, StructType}
import spray.json._

import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Locale
import scala.collection.JavaConverters._
import scala.util.Using

class AIPromptRecoveryEndpointSuite extends AIPromptOutputTestBase {
  private val inputSchema = new StructType().add("text", StringType)
  private val cases = Seq(
    ("chat_completions", "chat/completions",
      """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""),
    ("responses", "responses",
      """{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}]}"""),
    ("embeddings", "embeddings", """{"data":[{"index":0,"embedding":[0.25,0.5]}]}"""),
    ("decisions", "decisions", NativeRequestFixtures.DecisionResponse))

  cases.filter(entry => Set("chat_completions", "embeddings")(entry._1)).foreach { case (api, _, reply) =>
    test(s"deployment URL Unicode rejects malformed scalar and default $api bindings before storage") {
      val input = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
      val keys = Seq(OpenAIDeploymentNameKey, OpenAIEmbeddingDeploymentNameKey, AIFoundryModel)
      val previous = keys.map(key => key -> GlobalParams.getGlobalParam(key))
      val parent = Files.createTempDirectory("aiprompt-deployment-unicode-")
      try {
        Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
          val bindings = Seq("deployment", "model", "default-deployment", "default-model",
            "global-deployment", "global-model") ++ (if (api == "embeddings") Seq("global-embedding") else Seq.empty)
          for (malformed <- Seq("private-\uD800", "private-\uDC00"); binding <- bindings;
               durable <- Seq(true)) {
            keys.foreach(GlobalParams.resetGlobalParam)
            val stage = new TestableAIMutableJsonDefaultPrompt("deployment-unicode")
              .setApiType(api).setUrl(server.url).setInputCol("text")
            binding match {
              case "deployment" => stage.setDeploymentName(malformed)
              case "model" => stage.setModel(malformed)
              case "default-deployment" => stage.replaceJsonDefault(stage.deploymentName, Left(malformed))
              case "default-model" => stage.replaceJsonDefault(stage.model, Left(malformed))
              case "global-deployment" => GlobalParams.setGlobalParam(OpenAIDeploymentNameKey, Left(malformed))
              case "global-model" => GlobalParams.setGlobalParam(AIFoundryModel, Left(malformed))
              case "global-embedding" => GlobalParams.setGlobalParam(OpenAIEmbeddingDeploymentNameKey, Left(malformed))
            }
            if (durable) stage.setOutputPath(parent.resolve("run").toString)
            Seq[() => Any](() => stage.transformSchema(inputSchema), () => stage.transform(input)).foreach { invoke =>
              val error = intercept[IllegalArgumentException](invoke())
              assert(error.getMessage.contains("surrogate") && !error.getMessage.contains("private-"))
              assert(error.getMessage.contains(if (binding.endsWith("model")) "model" else "deploymentName"))
              assert(error.getCause == null)
            }
            assert(server.requests.isEmpty && !Files.exists(parent.resolve("run")))
          }
        }
      } finally {
        previous.foreach {
          case (key, Some(value)) => GlobalParams.setGlobalParam(key, value)
          case (key, None) => GlobalParams.resetGlobalParam(key)
        }
        org.apache.commons.io.FileUtils.deleteDirectory(parent.toFile)
      }
    }
  }
  for (custom <- Seq(false, true); component <- Seq("path", "query")) {
    test(s"endpoint Unicode rejects malformed $component with custom=$custom before HTTP or storage") {
      val input = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
      val parent = Files.createTempDirectory("aiprompt-endpoint-unicode-")
      try {
        Using.resource(new AIRecordingServer((_, _) => 200 -> "{}")) { server =>
          val stages = AIRequestAdapter.SupportedApis.map(new AIPrompt().setApiType(_)) ++
            Seq(new AISystemOne(), new OpenAIDecisions())
          for (stage <- stages; malformed <- Seq("\uD800", "\uDC00"); durable <- Seq(true)) {
            val endpoint = server.url + (if (component == "path") s"/private-$malformed/v1"
              else s"/v1?private=$malformed")
            stage.setModel("model").setInputCol("text")
            if (custom) stage.setCustomUrlRoot(endpoint) else stage.setUrl(endpoint)
            if (AIRequestAdapter.NativeApis(stage.getApiType)) {
              stage.setQuestions(if (stage.getApiType == "systemone") NativeRequestFixtures.SystemQuestions
                else NativeRequestFixtures.DecisionQuestions)
            }
            if (durable) stage.setOutputPath(parent.resolve("run").toString) else stage.clear(stage.outputPath)
            Seq[() => Any](() => stage.transformSchema(inputSchema), () => stage.transform(input)).foreach { invoke =>
              val error = intercept[IllegalArgumentException](invoke())
              assert(error.getMessage.contains("surrogate") && !error.getMessage.contains("private"))
              assert(error.getMessage.contains(if (custom) "customUrlRoot" else "url"))
              assert(error.getCause == null)
            }
            assert(server.requests.isEmpty && !Files.exists(parent.resolve("run")))
          }
        }
      } finally {
        org.apache.commons.io.FileUtils.deleteDirectory(parent.toFile)
      }
    }
  }
  test("dated API versions reject malformed scalar and default Unicode before HTTP or storage") {
    val input = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
    val previous = GlobalParams.getGlobalParam(OpenAIApiVersion)
    val root = Files.createTempDirectory("aiprompt-version-unicode-")
    try {
      cases.filterNot(_._1 == "decisions").foreach { case (api, _, reply) =>
        Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
          for (malformed <- Seq("private-\uD800", "private-\uDC00");
               binding <- Seq("explicit", "default", "global"); durable <- Seq(true)) {
            GlobalParams.resetGlobalParam(OpenAIApiVersion)
            val stage = new TestableAIMutableJsonDefaultPrompt("version-unicode").setApiType(api)
              .setUrl(server.url).setDeploymentName("model").setInputCol("text")
            binding match {
              case "explicit" => stage.setApiVersion(malformed)
              case "default" => stage.replaceJsonDefault(stage.apiVersion, Left(malformed))
              case "global" => GlobalParams.setGlobalParam(OpenAIApiVersion, Left(malformed))
            }
            if (durable) stage.setOutputPath(root.resolve("run").toString)
            Seq[() => Any](() => stage.transformSchema(inputSchema), () => stage.transform(input)).foreach { invoke =>
              val error = intercept[IllegalArgumentException](invoke())
              assert(error.getMessage.contains("surrogate") && !error.getMessage.contains("private-"))
              assert(error.getMessage.contains("apiVersion"))
              assert(error.getCause == null)
            }
            assert(server.requests.isEmpty && !Files.exists(root.resolve("run")))
          }
        }
      }
    } finally {
      previous match {
        case Some(value) => GlobalParams.setGlobalParam(OpenAIApiVersion, value)
        case None => GlobalParams.resetGlobalParam(OpenAIApiVersion)
      }
      org.apache.commons.io.FileUtils.deleteDirectory(root.toFile)
    }
  }
  Seq(false, true).foreach { explicit =>
    test(s"Responses forwards sampling options for GPT-5-named deployments with explicit=$explicit") {
      val previousTemperature = GlobalParams.getGlobalParam(OpenAITemperatureKey)
      val previousTopP = GlobalParams.getGlobalParam(OpenAITopPKey)
      val root = Files.createTempDirectory("aiprompt-responses-sampling-")
      val reply =
        """{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}]}"""
      try {
        GlobalParams.setGlobalParam(OpenAITemperatureKey, Left(0.25))
        GlobalParams.setGlobalParam(OpenAITopPKey, Left(0.75))
        Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
          val stage = new AIPrompt().setApiType("responses").setUrl(server.url + "/v1")
            .setDeploymentName("gpt-5-deployment").setInputCol("text").setOutputCol("answer").setErrorCol("error")
          if (explicit) stage.setTemperature(0.5).setTopP(0.9)
          val data = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
          val schema = stage.transformSchema(data.schema)
          val durable = stage.copy(ParamMap.empty).setOutputPath(root.resolve("run").toString)
          assert(durable.transformSchema(data.schema) == AIPromptOutput.storageSchema(schema))
          assert(server.requests.isEmpty && !Files.exists(root.resolve("run")))
          val result = stage.transform(data)
          assert(result.schema == schema && server.requests.isEmpty)
          val row = result.head()
          assert(row.getAs[String]("answer") == "answer" && row.getAs[Row]("error") == null)
          assert(server.requests.size() == 1)
          val body = AIJson.objectString(server.requests.peek()._2).fields
          assert(body("model") == JsString("gpt-5-deployment"))
          assert(body("temperature") == JsNumber(if (explicit) 0.5 else 0.25))
          assert(body("top_p") == JsNumber(if (explicit) 0.9 else 0.75))
        }
      } finally {
        previousTemperature match {
          case Some(value) => GlobalParams.setGlobalParam(OpenAITemperatureKey, value)
          case None => GlobalParams.resetGlobalParam(OpenAITemperatureKey)
        }
        previousTopP match {
          case Some(value) => GlobalParams.setGlobalParam(OpenAITopPKey, value)
          case None => GlobalParams.resetGlobalParam(OpenAITopPKey)
        }
        org.apache.commons.io.FileUtils.deleteDirectory(root.toFile)
      }
    }
  }
  test("invalid endpoint ports fail lazy and durable preflight without creating storage") {
    val input = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
    val parent = Files.createTempDirectory("aiprompt-port-preflight-")
    val root = parent.resolve("run")
    try {
      Seq(0, 65536, Int.MaxValue).foreach { port =>
        val endpoint = s"http://127.0.0.1:$port/v1"
        Seq(false, true).foreach { custom =>
          Seq(true).foreach { durable =>
            val stage = new AIPrompt().setModel("model").setInputCol("text")
            if (custom) stage.setCustomUrlRoot(endpoint) else stage.setUrl(endpoint)
            if (durable) stage.setOutputPath(root.toString)
            Seq[() => Any](() => stage.transformSchema(inputSchema), () => stage.transform(input)).foreach { call =>
              val error = intercept[IllegalArgumentException](call())
              assert(error.getMessage.contains("port") && !error.getMessage.contains(endpoint))
              assert(!Files.exists(root))
            }
          }
        }
      }
      Seq("", ":1", ":65535").foreach { port =>
        val stage = new AIPrompt().setUrl(s"http://127.0.0.1$port/v1").setModel("model").setInputCol("text")
        stage.transformSchema(inputSchema)
        stage.setOutputPath(root.toString).transformSchema(inputSchema)
        assert(!Files.exists(root))
      }
    } finally {
      org.apache.commons.io.FileUtils.deleteDirectory(parent.toFile)
    }
  }

  test("underscore endpoint hosts report the unsupported host before lazy or durable execution") {
    val input = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
    val parent = Files.createTempDirectory("aiprompt-host-preflight-")
    val root = parent.resolve("run")
    try {
      val stages = AIRequestAdapter.SupportedApis.map(new AIPrompt().setApiType(_)) ++
        Seq(new AISystemOne(), new OpenAIDecisions())
      stages.foreach { stage =>
        val endpoint = "http://llm_gateway.invalid:8000/v1"
        stage.setUrl(endpoint).setModel("model").setInputCol("text")
        if (AIRequestAdapter.NativeApis(stage.getApiType)) {
          stage.setQuestions(if (stage.getApiType == "systemone") NativeRequestFixtures.SystemQuestions
            else NativeRequestFixtures.DecisionQuestions)
        }
        Seq(false, true).foreach { custom =>
          if (custom) stage.setCustomUrlRoot(endpoint)
          Seq(true).foreach { durable =>
            if (durable) stage.setOutputPath(root.toString) else stage.clear(stage.outputPath)
            val calls = Seq[() => Any](
              () => stage.transformSchema(inputSchema),
              () => stage.transform(input))
            calls.foreach { call =>
              val error = intercept[IllegalArgumentException](call())
              assert(error.getMessage.contains("host") && error.getMessage.contains("underscores"))
              assert(!error.getMessage.contains("credentials") && !error.getMessage.contains(endpoint))
              assert(!Files.exists(root))
            }
          }
        }
      }
    } finally {
      org.apache.commons.io.FileUtils.deleteDirectory(parent.toFile)
    }
  }
  Seq("azure.com", "azure.us", "azure.cn").foreach { cloud =>
    test(s"Decisions rejects $cloud endpoints before lazy or durable execution") {
      val input = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
      val parent = Files.createTempDirectory("aiprompt-azure-preflight-")
      val root = parent.resolve("run")
      try {
        for {
          prefix <- Seq("resource.openai", "resource.services.ai", "resource.cognitiveservices")
          host <- Seq(s"$prefix.$cloud", s"$prefix.$cloud".toUpperCase(Locale.ROOT) + ".")
          route <- Seq("", "/v1", "/v1/decisions")
          stage <- Seq(new AIPrompt().setApiType("decisions"), new OpenAIDecisions())
          custom <- Seq(false, true)
          durable <- Seq(true)
        } {
          stage.setModel("model").setInputCol("text").setQuestions(NativeRequestFixtures.DecisionQuestions)
          val endpoint = s"https://$host$route"
          stage.clear(stage.customUrlRoot)
          if (custom) stage.setCustomUrlRoot(endpoint) else stage.setUrl(endpoint)
          if (durable) stage.setOutputPath(root.toString) else stage.clear(stage.outputPath)
          Seq[() => Any](() => stage.transformSchema(inputSchema), () => stage.transform(input)).foreach { call =>
            val error = intercept[IllegalArgumentException](call())
            assert(error.getMessage.contains("not a Foundry endpoint") && error.getCause == null)
            assert(!error.getMessage.contains(host) && !Files.exists(root))
          }
        }
      } finally {
        org.apache.commons.io.FileUtils.deleteDirectory(parent.toFile)
      }
    }
  }
  cases.foreach { case (api, route, reply) =>
    test(s"$api normalizes bare OpenAI roots without changing stored URLs or applying dated defaults") {
      val input = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
      val previousVersion = GlobalParams.getGlobalParam(OpenAIApiVersion)
      val parent = Files.createTempDirectory("aiprompt-openai-root-")
      val output = parent.resolve("run")
      try {
        GlobalParams.setGlobalParam(OpenAIApiVersion, Right("unavailableVersion"))
        Seq("https://api.openai.com", "https://eu.api.openai.com/", "https://US.API.OPENAI.COM.:443///")
          .foreach { endpoint =>
            val stage = new AIPrompt().setApiType(api).setUrl(endpoint).setModel("model")
              .setInputCol("text").setOutputCol("answer").setErrorCol("error").setSubscriptionKey("synthetic-key")
            if (api == "decisions") stage.setQuestions(NativeRequestFixtures.DecisionQuestions)
            val schema = stage.transformSchema(inputSchema)
            val durable = stage.copy(ParamMap.empty).setOutputPath(output.toString)
            assert(durable.transformSchema(inputSchema) == AIPromptOutput.storageSchema(schema))
            assert(!Files.exists(output))
            val expected = endpoint.replaceAll("/+$", "") + s"/v1/$route"
            stage.setHandler { (_, request) =>
              assert(request.requestLine.uri == expected)
              val body = AIJson.objectString(new String(request.entity.get.content, StandardCharsets.UTF_8))
              assert(body.fields("model") == JsString("model"))
              val headers = request.headers.map(h => h.name.toLowerCase(Locale.ROOT) -> h.value).toMap
              assert(headers.get("authorization").contains("Bearer " + "synthetic-key") &&
                !headers.contains("api-key"))
              HTTPSchema.stringToResponse(reply, 200, "OK")
            }
            val result = stage.transform(input)
            assert(result.schema == schema && stage.getUrl == endpoint)
            val row = result.head()
            assert(row.getAs[Row]("error") == null && row.getAs[Any]("answer") != null)
            stage.setApiVersion("explicit-version")
            intercept[IllegalArgumentException](stage.transformSchema(inputSchema))
            stage.setApiVersionCol("text")
            intercept[IllegalArgumentException](stage.transform(input))
          }
      } finally {
        previousVersion match {
          case Some(value) => GlobalParams.setGlobalParam(OpenAIApiVersion, value)
          case None => GlobalParams.resetGlobalParam(OpenAIApiVersion)
        }
        org.apache.commons.io.FileUtils.deleteDirectory(parent.toFile)
      }
    }
  }
}
