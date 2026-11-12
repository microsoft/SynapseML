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
import org.apache.spark.sql.{Encoders, Row}
import org.apache.spark.sql.types.{StringType, StructType}
import spray.json._

import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.util.Locale
import scala.collection.JavaConverters._
import scala.util.Using

class AIPromptEndpointSuite extends TestBase {
  private val inputSchema = new StructType().add("text", StringType)
  private val cases = Seq(
    ("chat_completions", "chat/completions",
      """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""),
    ("responses", "responses",
      """{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}]}"""),
    ("embeddings", "embeddings", """{"data":[{"index":0,"embedding":[0.25,0.5]}]}"""),
    ("decisions", "decisions", NativeRequestFixtures.DecisionResponse))

  cases.filter(entry => Set("chat_completions", "embeddings")(entry._1)).foreach { case (api, _, reply) =>
    test(s"deployment URL Unicode rejects malformed scalar and default $api bindings before HTTP") {
      val input = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
      val keys = Seq(OpenAIDeploymentNameKey, OpenAIEmbeddingDeploymentNameKey, AIFoundryModel)
      val previous = keys.map(key => key -> GlobalParams.getGlobalParam(key))
      try {
        Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
          val bindings = Seq("deployment", "model", "default-deployment", "default-model",
            "global-deployment", "global-model") ++ (if (api == "embeddings") Seq("global-embedding") else Seq.empty)
          for (malformed <- Seq("private-\uD800", "private-\uDC00"); binding <- bindings) {
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
            Seq[() => Any](() => stage.transformSchema(inputSchema), () => stage.transform(input)).foreach { invoke =>
              val error = intercept[IllegalArgumentException](invoke())
              assert(error.getMessage.contains("surrogate") && !error.getMessage.contains("private-"))
              assert(error.getMessage.contains(if (binding.endsWith("model")) "model" else "deploymentName"))
              assert(error.getCause == null)
            }
            assert(server.requests.isEmpty)
          }
        }
      } finally {
        previous.foreach {
          case (key, Some(value)) => GlobalParams.setGlobalParam(key, value)
          case (key, None) => GlobalParams.resetGlobalParam(key)
        }
      }
    }
  }

  test("deployment URL Unicode keeps column failures per row and preserves valid segment bytes") {
    val unicode = "caf\u00e9 \u96ea \uD83D\uDE00 \uFFFD+?/"
    val models = Seq("\uD800", "\uDC00", unicode, "\\" + "ud800")
    val encoded = Set("caf%C3%A9%20%E9%9B%AA%20%F0%9F%98%80%20%EF%BF%BD%2B%3F%2F", "%5Cud800")
    val schema = new StructType().add("text", StringType).add("target", StringType)
    val input = spark.range(models.size).map(index => Row("hello", models(index.toInt)))(Encoders.row(schema))
    for ((api, route, reply) <- cases.filter(entry => Set("chat_completions", "embeddings")(entry._1));
         deployment <- Seq(false, true)) {
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val stage = new AIPrompt().setApiType(api).setUrl(server.url).setInputCol("text")
          .setOutputCol("answer").setErrorCol("error")
        if (deployment) stage.setDeploymentNameCol("target") else stage.setModelCol("target")
        stage.transformSchema(schema)
        val rows = stage.transform(input).collect()
        val errors = rows.filter(_.getAs[Row]("error") != null)
        assert(rows.length == 4 && errors.length == 2 && server.requests.size() == 2)
        assert(errors.forall(row => row.getAs[Any]("answer") == null &&
          row.getAs[Row]("error").getString(0).contains("invalid_request") &&
          row.getAs[Row]("error").getString(0).contains("surrogate") &&
          row.getAs[Row]("error").getString(0).contains(if (deployment) "deploymentName" else "model")))
        val addresses = server.requests.asScala.map(_._1.split("\\?api-version=", 2).head).toSet
        assert(addresses == encoded.map(value => s"/openai/deployments/$value/$route"))
        val bodyModels = server.requests.asScala.map(entry => AIJson.objectString(entry._2).fields("model")).toSet
        assert(bodyModels == models.drop(2).map(JsString(_)).toSet)
      }
    }
  }

  test("deployment URL Unicode does not preflight model names used only in request bodies") {
    val data = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
    val protocols = cases :+ ("systemone", "providers/microsoft/v1/systemone", NativeRequestFixtures.SystemResponse)
    for ((api, _, reply) <- protocols; custom <- Seq(false, true); malformed <- Seq("\uD800", "\uDC00")) {
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val stage = new AIPrompt().setApiType(api).setModel(malformed).setInputCol("text")
          .setOutputCol("answer").setErrorCol("error")
        if (custom) stage.setCustomUrlRoot(server.url + "/complete")
        else stage.setUrl(if (api == "systemone") server.url else server.url + "/v1")
        if (api == "systemone") stage.setQuestions(NativeRequestFixtures.SystemQuestions)
        else if (api == "decisions") stage.setQuestions(NativeRequestFixtures.DecisionQuestions)
        stage.transformSchema(inputSchema)
        val row = stage.transform(data).head()
        assert(row.getAs[Any]("answer") == null && server.requests.isEmpty)
        assert(row.getAs[Row]("error").getString(0).contains("invalid_request") &&
          row.getAs[Row]("error").getString(0).contains("surrogate"))
      }
    }
  }

  test("deployment URL Unicode ignores model defaults overridden by the selected binding") {
    val data = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
    for ((api, _, reply) <- cases.filter(entry => Set("chat_completions", "embeddings")(entry._1));
         deployment <- Seq(false, true)) {
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val stage = new TestableAIMutableJsonDefaultPrompt("ignored-deployment")
          .setApiType(api).setUrl(server.url).setInputCol("text").setErrorCol("error")
        if (deployment) {
          stage.setDeploymentName("selected")
          stage.replaceJsonDefault(stage.model, Left("\uD800"))
        } else {
          stage.setModel("selected")
          stage.replaceJsonDefault(stage.deploymentName, Left("\uDC00"))
        }
        stage.transformSchema(inputSchema)
        assert(stage.transform(data).head().getAs[Row]("error") == null && server.requests.size() == 1)
        assert(server.requests.peek()._1.startsWith("/openai/deployments/selected/"))
      }
    }
  }

  for (custom <- Seq(false, true); component <- Seq("path", "query")) {
    test(s"endpoint Unicode rejects malformed $component with custom=$custom before HTTP") {
      val input = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
      Using.resource(new AIRecordingServer((_, _) => 200 -> "{}")) { server =>
        val stages = AIRequestAdapter.SupportedApis.map(new AIPrompt().setApiType(_)) ++
          Seq(new AISystemOne(), new OpenAIDecisions())
        for (stage <- stages; malformed <- Seq("\uD800", "\uDC00")) {
          val endpoint = server.url + (if (component == "path") s"/private-$malformed/v1"
            else s"/v1?private=$malformed")
          stage.setModel("model").setInputCol("text")
          if (custom) stage.setCustomUrlRoot(endpoint) else stage.setUrl(endpoint)
          if (AIRequestAdapter.NativeApis(stage.getApiType)) {
            stage.setQuestions(if (stage.getApiType == "systemone") NativeRequestFixtures.SystemQuestions
              else NativeRequestFixtures.DecisionQuestions)
          }
          Seq[() => Any](() => stage.transformSchema(inputSchema), () => stage.transform(input)).foreach { invoke =>
            val error = intercept[IllegalArgumentException](invoke())
            assert(error.getMessage.contains("surrogate") && !error.getMessage.contains("private"))
            assert(error.getMessage.contains(if (custom) "customUrlRoot" else "url"))
            assert(error.getCause == null)
          }
          assert(server.requests.isEmpty)
        }
      }
    }
  }

  test("endpoint Unicode preserves valid path and query bytes and ignores unused URL defaults") {
    val unicode = "caf\u00e9-\u96ea-\uD83D\uDE00-\uFFFD"
    val encoded = "caf%C3%A9-%E9%9B%AA-%F0%9F%98%80-%EF%BF%BD"
    val data = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
    val protocols = cases :+ ("systemone", "providers/microsoft/v1/systemone", NativeRequestFixtures.SystemResponse)
    for ((api, route, reply) <- protocols; custom <- Seq(false, true)) {
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val suffix = "/" + (if (api == "systemone") route else "v1/" + route)
        val address = server.url + "/" + unicode + suffix + (if (custom) "?marker=" + unicode else "")
        val stage = new AIPrompt().setApiType(api).setModel("model").setInputCol("text").setErrorCol("error")
        if (custom) stage.setUrl(server.url + "/unused-\uD800").setCustomUrlRoot(address) else stage.setUrl(address)
        if (api == "systemone") stage.setQuestions(NativeRequestFixtures.SystemQuestions)
        else if (api == "decisions") stage.setQuestions(NativeRequestFixtures.DecisionQuestions)
        assert(stage.transformSchema(inputSchema) == stage.transform(data).schema)
        assert(stage.transform(data).head().getAs[Row]("error") == null)
        assert(server.requests.size() == 1)
        assert(server.requests.peek()._1 == "/" + encoded + suffix + (if (custom) "?marker=" + encoded else ""))
      }
    }
  }

  test("dated API versions reject malformed scalar and default Unicode before HTTP") {
    val input = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
    val previous = GlobalParams.getGlobalParam(OpenAIApiVersion)
    try {
      cases.filterNot(_._1 == "decisions").foreach { case (api, _, reply) =>
        Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
          for (malformed <- Seq("private-\uD800", "private-\uDC00");
               binding <- Seq("explicit", "default", "global")) {
            GlobalParams.resetGlobalParam(OpenAIApiVersion)
            val stage = new TestableAIMutableJsonDefaultPrompt("version-unicode").setApiType(api)
              .setUrl(server.url).setDeploymentName("model").setInputCol("text")
            binding match {
              case "explicit" => stage.setApiVersion(malformed)
              case "default" => stage.replaceJsonDefault(stage.apiVersion, Left(malformed))
              case "global" => GlobalParams.setGlobalParam(OpenAIApiVersion, Left(malformed))
            }
            Seq[() => Any](() => stage.transformSchema(inputSchema), () => stage.transform(input)).foreach { invoke =>
              val error = intercept[IllegalArgumentException](invoke())
              assert(error.getMessage.contains("surrogate") && !error.getMessage.contains("private-"))
              assert(error.getMessage.contains("apiVersion"))
              assert(error.getCause == null)
            }
            assert(server.requests.isEmpty)
          }
        }
      }
    } finally {
      previous match {
        case Some(value) => GlobalParams.setGlobalParam(OpenAIApiVersion, value)
        case None => GlobalParams.resetGlobalParam(OpenAIApiVersion)
      }
    }
  }

  test("dated API version columns reject malformed Unicode and preserve valid URL encoding") {
    val unicode = "caf\u00e9 \u96ea \uD83D\uDE00 \uFFFD &+?/"
    val versions = Seq("\uD800", "\uDC00", unicode, "\\" + "ud800")
    val schema = new StructType().add("text", StringType).add("version", StringType)
    val data = spark.range(versions.size).map(index => Row("hello", versions(index.toInt)))(Encoders.row(schema))
    val reply = """{"data":[{"index":0,"embedding":[0.25,0.5]}]}"""
    Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
      val stage = new AIPrompt().setApiType("embeddings").setUrl(server.url).setDeploymentName("model")
        .setInputCol("text").setApiVersionCol("version").setOutputCol("answer").setErrorCol("error")
      assert(stage.transformSchema(schema).fieldNames.contains("answer"))
      val rows = stage.transform(data).collect()
      val errors = rows.filter(_.getAs[Row]("error") != null)
      assert(rows.length == 4 && errors.length == 2 && server.requests.size() == 2)
      assert(errors.forall(row => row.getAs[Row]("error").getString(0).contains("invalid_request") &&
        row.getAs[Row]("error").getString(0).contains("surrogate") &&
        row.getAs[Row]("error").getString(0).contains("apiVersion") && row.getAs[Any]("answer") == null))
      val queries = server.requests.asScala.map(_._1.split("\\?api-version=", 2).last).toSet
      assert(queries == Set("caf%C3%A9+%E9%9B%AA+%F0%9F%98%80+%EF%BF%BD+%26%2B%3F%2F", "%5Cud800"))
      assert(queries.map(value => URLDecoder.decode(value, "UTF-8")) == versions.drop(2).toSet)
    }
  }

  test("non-dated endpoints continue ignoring malformed API version defaults") {
    val input = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
    val protocols = cases :+ ("systemone", "providers/microsoft/v1/systemone", NativeRequestFixtures.SystemResponse)
    protocols.foreach { case (api, _, reply) =>
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val stage = new TestableAIMutableJsonDefaultPrompt("ignored-version").setApiType(api)
          .setUrl(if (api == "systemone") server.url else server.url + "/v1")
          .setModel("model").setInputCol("text").setErrorCol("error")
        stage.replaceJsonDefault(stage.apiVersion, Left("private-\uD800"))
        if (api == "systemone") stage.setQuestions(NativeRequestFixtures.SystemQuestions)
        else if (api == "decisions") stage.setQuestions(NativeRequestFixtures.DecisionQuestions)
        assert(stage.transform(input).head().getAs[Row]("error") == null)
        assert(server.requests.size() == 1 && !server.requests.peek()._1.contains("api-version"))
      }
    }
  }

  Seq(false, true).foreach { explicit =>
    test(s"Responses forwards sampling options for GPT-5-named deployments with explicit=$explicit") {
      val previousTemperature = GlobalParams.getGlobalParam(OpenAITemperatureKey)
      val previousTopP = GlobalParams.getGlobalParam(OpenAITopPKey)
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
          assert(server.requests.isEmpty)
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
      }
    }
  }

  test("Chat extraBody can select max_tokens for a dated route when typed token options are unset") {
    val input = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
    val reply = """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""
    Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
      val stage = new AIPrompt().setUrl(server.url).setDeploymentName("model").setApiVersion("2023-05-15")
        .setInputCol("text").setOutputCol("answer").setErrorCol("error").setExtraBody(Map("max_tokens" -> 32))
      val row = stage.transform(input).head()
      assert(row.getAs[String]("answer") == "answer" && row.getAs[Row]("error") == null)
      assert(server.requests.size() == 1)
      val (url, body) = server.requests.peek()
      assert(url == "/openai/deployments/model/chat/completions?api-version=2023-05-15")
      val fields = AIJson.objectString(body).fields
      assert(fields("max_tokens") == JsNumber(32))
      assert(!fields.contains("max_completion_tokens") && !fields.contains("max_output_tokens"))
    }
  }

  test("invalid endpoint ports fail preflight") {
    val input = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
    Seq(0, 65536, Int.MaxValue).foreach { port =>
      val endpoint = s"http://127.0.0.1:$port/v1"
      Seq(false, true).foreach { custom =>
        val stage = new AIPrompt().setModel("model").setInputCol("text")
        if (custom) stage.setCustomUrlRoot(endpoint) else stage.setUrl(endpoint)
        Seq[() => Any](() => stage.transformSchema(inputSchema), () => stage.transform(input)).foreach { call =>
          val error = intercept[IllegalArgumentException](call())
          assert(error.getMessage.contains("port") && !error.getMessage.contains(endpoint))
        }
      }
    }
    Seq("", ":1", ":65535").foreach { port =>
      val stage = new AIPrompt().setUrl(s"http://127.0.0.1$port/v1").setModel("model").setInputCol("text")
      stage.transformSchema(inputSchema)
    }
  }

  test("underscore endpoint hosts report the unsupported host before execution") {
    val input = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
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
        val calls = Seq[() => Any](() => stage.transformSchema(inputSchema), () => stage.transform(input))
        calls.foreach { call =>
          val error = intercept[IllegalArgumentException](call())
          assert(error.getMessage.contains("host") && error.getMessage.contains("underscores"))
          assert(!error.getMessage.contains("credentials") && !error.getMessage.contains(endpoint))
        }
      }
    }
  }

  test("native endpoint guards recognize absolute DNS names and regional OpenAI hosts") {
    val input = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
    val endpoints = Seq(
      "systemone" -> "https://API.OPENAI.COM.",
      "systemone" -> "https://eu.api.openai.com",
      "systemone" -> "https://EU.API.OPENAI.COM.",
      "decisions" -> "https://resource.openai.azure.com./v1",
      "decisions" -> "https://resource.services.ai.azure.com./v1")
    endpoints.foreach { case (api, endpoint) =>
      val stage = new AIPrompt().setApiType(api).setUrl(endpoint).setModel("model").setInputCol("text")
        .setQuestions(if (api == "systemone") NativeRequestFixtures.SystemQuestions
          else NativeRequestFixtures.DecisionQuestions)
      Seq(false, true).foreach { custom =>
        if (custom) stage.setCustomUrlRoot(endpoint)
        val error = intercept[IllegalArgumentException](stage.transformSchema(inputSchema))
        assert(error.getMessage.contains("endpoint") && error.getCause == null)
        intercept[IllegalArgumentException](stage.transform(input))
      }
    }
  }

  Seq("azure.com", "azure.us", "azure.cn").foreach { cloud =>
    test(s"Decisions rejects $cloud endpoints before execution") {
      val input = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
      for {
        prefix <- Seq("resource.openai", "resource.services.ai", "resource.cognitiveservices")
        host <- Seq(s"$prefix.$cloud", s"$prefix.$cloud".toUpperCase(Locale.ROOT) + ".")
        route <- Seq("", "/v1", "/v1/decisions")
        stage <- Seq(new AIPrompt().setApiType("decisions"), new OpenAIDecisions())
        custom <- Seq(false, true)
      } {
        stage.setModel("model").setInputCol("text").setQuestions(NativeRequestFixtures.DecisionQuestions)
        val endpoint = s"https://$host$route"
        stage.clear(stage.customUrlRoot)
        if (custom) stage.setCustomUrlRoot(endpoint) else stage.setUrl(endpoint)
        Seq[() => Any](() => stage.transformSchema(inputSchema), () => stage.transform(input)).foreach { call =>
          val error = intercept[IllegalArgumentException](call())
          assert(error.getMessage.contains("not a Foundry endpoint") && error.getCause == null)
          assert(!error.getMessage.contains(host))
        }
      }
    }
  }

  test("Decisions Azure host guards preserve explicit non-Azure proxy endpoints") {
    Seq("gateway.azure.us.example.invalid", "gateway.azure.cn.example.invalid", "notazure.us").foreach { host =>
      val stage = new AIPrompt().setApiType("decisions").setUrl(s"https://$host/v1")
        .setModel("model").setInputCol("text").setQuestions(NativeRequestFixtures.DecisionQuestions)
      stage.transformSchema(inputSchema)
      stage.setCustomUrlRoot(s"https://$host/proxy?version=custom").transformSchema(inputSchema)
    }
  }

  cases.foreach { case (api, route, reply) =>
    test(s"$api normalizes bare OpenAI roots without changing stored URLs or applying dated defaults") {
      val input = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
      val previousVersion = GlobalParams.getGlobalParam(OpenAIApiVersion)
      try {
        GlobalParams.setGlobalParam(OpenAIApiVersion, Right("unavailableVersion"))
        Seq("https://api.openai.com", "https://eu.api.openai.com/", "https://US.API.OPENAI.COM.:443///")
          .foreach { endpoint =>
            val stage = new AIPrompt().setApiType(api).setUrl(endpoint).setModel("model")
              .setInputCol("text").setOutputCol("answer").setErrorCol("error").setSubscriptionKey("synthetic-key")
            if (api == "decisions") stage.setQuestions(NativeRequestFixtures.DecisionQuestions)
            val schema = stage.transformSchema(inputSchema)
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
            if (api == "embeddings") {
              stage.setApiVersion("explicit-version")
              intercept[IllegalArgumentException](stage.transformSchema(inputSchema))
              stage.setApiVersionCol("text")
              intercept[IllegalArgumentException](stage.transform(input))
            }
          }
      } finally {
        previousVersion match {
          case Some(value) => GlobalParams.setGlobalParam(OpenAIApiVersion, value)
          case None => GlobalParams.resetGlobalParam(OpenAIApiVersion)
        }
      }
    }
  }

  test("OpenAI root normalization leaves custom URL roots and other hosts unchanged") {
    val input = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
    Seq("api.openai.com", "EU.API.OPENAI.COM.", "proxy.example.invalid",
      "api.openai.com.example.invalid", "notapi.openai.com").foreach { host =>
      val custom = host == "api.openai.com" || host == "EU.API.OPENAI.COM."
      val endpoint = s"https://$host/"
      val stage = new AIPrompt().setModel("model").setInputCol("text")
        .setOutputCol("answer").setErrorCol("error")
      if (custom) stage.setCustomUrlRoot(endpoint)
      else stage.setUrl(endpoint).setApiVersion("explicit-version")
      val expected = if (custom) endpoint else
        endpoint + "openai/deployments/model/chat/completions?api-version=explicit-version"
      stage.setHandler { (_, request) =>
        assert(request.requestLine.uri == expected)
        HTTPSchema.stringToResponse(
          """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}""", 200, "OK")
      }
      val row = stage.transform(input).head()
      assert(row.getAs[Row]("error") == null && row.getAs[String]("answer") == "answer")
    }
  }

  cases.filterNot(_._1 == "decisions").foreach { case (api, _, reply) =>
    test(s"$api uses bearer keys for regional and absolute DNS OpenAI hosts only") {
      val input = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
      val hosts = Seq("api.openai.com." -> true, "EU.API.OPENAI.COM." -> true,
        "us.api.openai.com" -> true, "api.openai.com.example.invalid" -> false,
        "notapi.openai.com" -> false, "resource.openai.azure.com." -> false)
      hosts.foreach { case (host, bearer) =>
        val stage = new AIPrompt().setApiType(api).setUrl(s"https://$host/v1").setModel("model")
          .setInputCol("text").setOutputCol("answer").setErrorCol("error").setSubscriptionKey("synthetic-key")
        stage.setHandler { (_, request) =>
          val headers = request.headers.map(h => h.name.toLowerCase(Locale.ROOT) -> h.value).toMap
          assert(headers.get("authorization") == (if (bearer) Some("Bearer synthetic-key") else None))
          assert(headers.get("api-key") == (if (bearer) None else Some("synthetic-key")))
          HTTPSchema.stringToResponse(reply, 200, "OK")
        }
        val row = stage.transform(input).head()
        assert(row.getAs[Row]("error") == null && row.getAs[Any]("answer") != null)
      }
    }
  }

  test("complete Azure deployment routes named v1 retain dated API versions") {
    val input = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
    val previous = GlobalParams.getGlobalParam(OpenAIApiVersion)
    try {
      GlobalParams.setGlobalParam(OpenAIApiVersion, Left("global-version"))
      cases.filter(entry => Set("chat_completions", "embeddings")(entry._1)).foreach { case (api, route, reply) =>
        Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
          Seq("v1", "V1").foreach { deployment =>
            val suffix = s"/openai/deployments/$deployment/$route"
            val stage = new AIPrompt().setApiType(api).setUrl(server.url + suffix)
              .setDeploymentName(deployment).setInputCol("text").setOutputCol("answer").setErrorCol("error")
            Seq(None, Some("explicit-version"), Some("column-version")).foreach { version =>
              version.foreach {
                case "column-version" => stage.setApiVersionCol("text")
                case value => stage.setApiVersion(value)
              }
              assert(stage.transform(input).head().getAs[Row]("error") == null)
              val expected = version.map {
                case "column-version" => "hello"
                case value => value
              }.getOrElse("global-version")
              assert(server.requests.asScala.last._1 == suffix + s"?api-version=$expected")
            }
          }
          assert(server.requests.size() == 6)
        }
      }
    } finally {
      previous match {
        case Some(value) => GlobalParams.setGlobalParam(OpenAIApiVersion, value)
        case None => GlobalParams.resetGlobalParam(OpenAIApiVersion)
      }
    }
  }

  test("incomplete Azure deployment bases reject before HTTP instead of matching operation names") {
    val input = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
    Using.resource(new AIRecordingServer((_, _) => 200 -> "{}")) { server =>
      Seq("embeddings", "responses", "v1", "model").foreach { deployment =>
        Seq("chat_completions", "responses", "embeddings").foreach { api =>
          val stage = new AIPrompt().setApiType(api).setUrl(server.url + s"/openai/deployments/$deployment")
            .setDeploymentName(deployment).setInputCol("text")
          val error = intercept[IllegalArgumentException](stage.transform(input))
          assert(error.getMessage.contains("resource endpoint or a complete API route"))
        }
      }
      assert(server.requests.isEmpty)
    }
  }

  test("v1 base routing preserves legacy case detection and normalizes redundant trailing slashes") {
    val input = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
    cases.foreach { case (api, route, reply) =>
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        Seq("/v1", "/openai/V1").foreach { base =>
          Seq("", "/", "///").foreach { trailing =>
            val stage = new AIPrompt().setApiType(api).setUrl(server.url + base + trailing)
              .setModel("model").setInputCol("text").setOutputCol("answer").setErrorCol("error")
            if (api == "decisions") stage.setQuestions(NativeRequestFixtures.DecisionQuestions)
            val row = stage.transform(input).head()
            assert(row.getAs[Row]("error") == null && row.getAs[Any]("answer") != null)
            val (address, body) = server.requests.asScala.last
            assert(address == s"$base/$route")
            assert(AIJson.objectString(body).fields("model") == JsString("model"))
            stage.setApiVersion("unused")
            if (api == "embeddings") intercept[IllegalArgumentException](stage.transformSchema(inputSchema))
            else {
              assert(stage.transform(input).head().getAs[Row]("error") == null)
              assert(server.requests.asScala.last._1 == s"$base/$route?api-version=unused")
            }
          }
        }
        assert(server.requests.size() == (if (api == "embeddings") 6 else 12))
      }
    }
  }

  test("complete v1 overrides keep path and query bytes without adding routes or versions") {
    val input = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
    cases.foreach { case (api, route, reply) =>
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val suffix = s"/openai/V1/$route///?value=A%2Fb%2F&case=A+B"
        val stage = new AIPrompt().setApiType(api).setCustomUrlRoot(server.url + suffix)
          .setModel("model").setInputCol("text").setOutputCol("answer").setErrorCol("error")
        if (api == "decisions") stage.setQuestions(NativeRequestFixtures.DecisionQuestions)
        assert(stage.transform(input).head().getAs[Row]("error") == null)
        assert(server.requests.size() == 1 && server.requests.peek()._1 == suffix)
      }
    }
  }

  test("complete mixed-case v1 URLs preserve their destination with explicit query versions") {
    val input = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
    cases.foreach { case (api, route, reply) =>
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        Seq(route, route.toUpperCase(Locale.ROOT)).foreach { operation =>
          val suffix = s"/openai/V1/$operation"
          val stage = new AIPrompt().setApiType(api).setUrl(server.url + suffix + "///")
            .setModel("model").setInputCol("text").setOutputCol("answer").setErrorCol("error")
          if (api == "decisions") stage.setQuestions(NativeRequestFixtures.DecisionQuestions)
          assert(stage.transform(input).head().getAs[Row]("error") == null)
          assert(server.requests.asScala.last._1 == suffix)
          stage.setApiVersion("unused")
          if (api == "embeddings") {
            intercept[IllegalArgumentException](stage.transformSchema(inputSchema))
            stage.setApiVersionCol("text")
            intercept[IllegalArgumentException](stage.transformSchema(inputSchema))
          } else {
            assert(stage.transform(input).head().getAs[Row]("error") == null)
            assert(server.requests.asScala.last._1 == suffix + "?api-version=unused")
            assert(stage.setApiVersionCol("text").transform(input).head().getAs[Row]("error") == null)
            assert(server.requests.asScala.last._1 == suffix + "?api-version=hello")
          }
        }
        assert(server.requests.size() == (if (api == "embeddings") 2 else 6))
      }
    }
  }

  test("route classification does not decode encoded path separators") {
    val input = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
    cases.filterNot(_._1 == "decisions").foreach { case (api, route, reply) =>
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val base = "/proxy%2Fv1%2F" + route.replace("/", "%2F")
        val stage = new AIPrompt().setApiType(api).setUrl(server.url + base)
          .setModel("Model").setInputCol("text").setOutputCol("answer").setErrorCol("error")
        assert(stage.transform(input).head().getAs[Row]("error") == null)
        val suffix = if (api == "responses") "/openai/responses"
          else s"/openai/deployments/Model/$route"
        assert(server.requests.size() == 1)
        assert(server.requests.peek()._1 == base + suffix + "?api-version=2025-04-01-preview")
      }
    }
    Using.resource(new AIRecordingServer((_, _) => 200 -> "{}")) { server =>
      val decisions = new AIPrompt().setApiType("decisions").setUrl(server.url + "/proxy%2Fv1%2Fdecisions")
        .setModel("model").setInputCol("text").setQuestions(NativeRequestFixtures.DecisionQuestions)
      val systemone = new AISystemOne().setUrl(server.url + "/providers%2Fmicrosoft%2Fv1%2Fsystemone")
        .setModel("model").setInputCol("text").setQuestions(NativeRequestFixtures.SystemQuestions)
      Seq(decisions, systemone).foreach(stage =>
        intercept[IllegalArgumentException](stage.transform(input)))
      assert(server.requests.isEmpty)
    }
  }

  test("mixed-case complete routes still validate deployment identity and API selection") {
    val input = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
    Using.resource(new AIRecordingServer((_, _) => 200 -> cases.head._3)) { server =>
      val suffix = "/OPENAI/DEPLOYMENTS/Model/CHAT/COMPLETIONS"
      val stage = new AIPrompt().setUrl(server.url + suffix).setDeploymentName("Model").setInputCol("text")
        .setOutputCol("answer").setErrorCol("error")
      assert(stage.transform(input).head().getAs[Row]("error") == null)
      assert(server.requests.peek()._1 == suffix + "?api-version=2025-04-01-preview")
      stage.setDeploymentName("model")
      intercept[IllegalArgumentException](stage.transform(input))
      stage.setDeploymentName("Model").setApiType("embeddings")
      intercept[IllegalArgumentException](stage.transform(input))
      stage.setApiType("chat_completions").setUrl(server.url + "/MODELS/CHAT/COMPLETIONS")
      intercept[IllegalArgumentException](stage.transform(input))
      assert(server.requests.size() == 1)
    }
    Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.SystemResponse)) { server =>
      val suffix = "/PROVIDERS/MICROSOFT/V1/SYSTEMONE"
      val stage = new AIPrompt().setApiType("systemone").setUrl(server.url + suffix)
        .setModel("model").setInputCol("text")
        .setQuestions(NativeRequestFixtures.SystemQuestions).setOutputCol("answer").setErrorCol("error")
      assert(stage.transform(input).head().getAs[Row]("error") == null)
      assert(server.requests.size() == 1 && server.requests.peek()._1 == suffix)
      stage.setApiType("decisions").setQuestions(NativeRequestFixtures.DecisionQuestions)
      intercept[IllegalArgumentException](stage.transform(input))
    }
  }
}
