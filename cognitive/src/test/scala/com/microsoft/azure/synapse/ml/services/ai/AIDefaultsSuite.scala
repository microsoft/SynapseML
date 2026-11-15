// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.param.{GlobalKey, GlobalParams}
import com.microsoft.azure.synapse.ml.services.language.AnalyzeText
import com.microsoft.azure.synapse.ml.services.openai.{OpenAIDefaults, OpenAIDecisions, OpenAIPrompt}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{MapType, StringType, StructType}
import spray.json.{JsNumber, JsString, JsonParser}

import scala.collection.JavaConverters._
import scala.util.Using

class AIDefaultsSuite extends AIPromptTestBase {
  private val chatReply =
    """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"},"finish_reason":"stop"}]}"""
  private val responseReply = """{"status":"completed","output":[{"type":"message","role":"assistant",
    |"content":[{"type":"output_text","text":"answer"}]}]}""".stripMargin

  private def input: DataFrame =
    spark.createDataFrame(Seq(Row("hello", "column-model")).asJava,
      new StructType().add("text", StringType).add("model", StringType))

  private def isolated(body: => Unit): Unit = {
    val previous = AIDefaults.LegacyKeys.values.map(_.asInstanceOf[GlobalKey[Any]])
      .map(key => key -> GlobalParams.getGlobalParam(key)).toSeq
    AIDefaults.resetAll()
    try body finally {
      AIDefaults.resetAll()
      previous.foreach { case (key, value) => value.foreach(GlobalParams.setGlobalParam(key, _)) }
    }
  }

  private def stage(api: String = "chat_completions"): AIPrompt =
    new AIPrompt().setApiType(api).setInputCol("text").setOutputCol("answer").setErrorCol("error")

  test("legacy and unified defaults share values and resets in both directions") { isolated {
    val defaults = AIDefaults.forApi("openai")
    OpenAIDefaults.setURL("https://legacy.example.invalid/v1")
    OpenAIDefaults.setSubscriptionKey("synthetic-legacy")
    OpenAIDefaults.setDeploymentName("legacy-model")
    assert(defaults.getEndpoint.contains("https://legacy.example.invalid/v1"))
    assert(defaults.getApiKey.contains("synthetic-legacy"))
    assert(defaults.getDeploymentName.contains("legacy-model"))
    defaults.setEndpoint("https://new.example.invalid/v1").setApiKey("synthetic-new")
      .setModel("foundry-model").setTemperature(0.5).setApiVersion("version")
      .setEmbeddingDeploymentName("embedding").setTopP(0.8).setSeed(7)
      .setVerbosity("low").setReasoningEffort("high").setApiType("responses")
    assert(OpenAIDefaults.getURL == defaults.getEndpoint)
    assert(OpenAIDefaults.getSubscriptionKey == defaults.getApiKey)
    assert(OpenAIDefaults.getModel == defaults.getModel)
    assert(OpenAIDefaults.getTemperature == defaults.getTemperature)
    assert(OpenAIDefaults.getApiVersion == defaults.getApiVersion)
    assert(OpenAIDefaults.getEmbeddingDeploymentName == defaults.getEmbeddingDeploymentName)
    assert(OpenAIDefaults.getTopP == defaults.getTopP && OpenAIDefaults.getSeed == defaults.getSeed)
    assert(OpenAIDefaults.getVerbosity == defaults.getVerbosity)
    assert(OpenAIDefaults.getReasoningEffort == defaults.getReasoningEffort)
    assert(OpenAIDefaults.getApiType == defaults.getApiType)
    OpenAIDefaults.resetSubscriptionKey()
    defaults.resetEndpoint()
    assert(defaults.getApiKey.isEmpty && OpenAIDefaults.getURL.isEmpty)
    defaults.reset()
    assert(AIDefaults.LegacyKeys.values.forall(key =>
      GlobalParams.getGlobalParam(key.asInstanceOf[GlobalKey[Any]]).isEmpty))
  }}

  test("public requests use API-specific defaults ahead of legacy values and honor explicit columns") { isolated {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      OpenAIDefaults.setURL("https://unused.example.invalid")
      OpenAIDefaults.setSubscriptionKey("synthetic-legacy")
      OpenAIDefaults.setDeploymentName("legacy-model")
      val defaults = AIDefaults.forApi("chat_completions")
        .setEndpoint(server.url + "/v1").setApiKey("synthetic-scoped").setModel("scope-model")
      val original = stage()
      val before = original.extractParamMap().toSeq.toSet
      assert(original.transformSchema(input.schema).fieldNames.contains("answer"))
      assert(original.transform(input).head().getAs[String]("answer") == "answer")
      assert(original.extractParamMap().toSeq.toSet == before)
      assert(JsonParser(server.requests.peek()._2).asJsObject.fields("model") == JsString("scope-model"))
      assert(server.headers.peek().get("api-key").contains("synthetic-scoped"))
      assert(original.setModelCol("model").setSubscriptionKey("synthetic-explicit")
        .transform(input).head().getAs[String]("answer") == "answer")
      assert(JsonParser(server.requests.asScala.last._2).asJsObject.fields("model") == JsString("column-model"))
      assert(server.headers.asScala.last.get("api-key").contains("synthetic-explicit"))
      defaults.resetModel()
      assert(stage().effectiveCopy().getDeploymentName == "legacy-model")
      defaults.reset()
      AIDefaults.forApi("openai").setEndpoint(server.url + "/v1").setApiKey("synthetic-common")
      defaults.setModel("model-only-override")
      stage().transform(input).collect()
      assert(server.headers.asScala.last.get("api-key").contains("synthetic-common"))
      assert(JsonParser(server.requests.asScala.last._2).asJsObject.fields("model") == JsString("model-only-override"))
    }
  }}

  test("legacy OpenAIPrompt consumes settings written through the unified openai scope") { isolated {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      AIDefaults.forApi("openai").setEndpoint(server.url + "/v1").setDeploymentName("legacy-compatible")
        .setApiKey("synthetic-shared").setApiType("chat_completions")
      val legacy = new OpenAIPrompt().setPromptTemplate("{text}").setOutputCol("answer").setErrorCol("error")
      assert(legacy.transform(input).head().getAs[String]("answer") == "answer")
      assert(server.headers.peek().get("api-key").contains("synthetic-shared"))
    }
  }}

  test("embeddings scope overrides the legacy embedding deployment and reset restores that fallback") { isolated {
    val reply = """{"data":[{"index":0,"embedding":[0.25,0.5]}],"model":"returned-model"}"""
    Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
      OpenAIDefaults.setEmbeddingDeploymentName("legacy-embedding")
      val defaults = AIDefaults.forApi("embeddings").setEndpoint(server.url + "/v1").setModel("scoped-embedding")
      stage("embeddings").transform(input).collect()
      assert(JsonParser(server.requests.peek()._2).asJsObject.fields("model") == JsString("scoped-embedding"))
      defaults.resetModel()
      stage("embeddings").transform(input).collect()
      assert(JsonParser(server.requests.asScala.last._2).asJsObject.fields("model") == JsString("legacy-embedding"))
    }
  }}

  test("Responses scopes and persisted unresolved stages observe current defaults without storing secrets") { isolated {
    Using.resource(new AIRecordingServer((_, _) => 200 -> responseReply)) { server =>
      val defaults = AIDefaults.forApi("responses").setEndpoint(server.url + "/v1")
        .setApiKey("synthetic-responses").setModel("first")
      AIDefaults.forApi("chat_completions").setEndpoint("https://unused.example.invalid").setModel("other")
      val original = stage("responses")
      val saved = path()
      original.write.save(saved)
      val loaded = AIPrompt.load(saved)
      defaults.setModel("second")
      Seq(original.copy(ParamMap.empty), loaded).foreach { candidate =>
        assert(!candidate.isSet(candidate.url) && !candidate.isSet(candidate.subscriptionKey))
        assert(candidate.transform(input).head().getAs[String]("answer") == "answer")
      }
      assert(server.requests.asScala.forall { case (url, body) =>
        url == "/v1/responses" && JsonParser(body).asJsObject.fields("model") == JsString("second")
      })
    }
  }}

  test("keys do not cross origins or fall back from an API-specific destination to legacy credentials") { isolated {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { first =>
      Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { second =>
        OpenAIDefaults.setSubscriptionKey("synthetic-legacy")
        val defaults = AIDefaults.forApi("chat_completions").setEndpoint(first.url + "/v1")
          .setApiKey("synthetic-first").setModel("model")
        stage().setUrl(second.url + "/v1").transform(input).collect()
        assert(!second.headers.peek().contains("api-key"))
        defaults.resetApiKey()
        stage().transform(input).collect()
        assert(!first.headers.peek().contains("api-key"))
        defaults.setApiKey("synthetic-first")
        stage().setAADToken("synthetic-aad").transform(input).collect()
        assert(first.headers.asScala.last.get("authorization").contains("Bearer synthetic-aad"))
        assert(!first.headers.asScala.last.contains("api-key"))
        stage().setCustomHeaders(Map("X-Correlation" -> "example")).transform(input).collect()
        assert(first.headers.asScala.last.get("api-key").contains("synthetic-first"))
        stage().setCustomHeaders(Map("Authorization" -> "Bearer synthetic-custom")).transform(input).collect()
        assert(first.headers.asScala.last.get("authorization").contains("Bearer synthetic-custom"))
        assert(!first.headers.asScala.last.contains("api-key"))
      }
    }
  }}

  test("native APIs use their own defaults without inheriting OpenAI endpoints keys or models") { isolated {
    OpenAIDefaults.setURL("https://unrelated.example.invalid/v1")
    OpenAIDefaults.setSubscriptionKey("synthetic-openai")
    OpenAIDefaults.setModel("unrelated")
    AIDefaults.forApi("systemone").setEndpoint("https://system.example.invalid").setModel("system-model")
    AIDefaults.forApi("decisions").setEndpoint("https://decision.example.invalid/v1").setModel("decision-model")
    Seq(new AISystemOne() -> "system-model", new OpenAIDecisions() -> "decision-model").foreach {
      case (native, model) =>
        val resolved = native.effectiveCopy()
        assert(resolved.getModel == model)
        assert(!resolved.isDefined(resolved.subscriptionKey))
        assert(resolved.getUrl != OpenAIDefaults.getURL.get)
    }
  }}

  test("API versions are scoped on dated endpoints but inactive on v1 endpoints") { isolated {
    OpenAIDefaults.setURL("https://example.services.ai.azure.com")
    OpenAIDefaults.setApiVersion("legacy-version")
    val defaults = AIDefaults.forApi("chat_completions").setModel("model").setApiVersion("scope-version")
    assert(stage().effectiveCopy().getApiVersion == "scope-version")
    assert(stage().setApiVersion("explicit-version").effectiveCopy().getApiVersion == "explicit-version")
    defaults.setEndpoint("https://example.services.ai.azure.com/v1")
    val versionless = stage().effectiveCopy()
    assert(!versionless.isSet(versionless.apiVersion))
    stage().transformSchema(input.schema)
  }}

  test("review regression: header columns retain scoped credentials only when rows have no authentication") { isolated {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      AIDefaults.forApi("chat_completions").setEndpoint(server.url + "/v1")
        .setApiKey("synthetic-fallback").setModel("model")
      val headers = Seq(Map.empty[String, String], Map("X-Correlation" -> "example"),
        Map("Authorization" -> "Bearer synthetic-row"), Map("api-key" -> "synthetic-row-key"),
        Map("Authorization" -> ""), Option.empty[Map[String, String]].orNull)
      val data = spark.createDataFrame(headers.map(h => Row("hello", h)).asJava,
        new StructType().add("text", StringType).add("headers", MapType(StringType, StringType)))
      val prompt = stage()
      prompt.setVectorParam(prompt.customHeaders, "headers")
      assert(prompt.transform(data).collect().forall(_.getAs[String]("answer") == "answer"))
      val sent = server.headers.asScala.toSeq
      assert(sent.count(_.get("api-key").contains("synthetic-fallback")) == 4)
      assert(sent.count(_.get("authorization").contains("Bearer synthetic-row")) == 1)
      assert(sent.count(_.get("api-key").contains("synthetic-row-key")) == 1)
      prompt.setSubscriptionKey("synthetic-explicit").transform(data).collect()
      assert(server.headers.asScala.toSeq.takeRight(headers.size)
        .forall(_.get("api-key").contains("synthetic-explicit")))
      prompt.clear(prompt.subscriptionKey)
      AIDefaults.forApi("chat_completions").setApiKey("\u0100")
      val authenticated = data.filter("headers['Authorization'] = 'Bearer synthetic-row'")
      assert(prompt.transform(authenticated).head().getAs[String]("answer") == "answer")
    }
  }}

  test("review regression: Decisions versions reach URLs and respect explicit or complete URL overrides") { isolated {
    Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
      AIDefaults.forApi("decisions").setEndpoint(server.url + "/v1").setModel("model").setApiVersion("preview")
      val prompt = stage("decisions").setQuestions(NativeRequestFixtures.DecisionQuestions)
      prompt.transform(input).collect()
      assert(server.requests.peek()._1 == "/v1/decisions?api-version=preview")
      prompt.setApiVersion("explicit").transform(input).collect()
      assert(server.requests.asScala.last._1 == "/v1/decisions?api-version=explicit")
      prompt.clear(prompt.apiVersion)
      prompt.setCustomUrlRoot(server.url + "/v1/decisions?api-version=complete").transform(input).collect()
      assert(server.requests.asScala.last._1 == "/v1/decisions?api-version=complete")
    }
  }}

  test("review regression: extraBody overrides scoped generation defaults but not explicit typed options") { isolated {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      OpenAIDefaults.setTemperature(0.9)
      AIDefaults.forApi("chat_completions").setEndpoint(server.url + "/v1").setModel("model").setTemperature(0.5)
      val prompt = stage()
      prompt.transform(input).collect()
      assert(JsonParser(server.requests.peek()._2).asJsObject.fields("temperature") == JsNumber(0.5))
      prompt.setExtraBody(Map("temperature" -> 0.2)).transform(input).collect()
      assert(JsonParser(server.requests.asScala.last._2).asJsObject.fields("temperature") == JsNumber(0.2))
      intercept[IllegalArgumentException] {
        prompt.setTemperature(0.7).transformSchema(input.schema)
      }
    }
  }}

  test("Foundry content settings stay isolated and invalid scopes fields or values leave prior values intact") {
    isolated {
      val content = AIDefaults.forApi("content_understanding").setEndpoint("https://cu.example.invalid")
        .setApiKey("synthetic-cu").setAnalyzerId("prebuilt-read")
        .setModelDeployments(Map("completion" -> "deployment")).setAllowInputTruncation(false)
      assert(AIDefaults.forApi("openai").getEndpoint.isEmpty)
      val unrelated = stage().effectiveCopy()
      assert(!unrelated.isDefined(unrelated.url))
      assert(content.getModelDeployments.contains(Map("completion" -> "deployment")))
      assert(content.getAllowInputTruncation.contains(false))
      intercept[IllegalArgumentException](AIDefaults.forApi("typo"))
      intercept[IllegalArgumentException](content.setModel("wrong"))
      intercept[IllegalArgumentException](content.setEndpoint("https://user:password@example.invalid"))
      intercept[IllegalArgumentException](content.setApiKey(Option.empty[String].orNull))
      intercept[IllegalArgumentException](content.applyTo(new AnalyzeText()))
      val chat = AIDefaults.forApi("chat_completions").setModel("model").setTemperature(0.5)
      intercept[IllegalArgumentException](chat.setDeploymentName("ambiguous"))
      intercept[IllegalArgumentException](chat.setTemperature(Double.NaN))
      assert(chat.getTemperature.contains(0.5))
      assert(content.getEndpoint.contains("https://cu.example.invalid"))
      chat.setApiKey("synthetic-unbound")
      intercept[IllegalArgumentException](stage().effectiveCopy())
      content.reset()
      assert(content.getModelDeployments.isEmpty && content.getAllowInputTruncation.isEmpty)
    }
  }
}
