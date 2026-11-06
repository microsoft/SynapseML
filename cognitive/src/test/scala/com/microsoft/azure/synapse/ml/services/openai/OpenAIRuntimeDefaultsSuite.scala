// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.param.HasGlobalParams
import com.microsoft.azure.synapse.ml.services.HasCognitiveServiceInput
import com.microsoft.azure.synapse.ml.services.aifoundry.AIFoundryChatCompletion
import com.microsoft.azure.synapse.ml.services.language.AnalyzeText
import org.apache.http.entity.AbstractHttpEntity
import org.apache.http.util.EntityUtils
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import spray.json._

class OpenAIRuntimeDefaultsSuite extends TestBase {
  import spark.implicits._

  private type TextStage = HasOpenAITextParams with HasCognitiveServiceInput with HasGlobalParams

  private val messageSchema = new StructType().add("role", StringType).add("content", StringType)
  private val schema = new StructType().add("messages", ArrayType(messageSchema)).add("deployment", StringType)
  private val prepareEntity = classOf[HasCognitiveServiceInput].getDeclaredMethod("prepareEntity")
  prepareEntity.setAccessible(true)
  private val prepareUrl = classOf[HasCognitiveServiceInput].getDeclaredMethod("prepareUrl")
  prepareUrl.setAccessible(true)

  private def stages: Seq[TextStage] = Seq(
    new OpenAIChatCompletion().setMessagesCol("messages"),
    new OpenAIResponses().setMessagesCol("messages"),
    new OpenAIPrompt().setMessagesCol("messages").setApiType("chat_completions"),
    new OpenAIPrompt().setMessagesCol("messages").setApiType("responses")
  ).map(_.setUrl("https://example.openai.azure.com/openai/v1/"))

  private def row(deployment: String = "row-deployment"): Row = {
    val message = new GenericRowWithSchema(Array[Any]("user", "hello"), messageSchema)
    new GenericRowWithSchema(Array[Any](Seq(message), deployment), schema)
  }

  private def payload(stage: TextStage, input: Row = row()): JsObject = {
    val build = prepareEntity.invoke(stage).asInstanceOf[Row => Option[AbstractHttpEntity]]
    EntityUtils.toString(build(input).get).parseJson.asJsObject
  }

  test("unset OpenAI text deployments send Luna without creating a global default") {
    assert(OpenAIDefaults.getDeploymentName.isEmpty)
    stages.foreach { stage =>
      assert(!stage.isSet(stage.deploymentName))
      assert(payload(stage).fields("model") == JsString("gpt-6-luna"))
    }
    val chat = new OpenAIChatCompletion().setUrl("https://example.openai.azure.com/")
    val url = prepareUrl.invoke(chat).asInstanceOf[Row => String].apply(row())
    assert(url.contains("/openai/deployments/gpt-6-luna/chat/completions"))
    val responses = new OpenAIResponses().setUrl("https://example.openai.azure.com/").setMessagesCol("messages")
    assert(payload(responses).fields("model") == JsString("gpt-6-luna"))
    assert(OpenAIDefaults.getDeploymentName.isEmpty)
  }

  test("explicit scalar column and global deployments override the Luna fallback") {
    try {
      OpenAIDefaults.setDeploymentName("global-deployment")
      stages.foreach { stage =>
        stage.transferGlobalParamsToParamMap()
        assert(payload(stage).fields("model") == JsString("global-deployment"))
      }
      stages.foreach { stage =>
        stage.setDeploymentName("custom-deployment")
        stage.transferGlobalParamsToParamMap()
        assert(payload(stage).fields("model") == JsString("custom-deployment"))
        stage.setDeploymentNameCol("deployment")
        stage.transferGlobalParamsToParamMap()
        assert(payload(stage).fields("model") == JsString("row-deployment"))
      }
    } finally {
      OpenAIDefaults.resetDeploymentName()
    }
  }

  test("a null explicitly bound deployment never falls back to Luna") {
    stages.foreach { stage =>
      stage.setDeploymentNameCol("deployment")
      intercept[RuntimeException] {
        payload(stage, row(Option.empty[String].orNull))
      }
    }
  }

  test("prompt model aliases still override the fallback but not explicit deployments") {
    for (api <- Seq("chat_completions", "responses")) {
      val prompt = new OpenAIPrompt().setApiType(api)
        .setUrl("https://example.openai.azure.com/openai/v1/").setMessagesCol("messages")
        .setModel("custom-model")
      assert(payload(prompt).fields("model") == JsString("custom-model"))
      prompt.setDeploymentName("custom-deployment")
      assert(payload(prompt).fields("model") == JsString("custom-deployment"))
      prompt.setDeploymentNameCol("deployment")
      assert(payload(prompt).fields("model") == JsString("row-deployment"))
    }
    try {
      OpenAIDefaults.setModel("global-model")
      stages.collect { case prompt: OpenAIPrompt => prompt }.foreach { prompt =>
        prompt.transferGlobalParamsToParamMap()
        assert(payload(prompt).fields("model") == JsString("global-model"))
      }
    } finally {
      OpenAIDefaults.resetModel()
    }
  }

  test("embedding and Foundry model selection stay explicit") {
    val embedding = new OpenAIEmbedding()
    assert(!embedding.isDefined(embedding.deploymentName))
    Seq("https://example.services.ai.azure.com/", "https://example.services.ai.azure.com/openai/v1/")
      .foreach { url =>
        val foundry = new AIFoundryChatCompletion().setMessagesCol("messages")
          .setUrl(url).setModel("foundry-model")
        assert(!foundry.isDefined(foundry.deploymentName))
        assert(payload(foundry).fields("model") == JsString("foundry-model"))
        foundry.setDeploymentName("ignored-deployment")
        assert(payload(foundry).fields("model") == JsString("foundry-model"))
      }
    val prompt = new OpenAIPrompt().setMessagesCol("messages")
      .setUrl("https://example.services.ai.azure.com/").setModel("foundry-model")
    assert(payload(prompt).fields("model") == JsString("foundry-model"))
  }

  test("non-v1 Responses preserves an explicit model alias and nested reasoning options") {
    val prompt = new OpenAIPrompt().setApiType("responses").setMessagesCol("messages")
      .setUrl("https://example.openai.azure.com/").setModel("custom-model")
      .setVerbosity("low").setReasoningEffort("low").setStore(false)
    val result = payload(prompt)
    assert(result.fields.contains("input"))
    assert(!result.fields.contains("messages"))
    assert(result.fields("model") == JsString("custom-model"))
    assert(result.fields("text").asJsObject.fields("verbosity") == JsString("low"))
    assert(result.fields("reasoning").asJsObject.fields("effort") == JsString("low"))
  }

  test("saved non-OpenAI API versions are not replaced by OpenAI globals") {
    val input = Seq.empty[String].toDF("text")
    val path = tmpDir.resolve("saved-language-defaults").toString
    try {
      Seq(None, Some("saved-custom-version")).foreach { explicit =>
        OpenAIDefaults.resetApiVersion()
        val original = new AnalyzeText().setTextCol("text").setKind("SentimentAnalysis")
          .setUrl("https://example.cognitiveservices.azure.com/").setSubscriptionKey("unused")
        explicit.foreach(original.setApiVersion)
        val expectedVersion = original.getApiVersion
        original.write.overwrite().save(path)
        OpenAIDefaults.setApiVersion("2025-04-01-preview")
        val restored = AnalyzeText.load(path)
        assert(restored.transform(input).collect().isEmpty)
        assert(restored.getApiVersion == expectedVersion)
        val requestUrl = prepareUrl.invoke(restored).asInstanceOf[Row => String].apply(row())
        assert(requestUrl.contains(s"api-version=$expectedVersion"))
      }
    } finally {
      OpenAIDefaults.resetApiVersion()
    }
  }

  test("legacy Chat rejects model-only selection instead of silently using the fallback") {
    try {
      OpenAIDefaults.setModel("global-model")
      val explicit = new OpenAIPrompt().setModel("custom-model")
      val inherited = new OpenAIPrompt()
      Seq(explicit, inherited).foreach { prompt =>
        prompt.setUrl("https://example.openai.azure.com/").setMessagesCol("messages")
        prompt.transferGlobalParamsToParamMap()
        val error = intercept[IllegalArgumentException] {
          payload(prompt)
        }
        assert(error.getMessage.contains("setDeploymentName"))
        prompt.setDeploymentName("custom-deployment")
        assert(payload(prompt).fields.contains("messages"))
      }
    } finally {
      OpenAIDefaults.resetModel()
    }
  }

  test("legacy Chat custom URL roots preserve explicit and global model configurations") {
    val input = Seq.empty[String].toDF("text")
    val customRoot = "https://gateway.example.org/chat/completions"
    Seq(false, true).foreach { globalModel =>
      try {
        val prompt = new OpenAIPrompt().setUrl("https://example.openai.azure.com/")
          .setCustomUrlRoot(customRoot).setSubscriptionKey("unused")
          .setPromptTemplate("{text}").setMessagesCol("messages").setOutputCol("output").setErrorCol("error")
        if (globalModel) {
          OpenAIDefaults.setModel("global-model")
        } else {
          prompt.setModel("custom-model")
        }
        assert(prompt.transformSchema(input.schema).fieldNames.contains("output"))
        assert(!prompt.isSet(prompt.deploymentName))
        val result = new Pipeline().setStages(Array(prompt)).fit(input).transform(input)
        assert(result.collect().isEmpty)
        val body = payload(prompt)
        assert(body.fields.contains("messages"))
        assert(!body.fields.contains("model"))
        assert(prepareUrl.invoke(prompt).asInstanceOf[Row => String].apply(row()) == customRoot)
      } finally {
        OpenAIDefaults.resetModel()
      }
    }
  }

  for ((name, url, deployment, api) <- Seq(
    ("v1 URL", "https://example.openai.azure.com/openai/v1/", None, None),
    ("Foundry URL", "https://example.services.ai.azure.com/", None, None),
    ("deployment", "https://example.openai.azure.com/", Some("global-deployment"), None),
    ("Responses API", "https://example.openai.azure.com/", None, Some("responses"))
  )) {
    test(s"global prompt configuration resolves $name during schema validation and pipeline execution") {
      try {
        OpenAIDefaults.setURL(url)
        deployment.foreach(OpenAIDefaults.setDeploymentName)
        api.foreach(OpenAIDefaults.setApiType)
        def prompt: OpenAIPrompt = new OpenAIPrompt().setModel("custom-model")
          .setPromptTemplate("{text}").setSubscriptionKey("unused")
          .setOutputCol("output").setErrorCol("error")
        val input = Seq.empty[String].toDF("text")
        val schemaStage = prompt
        assert(schemaStage.transformSchema(input.schema).fieldNames.contains("output"))
        assert(!schemaStage.isSet(schemaStage.url))
        assert(!schemaStage.isSet(schemaStage.deploymentName))
        assert(!schemaStage.isSet(schemaStage.apiType))
        val result = new Pipeline().setStages(Array(prompt)).fit(input).transform(input)
        assert(result.columns.contains("output"))
        assert(result.collect().isEmpty)
      } finally {
        OpenAIDefaults.resetURL()
        OpenAIDefaults.resetDeploymentName()
        OpenAIDefaults.resetApiType()
      }
    }
  }

  test("schema-only validation does not materialize global credentials on the prompt") {
    try {
      OpenAIDefaults.setURL("https://example.openai.azure.com/openai/v1/")
      OpenAIDefaults.setSubscriptionKey("synthetic-schema-only-key")
      val prompt = new OpenAIPrompt().setModel("custom-model").setPromptTemplate("{text}")
      val before = prompt.extractParamMap().toSeq.toSet
      prompt.transformSchema(Seq.empty[String].toDF("text").schema)
      assert(!prompt.isSet(prompt.subscriptionKey))
      assert(prompt.extractParamMap().toSeq.toSet == before)
    } finally {
      OpenAIDefaults.resetURL()
      OpenAIDefaults.resetSubscriptionKey()
    }
  }

  test("schema-only validation resolves changed globals without freezing the first configuration") {
    try {
      val prompt = new OpenAIPrompt().setModel("custom-model").setPromptTemplate("{text}")
      val input = Seq.empty[String].toDF("text")
      OpenAIDefaults.setURL("https://example.openai.azure.com/openai/v1/")
      prompt.transformSchema(input.schema)
      OpenAIDefaults.setURL("https://example.openai.azure.com/")
      val error = intercept[IllegalArgumentException] {
        prompt.transformSchema(input.schema)
      }
      assert(error.getMessage.contains("setDeploymentName"))
      assert(!prompt.isSet(prompt.url))
    } finally {
      OpenAIDefaults.resetURL()
    }
  }

  test("fitting and saving a prompt pipeline does not persist schema-only global credentials") {
    try {
      val syntheticKey = "synthetic-fit-save-key"
      OpenAIDefaults.setURL("https://example.openai.azure.com/openai/v1/")
      OpenAIDefaults.setSubscriptionKey(syntheticKey)
      val prompt = new OpenAIPrompt().setModel("custom-model").setPromptTemplate("{text}")
      val fitted = new Pipeline().setStages(Array(prompt)).fit(Seq.empty[String].toDF("text"))
      val path = tmpDir.resolve("schema-only-pipeline").toString
      fitted.write.save(path)
      val metadata = spark.read.text(s"$path/stages/*/metadata/part-*").collect()
      assert(metadata.nonEmpty)
      assert(metadata.forall(row => !row.getString(0).contains(syntheticKey)))
      val restored = PipelineModel.load(path).stages.head.asInstanceOf[OpenAIPrompt]
      assert(!restored.isSet(restored.subscriptionKey))
      assert(!restored.isSet(restored.url))
      OpenAIDefaults.setSubscriptionKey("synthetic-later-key")
      restored.transferGlobalParamsToParamMap()
      assert(restored.getSubscriptionKey == "synthetic-later-key")
    } finally {
      OpenAIDefaults.resetURL()
      OpenAIDefaults.resetSubscriptionKey()
    }
  }
}
