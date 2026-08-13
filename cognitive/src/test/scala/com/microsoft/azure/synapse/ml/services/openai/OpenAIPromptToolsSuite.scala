// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class OpenAIPromptToolsSuite extends TestBase {
  import spark.implicits._

  private val inputSchema = StructType(Seq(StructField("city", StringType)))

  private def ensureSparkSession(): Unit =
    assert(!spark.sparkContext.isStopped)

  private def configuredPrompt: OpenAIPrompt = new OpenAIPrompt()
    .setApiType("responses")
    .setPromptTemplate("Weather in {city}?")
    .setDeploymentName("gpt-5.1")
    .setTools(ToolTestFixtures.WeatherToolJson)
    .setToolChoice("auto")
    .setParallelToolCalls(true)
    .setMaxToolCalls(4)
    .setInstructions("Use tools.")
    .setTruncation("auto")
    .setMetadata(Map("suite" -> "prompt"))
    .setInclude(Seq("reasoning.encrypted_content"))
    .setTopLogprobs(3)
    .setSafetyIdentifier("hashed-user")
    .setPromptCacheKey("weather")
    .setServiceTier("default")
    .setConversation("conv_1")
    .setReasoningSummary("concise")
    .setReasoningContext("current_turn")
    .setReasoningMode("standard")

  test("Prompt forwards every tool and modern Responses param by name") {
    val prompt = configuredPrompt
    val child = prompt.getOpenAIChatService.asInstanceOf[OpenAIResponses]
    assert(child.getTools === prompt.getTools)
    assert(child.getToolChoice === "auto")
    assert(child.getParallelToolCalls)
    assert(child.getMaxToolCalls === 4)
    assert(child.getInstructions === "Use tools.")
    assert(child.getTruncation === "auto")
    assert(child.getMetadata === Map("suite" -> "prompt"))
    assert(child.getInclude === Seq("reasoning.encrypted_content"))
    assert(child.getTopLogprobs === 3)
    assert(child.getSafetyIdentifier === "hashed-user")
    assert(child.getPromptCacheKey === "weather")
    assert(child.getServiceTier === "default")
    assert(child.getConversation === "conv_1")
    assert(child.getReasoningSummary === "concise")
    assert(child.getReasoningContext === "current_turn")
    assert(child.getReasoningMode === "standard")
    assert(!child.isSet(child.toolCallsCol))
    assert(!child.hasParam("responseStructCol"))
  }

  test("Prompt forwards column-bound tool and modern params") {
    val prompt = new OpenAIPrompt()
      .setApiType("responses")
      .setToolsCol("tools_json")
      .setToolChoiceCol("choice_json")
      .setParallelToolCallsCol("parallel")
      .setMaxToolCallsCol("max_calls")
      .setInstructionsCol("instructions")
      .setReasoningSummaryCol("summary")
    val child = prompt.getOpenAIChatService.asInstanceOf[OpenAIResponses]
    assert(child.getToolsCol === "tools_json")
    assert(child.getToolChoiceCol === "choice_json")
    assert(child.getParallelToolCallsCol === "parallel")
    assert(child.getMaxToolCallsCol === "max_calls")
    assert(child.getInstructionsCol === "instructions")
    assert(child.getReasoningSummaryCol === "summary")
  }

  test("Prompt forwards tools and structured outputs to Chat Completions") {
    ensureSparkSession()
    val prompt = new OpenAIPrompt()
      .setPromptTemplate("Weather in {city}?")
      .setDeploymentName("gpt-5.1")
      .setTools(ToolTestFixtures.WeatherToolJson)
      .setToolChoiceFunction("get_weather")
      .setParallelToolCalls(false)
      .setToolCallsCol("tool_calls")
      .setResponseStructCol("response_struct")
    val child = prompt.getOpenAIChatService.asInstanceOf[OpenAIChatCompletion]
    assert(OpenAIToolUtils.parseTools(child.getTools) ===
      OpenAIToolUtils.parseTools(prompt.getTools))
    assert(!child.getParallelToolCalls)

    val schema = prompt.transformSchema(inputSchema)
    assert(schema("tool_calls").dataType === OpenAIToolColumns.ToolCallStructType)
    assert(schema("response_struct").dataType === ChatModelResponseV2.schema)
  }

  test("Responses-only params still fail explicitly for chat completions") {
    val setters: Seq[(String, OpenAIPrompt => Unit)] = Seq(
      "maxToolCalls" -> (_.setMaxToolCalls(2)),
      "instructions" -> (_.setInstructions("i")),
      "truncation" -> (_.setTruncation("auto")),
      "metadata" -> (_.setMetadata(Map("k" -> "v"))),
      "include" -> (_.setInclude(Seq("reasoning.encrypted_content"))),
      "topLogprobs" -> (_.setTopLogprobs(1)),
      "safetyIdentifier" -> (_.setSafetyIdentifier("u")),
      "promptCacheKey" -> (_.setPromptCacheKey("k")),
      "serviceTier" -> (_.setServiceTier("default")),
      "conversation" -> (_.setConversation("c")),
      "reasoningSummary" -> (_.setReasoningSummary("auto")),
      "reasoningContext" -> (_.setReasoningContext("current_turn")),
      "reasoningMode" -> (_.setReasoningMode("standard"))
    )

    setters.foreach { case (name, setParam) =>
      val prompt = new OpenAIPrompt().setPromptTemplate("{city}")
      setParam(prompt)
      val error = intercept[IllegalArgumentException] {
        prompt.transformSchema(inputSchema)
      }
      assert(error.getMessage.contains(name), error.getMessage)
      assert(error.getMessage.contains("apiType='responses'"), error.getMessage)
    }
  }

  test("Prompt rejects tool calling on AI Foundry chat endpoints") {
    val prompt = new OpenAIPrompt()
      .setAIFoundryCustomServiceName("foundry-project")
      .setModel("gpt-5.1")
      .setPromptTemplate("{city}")
      .setTools(ToolTestFixtures.WeatherToolJson)
    val error = intercept[IllegalArgumentException] {
      prompt.transformSchema(inputSchema)
    }
    assert(error.getMessage.contains("AI Foundry chat endpoints"))
  }

  test("Prompt opt-in schema retains structured calls and parsed response") {
    ensureSparkSession()
    val prompt = new OpenAIPrompt()
      .setApiType("responses")
      .setPromptTemplate("Weather in {city}?")
      .setDeploymentName("gpt-5.1")
      .setTools(ToolTestFixtures.WeatherToolJson)
      .setToolCallsCol("tool_calls")
      .setResponseStructCol("response_struct")
      .setOutputCol("answer")
      .setErrorCol("error")

    val schema = prompt.transformSchema(inputSchema)
    assert(schema("tool_calls").dataType === OpenAIToolColumns.ToolCallStructType)
    assert(schema("response_struct").dataType === ResponsesModelResponseV2.schema)
    assert(schema("answer").dataType === StringType)
    assert(schema.fieldNames.last === "error")

    val defaultPrompt = new OpenAIPrompt()
      .setApiType("responses")
      .setPromptTemplate("{city}")
      .setDeploymentName("gpt-5.1")
    val defaultSchema = defaultPrompt.transformSchema(inputSchema)
    assert(!defaultSchema.fieldNames.contains("tool_calls"))
    assert(!defaultSchema.fieldNames.contains("response_struct"))
    assert(defaultSchema.fieldNames.last === defaultPrompt.getErrorCol)
  }

  test("Prompt output column collisions fail before execution") {
    ensureSparkSession()
    val prompt = new OpenAIPrompt()
      .setApiType("responses")
      .setPromptTemplate("{city}")
      .setDeploymentName("gpt-5.1")
      .setToolCallsCol("city")
    val error = intercept[IllegalArgumentException] {
      prompt.transformSchema(inputSchema)
    }
    assert(error.getMessage.contains("Column 'city' already exists"))

    val transformError = intercept[IllegalArgumentException] {
      new OpenAIPrompt()
        .setApiType("responses")
        .setPromptTemplate("{city}")
        .setToolCallsCol("tool_calls")
        .transform(Seq(("Seattle", "existing")).toDF("city", "tool_calls"))
    }
    assert(transformError.getMessage.contains("Column 'tool_calls' already exists"))
  }

  test("Prompt tools and output params survive save and load") {
    ensureSparkSession()
    val prompt = configuredPrompt
      .setToolCallsCol("tool_calls")
      .setResponseStructCol("response_struct")
    val path = tmpDir.resolve("prompt-tools").toString
    prompt.write.overwrite().save(path)
    val loaded = OpenAIPrompt.load(path)
    assert(loaded.getApiType === "responses")
    assert(OpenAIToolUtils.parseTools(loaded.getTools) ===
      OpenAIToolUtils.parseTools(prompt.getTools))
    assert(loaded.getToolCallsCol === "tool_calls")
    assert(loaded.getResponseStructCol === "response_struct")
    assert(loaded.getReasoningMode === "standard")
  }

  test("Prompt generated Python exposes tools modern params and opt-in outputs") {
    val generated = new OpenAIPrompt().generatedPythonClass
    Seq(
      "def setTools(",
      "def setToolsCol(",
      "def setToolChoice(",
      "def setInstructions(",
      "def setTruncation(",
      "def setMetadata(",
      "def setInclude(",
      "def setTopLogprobs(",
      "def setSafetyIdentifier(",
      "def setPromptCacheKey(",
      "def setServiceTier(",
      "def setConversation(",
      "def setReasoningSummary(",
      "def setReasoningContext(",
      "def setReasoningMode(",
      "def setToolCallsCol(",
      "def setResponseStructCol(",
      "def getToolsAsList("
    ).foreach(method => assert(generated.contains(method), method))
    assert(generated.contains("def _resolveToolResponseStructCol("))
    assert(generated.contains("if self.isSet(self.responseStructCol):"))
    assert(generated.contains(
      """self._resolveToolResponseStructCol(outputCol, "toolCallsColumn")"""))
    assert(generated.contains(
      """self._resolveToolResponseStructCol(outputCol, "replayItemsColumn")"""))
    assert(generated.contains(
      "{} requires outputCol or a configured responseStructCol"))
    assert(!generated.contains(
      "self._java_obj.toolCallsColumn(outputCol or self.getOutputCol())"))
    assert(!generated.contains(
      "self._java_obj.replayItemsColumn(outputCol or self.getOutputCol())"))
  }
}
