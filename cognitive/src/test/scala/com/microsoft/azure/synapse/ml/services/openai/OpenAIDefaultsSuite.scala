// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.Flaky
import org.apache.spark.sql.{DataFrame, Row}

class OpenAIDefaultsSuite extends Flaky with OpenAIAPIKey {

  import spark.implicits._

  def promptCompletion: OpenAIChatCompletion = new OpenAIChatCompletion()
    .setMaxTokens(200)
    .setOutputCol("out")
    .setMessagesCol("prompt")

  lazy val promptDF: DataFrame = Seq(
    Seq(
      OpenAIMessage("system", "You are an AI chatbot with red as your favorite color"),
      OpenAIMessage("user", "Whats your favorite color")
    ),
    Seq(
      OpenAIMessage("system", "You are very excited"),
      OpenAIMessage("user", "How are you today")
    ),
    Seq(
      OpenAIMessage("system", "You are very excited"),
      OpenAIMessage("user", "How are you today"),
      OpenAIMessage("system", "Better than ever"),
      OpenAIMessage("user", "Why?")
    )
  ).toDF("prompt")

  test("Completion w Globals") {
    OpenAIDefaults.setDeploymentName(deploymentName)
    OpenAIDefaults.setSubscriptionKey(openAIAPIKey)
    OpenAIDefaults.setTemperature(0.05)
    OpenAIDefaults.setURL(s"https://$openAIServiceName.openai.azure.com/")

    val fromRow = ChatModelResponse.makeFromRowConverter
    promptCompletion.transform(promptDF).collect().foreach(r =>
      fromRow(r.getAs[Row]("out")).choices.foreach(c =>
        assert(c.message.content.length > 10)))
  }

  lazy val prompt: OpenAIPrompt = new OpenAIPrompt()
    .setOutputCol("outParsed")

  lazy val df: DataFrame = Seq(
    ("apple", "fruits"),
    ("mercedes", "cars"),
    ("cake", "dishes"),
    (null, "none") //scalastyle:ignore null
  ).toDF("text", "category")

  test("OpenAIPrompt w Globals") {
    OpenAIDefaults.setDeploymentName(deploymentName)
    OpenAIDefaults.setSubscriptionKey(openAIAPIKey)
    OpenAIDefaults.setTemperature(0.05)
    OpenAIDefaults.setURL(s"https://$openAIServiceName.openai.azure.com/")

    val nonNullCount = prompt
      .setPromptTemplate("here is a comma separated list of 5 {category}: {text}, ")
      .setPostProcessing("csv")
      .transform(df)
      .select("outParsed")
      .collect()
      .count(r => Option(r.getSeq[String](0)).isDefined)

    assert(nonNullCount == 3)

    assert(prompt.getDeploymentName == deploymentName)
    assert(prompt.getSubscriptionKey == openAIAPIKey)
    assert(prompt.getTemperature == 0.05)
  }

  test("Test Getters") {
    OpenAIDefaults.setDeploymentName(deploymentName)
    OpenAIDefaults.setSubscriptionKey(openAIAPIKey)
    OpenAIDefaults.setTemperature(0.05)
    OpenAIDefaults.setSeed(42)
    OpenAIDefaults.setTopP(0.9)
    OpenAIDefaults.setURL(s"https://$openAIServiceName.openai.azure.com/")
    OpenAIDefaults.setApiVersion("2024-05-01-preview")
    OpenAIDefaults.setModel("grok-3-mini")
    OpenAIDefaults.setEmbeddingDeploymentName("text-embedding-ada-002")
    OpenAIDefaults.setVerbosity("medium")
    OpenAIDefaults.setReasoningEffort("medium")
    OpenAIDefaults.setApiType("responses")

    assert(OpenAIDefaults.getDeploymentName.contains(deploymentName))
    assert(OpenAIDefaults.getSubscriptionKey.contains(openAIAPIKey))
    assert(OpenAIDefaults.getTemperature.contains(0.05))
    assert(OpenAIDefaults.getSeed.contains(42))
    assert(OpenAIDefaults.getTopP.contains(0.9))
    assert(OpenAIDefaults.getURL.contains(s"https://$openAIServiceName.openai.azure.com/"))
    assert(OpenAIDefaults.getApiVersion.contains("2024-05-01-preview"))
    assert(OpenAIDefaults.getModel.contains("grok-3-mini"))
    assert(OpenAIDefaults.getEmbeddingDeploymentName.contains("text-embedding-ada-002"))
    assert(OpenAIDefaults.getVerbosity.contains("medium"))
    assert(OpenAIDefaults.getReasoningEffort.contains("medium"))
    assert(OpenAIDefaults.getApiType.contains("responses"))
  }

  test("Test Resetters") {
    OpenAIDefaults.setDeploymentName(deploymentName)
    OpenAIDefaults.setSubscriptionKey(openAIAPIKey)
    OpenAIDefaults.setTemperature(0.05)
    OpenAIDefaults.setSeed(42)
    OpenAIDefaults.setTopP(0.9)
    OpenAIDefaults.setURL(s"https://$openAIServiceName.openai.azure.com/")
    OpenAIDefaults.setApiVersion("2024-05-01-preview")
    OpenAIDefaults.setModel("grok-3-mini")
    OpenAIDefaults.setEmbeddingDeploymentName("text-embedding-ada-002")
    OpenAIDefaults.setVerbosity("medium")
    OpenAIDefaults.setReasoningEffort("medium")
    OpenAIDefaults.setApiType("responses")

    OpenAIDefaults.resetDeploymentName()
    OpenAIDefaults.resetSubscriptionKey()
    OpenAIDefaults.resetTemperature()
    OpenAIDefaults.resetSeed()
    OpenAIDefaults.resetTopP()
    OpenAIDefaults.resetURL()
    OpenAIDefaults.resetApiVersion()
    OpenAIDefaults.resetModel()
    OpenAIDefaults.resetEmbeddingDeploymentName()
    OpenAIDefaults.resetVerbosity()
    OpenAIDefaults.resetReasoningEffort()
    OpenAIDefaults.resetApiType()

    assert(OpenAIDefaults.getDeploymentName.isEmpty)
    assert(OpenAIDefaults.getSubscriptionKey.isEmpty)
    assert(OpenAIDefaults.getTemperature.isEmpty)
    assert(OpenAIDefaults.getSeed.isEmpty)
    assert(OpenAIDefaults.getTopP.isEmpty)
    assert(OpenAIDefaults.getURL.isEmpty)
    assert(OpenAIDefaults.getApiVersion.isEmpty)
    assert(OpenAIDefaults.getModel.isEmpty)
    assert(OpenAIDefaults.getEmbeddingDeploymentName.isEmpty)
    assert(OpenAIDefaults.getVerbosity.isEmpty)
    assert(OpenAIDefaults.getReasoningEffort.isEmpty)
    assert(OpenAIDefaults.getApiType.isEmpty)
  }

  test("Test Parameter Validation") {
    // Test valid temperature values
    OpenAIDefaults.setTemperature(0.0)
    OpenAIDefaults.setTemperature(1.0)
    OpenAIDefaults.setTemperature(2.0)
    OpenAIDefaults.setTemperature(0)
    OpenAIDefaults.setTemperature(1)
    OpenAIDefaults.setTemperature(2)

    // Test valid topP values
    OpenAIDefaults.setTopP(0.0)
    OpenAIDefaults.setTopP(0.5)
    OpenAIDefaults.setTopP(1.0)
    OpenAIDefaults.setTopP(0)
    OpenAIDefaults.setTopP(1)

    // Test invalid temperature values
    assertThrows[IllegalArgumentException] {
      OpenAIDefaults.setTemperature(-0.1)
    }
    assertThrows[IllegalArgumentException] {
      OpenAIDefaults.setTemperature(2.1)
    }

    // Test invalid topP values
    assertThrows[IllegalArgumentException] {
      OpenAIDefaults.setTopP(-0.1)
    }
    assertThrows[IllegalArgumentException] {
      OpenAIDefaults.setTopP(1.1)
    }

    // Test verbosity values
    OpenAIDefaults.setVerbosity("low")
    OpenAIDefaults.setVerbosity("anything")

    // Test reasoning effort values
    OpenAIDefaults.setReasoningEffort("low")
    OpenAIDefaults.setReasoningEffort("anything")
  }
}
