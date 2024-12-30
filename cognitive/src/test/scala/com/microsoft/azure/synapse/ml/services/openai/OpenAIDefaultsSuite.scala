// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.Flaky
import org.apache.spark.sql.{DataFrame, Row}

class OpenAIDefaultsSuite extends Flaky with OpenAIAPIKey {

  import spark.implicits._

  def promptCompletion: OpenAICompletion = new OpenAICompletion()
    .setMaxTokens(200)
    .setOutputCol("out")
    .setPromptCol("prompt")

  lazy val promptDF: DataFrame = Seq(
    "Once upon a time",
    "Best programming language award goes to",
    "SynapseML is "
  ).toDF("prompt")

  test("Completion w Globals") {
    OpenAIDefaults.setDeploymentName(deploymentName)
    OpenAIDefaults.setSubscriptionKey(openAIAPIKey)
    OpenAIDefaults.setTemperature(0.05)
    OpenAIDefaults.setURL(s"https://$openAIServiceName.openai.azure.com/")

    val fromRow = CompletionResponse.makeFromRowConverter
    promptCompletion.transform(promptDF).collect().foreach(r =>
      fromRow(r.getAs[Row]("out")).choices.foreach(c =>
        assert(c.text.length > 10)))
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
    OpenAIDefaults.setURL(s"https://$openAIServiceName.openai.azure.com/")

    assert(OpenAIDefaults.getDeploymentName.contains(deploymentName))
    assert(OpenAIDefaults.getSubscriptionKey.contains(openAIAPIKey))
    assert(OpenAIDefaults.getTemperature.contains(0.05))
    assert(OpenAIDefaults.getURL.contains(s"https://$openAIServiceName.openai.azure.com/"))
  }

  test("Test Resetters") {
    OpenAIDefaults.setDeploymentName(deploymentName)
    OpenAIDefaults.setSubscriptionKey(openAIAPIKey)
    OpenAIDefaults.setTemperature(0.05)
    OpenAIDefaults.setURL(s"https://$openAIServiceName.openai.azure.com/")

    OpenAIDefaults.resetDeploymentName()
    OpenAIDefaults.resetSubscriptionKey()
    OpenAIDefaults.resetTemperature()
    OpenAIDefaults.resetURL()

    assert(OpenAIDefaults.getDeploymentName.isEmpty)
    assert(OpenAIDefaults.getSubscriptionKey.isEmpty)
    assert(OpenAIDefaults.getTemperature.isEmpty)
    assert(OpenAIDefaults.getURL.isEmpty)
  }
}
