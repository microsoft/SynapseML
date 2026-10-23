// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.Secrets.getAccessToken
import com.microsoft.azure.synapse.ml.core.test.base.Flaky
import org.apache.spark.sql.DataFrame

class OpenAIPromptResponsesSuite extends Flaky with OpenAIAPIKey {

  import spark.implicits._

  override def beforeAll(): Unit = {
    val aadToken = getAccessToken("https://cognitiveservices.azure.com/")
    println(s"Triggering token creation early ${aadToken.length}")
    super.beforeAll()
  }

  lazy val df: DataFrame = Seq(
    ("apple", "fruits"),
    ("mercedes", "cars"),
    ("cake", "dishes")
  ).toDF("text", "category")

  private def responsesPrompt(outputCol: String, deployment: String): OpenAIPrompt = {
    new OpenAIPrompt()
      .setSubscriptionKey(openAIAPIKey)
      .setDeploymentName(deployment)
      .setCustomServiceName(openAIServiceName)
      .setApiType("responses")
      .setApiVersion("2025-04-01-preview")
      .setReasoningEffort("low")
      .setVerbosity("low")
      .setOutputCol(outputCol)
  }

  private def assertResponsesOutputForDeployment(
      deployment: String,
      outputCol: String,
      expectedToken: String): Unit = {
    val prompt = responsesPrompt(outputCol, deployment)
      .setPromptTemplate(s"Return exactly the word $expectedToken for {text}.")

    val output = prompt.transform(df.limit(1))
      .select(outputCol)
      .collect()
      .head
      .getString(0)

    assert(output != null)
    assert(output.toLowerCase.contains(expectedToken.toLowerCase))
  }

  test("Responses API OpenAIPrompt returns text for gpt-6-luna outputs") {
    assertResponsesOutputForDeployment(
      deploymentName,
      "responses_luna_output",
      "fruit")
  }
}
