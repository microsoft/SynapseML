// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.Flaky
import org.apache.spark.sql.{Row, functions => F}

/** Credential-gated smoke tests. Offline suites must never mix in OpenAIAPIKey. */
class OpenAIToolsLiveSuite extends Flaky with OpenAIAPIKey {
  import spark.implicits._

  private def toolsDeployment: String =
    sys.env.getOrElse("OPENAI_DEPLOYMENT_TOOLS", deploymentName5p1)

  test("Responses performs a real function call and stored continuation") {
    val turn1 = new OpenAIResponses()
      .setSubscriptionKey(openAIAPIKey)
      .setCustomServiceName(openAIServiceName)
      .setDeploymentName(toolsDeployment)
      .setMessagesCol("messages")
      .setTools(ToolTestFixtures.WeatherToolJson)
      .setToolChoice("required")
      .setStore(true)
      .setToolCallsCol("tool_calls")
      .setOutputCol("response")

    val asked = turn1.transform(Seq(
      Seq(OpenAIMessage("user", "What is the weather in Seattle?"))
    ).toDF("messages")).persist()
    asked.count()
    val first = asked.collect().head
    val call = first.getAs[Seq[Row]]("tool_calls").head
    assert(call.getAs[String]("name") === "get_weather")

    val results = asked.select(
      F.col("response.id").as("response_id"),
      F.array(F.struct(
        F.col("tool_calls").getItem(0).getField("call_id").as("call_id"),
        F.lit("""{"tempC":20}""").as("output"),
        F.lit("completed").as("status")
      )).as("tool_results")
    )
    val turn2 = new OpenAIResponses()
      .setSubscriptionKey(openAIAPIKey)
      .setCustomServiceName(openAIServiceName)
      .setDeploymentName(toolsDeployment)
      .setFunctionCallOutputsCol("tool_results")
      .setPreviousResponseIdCol("response_id")
      .setTools(ToolTestFixtures.WeatherToolJson)
      .setOutputCol("response2")
    val answer = turn2.transform(results)
      .select(turn2.getOutputMessageText("response2").as("text"))
      .collect().head.getString(0)
    assert(answer.nonEmpty)
  }

  test("OpenAIPrompt exposes structured calls from a real Responses request") {
    val prompt = new OpenAIPrompt()
      .setSubscriptionKey(openAIAPIKey)
      .setCustomServiceName(openAIServiceName)
      .setDeploymentName(toolsDeployment)
      .setApiType("responses")
      .setPromptTemplate("What is the weather in {city}?")
      .setTools(ToolTestFixtures.WeatherToolJson)
      .setToolChoice("required")
      .setToolCallsCol("tool_calls")
      .setResponseStructCol("response_struct")
    val row = prompt.transform(Seq("Seattle").toDF("city")).collect().head
    assert(row.getAs[Seq[Row]]("tool_calls").nonEmpty)
    assert(row.getAs[Row]("response_struct").getAs[String]("id").startsWith("resp_"))
    assert(row.getAs[Row](prompt.getErrorCol) == null) //scalastyle:ignore null
  }
}
