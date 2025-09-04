// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.aifoundry

import com.microsoft.azure.synapse.ml.Secrets
import com.microsoft.azure.synapse.ml.core.test.base.Flaky
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.services.openai._
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row}
import org.scalactic.Equality

trait AIFoundryAPIKey {
  lazy val aiFoundryAPIKey: String = sys.env.getOrElse("AI_FOUNDRY_API_KEY", Secrets.AIFoundryApiKey)
  lazy val aiFoundryServiceName: String = sys.env.getOrElse("AI_FOUNDRY_SERVICE_NAME", "synapseml-ai-foundry-resource")
  lazy val aiFoundryModelName: String = "Llama-3.3-70B-Instruct" //"Phi-4-mini-instruct"
}

class AIFoundryChatCompletionSuite extends TransformerFuzzing[AIFoundryChatCompletion] with AIFoundryAPIKey with Flaky {

  import spark.implicits._

  lazy val completion: AIFoundryChatCompletion = new AIFoundryChatCompletion()
    .setCustomServiceName(aiFoundryServiceName)
    .setApiVersion("2024-05-01-preview")
    .setMaxTokens(2048)
    .setOutputCol("out")
    .setMessagesCol("messages")
    .setTemperature(0)
    .setTopP(0.1)
    .setPresencePenalty(0.0)
    .setFrequencyPenalty(0.0)
    .setModel(aiFoundryModelName)
    .setSubscriptionKey(aiFoundryAPIKey)

  lazy val goodDf: DataFrame = Seq(
    Seq(
      OpenAIMessage("system", "You are an AI chatbot with red as your favorite color"),
      OpenAIMessage("user", "Whats your favorite color")
    ),
    Seq(
      OpenAIMessage("system", "You are very excited"),
      OpenAIMessage("user", "How are you today")
    ),
    Seq(
      OpenAIMessage("system", "You are a helpful assistant"),
      OpenAIMessage("user", "I need to calculate how much apples I sold today"),
      OpenAIMessage("system", "How many apples you sold in each transaction"),
      OpenAIMessage("user", "One in the first, two in the second")
    )
  ).toDF("messages")

  lazy val badDf: DataFrame = Seq(
    Seq(),
    Seq(
      OpenAIMessage("system", "You are very excited"),
      OpenAIMessage("user", null) //scalastyle:ignore null
    ),
    null //scalastyle:ignore null
  ).toDF("messages")

  import spark.implicits._

  def promptCompletion: AIFoundryChatCompletion = new AIFoundryChatCompletion()
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

  test("Basic Usage") {
    testCompletion(completion, goodDf)
  }

  test("Robustness to bad inputs") {
    val results = completion.transform(badDf).collect()
    assert(Option(results.head.getAs[Row](completion.getErrorCol)).isDefined)
    // empty user message is valid for Phi 4, no error
    //assert(Option(results.apply(1).getAs[Row](completion.getErrorCol)).isDefined)
    assert(Option(results.apply(2).getAs[Row](completion.getErrorCol)).isEmpty)
    assert(Option(results.apply(2).getAs[Row]("out")).isEmpty)
  }


  test("getOptionalParam should include responseFormat"){
    val completion = new AIFoundryChatCompletion()
      .setCustomServiceName(aiFoundryServiceName)

    def validateResponseFormat(params: Map[String, Any], responseFormat: String): Unit = {
      val responseFormatPayloadName = this.completion.responseFormat.payloadName
      assert(params.contains(responseFormatPayloadName))
      val responseFormatMap = params(responseFormatPayloadName).asInstanceOf[Map[String, String]]
      assert(responseFormatMap.contains("type"))
      assert(responseFormatMap("type") == responseFormat)
    }

    val messages: Seq[Row] = Seq(
      OpenAIMessage("user", "Whats your favorite color")
    ).toDF("role", "content", "name").collect()

    val optionalParams: Map[String, Any] = completion.getOptionalParams(messages.head)
    assert(!optionalParams.contains("response_format"))

    completion.setResponseFormat("")
    val optionalParams0: Map[String, Any] = completion.getOptionalParams(messages.head)
    assert(!optionalParams0.contains("response_format"))

    completion.setResponseFormat("json_object")
    val optionalParams1: Map[String, Any] = completion.getOptionalParams(messages.head)
    validateResponseFormat(optionalParams1, "json_object")

    completion.setResponseFormat("text")
    val optionalParams2: Map[String, Any] = completion.getOptionalParams(messages.head)
    validateResponseFormat(optionalParams2, "text")

    completion.setResponseFormat(Map("type" -> "json_object"))
    val optionalParams3: Map[String, Any] = completion.getOptionalParams(messages.head)
    validateResponseFormat(optionalParams3, "json_object")

    completion.setResponseFormat(OpenAIResponseFormat.TEXT)
    val optionalParams4: Map[String, Any] = completion.getOptionalParams(messages.head)
    validateResponseFormat(optionalParams4, "text")
  }

  test("setResponseFormat should throw exception if invalid format"){
    val completion = new AIFoundryChatCompletion()
      .setCustomServiceName(aiFoundryServiceName)

    assertThrows[IllegalArgumentException] {
      completion.setResponseFormat("invalid_format")
    }

    assertThrows[IllegalArgumentException] {
      completion.setResponseFormat(Map("type" -> "invalid_format"))
    }

    assertThrows[IllegalArgumentException] {
      completion.setResponseFormat(Map("invalid_key" -> "json_object"))
    }
  }

  test("validate accepts json_object response format") {
    val goodDf: DataFrame = Seq(
      Seq(
        OpenAIMessage("system", "You are an AI chatbot with red as your favorite color"),
        OpenAIMessage("system", OpenAIResponseFormat.JSON.prompt),
        OpenAIMessage("user", "Whats your favorite color")
      ),
      Seq(
        OpenAIMessage("system", "You are very excited"),
        OpenAIMessage("system", OpenAIResponseFormat.JSON.prompt),
        OpenAIMessage("user", "How are you today")
      ),
      Seq(
        OpenAIMessage("system", OpenAIResponseFormat.JSON.prompt),
        OpenAIMessage("system", "You are very excited"),
        OpenAIMessage("user", "How are you today"),
        OpenAIMessage("system", "Better than ever"),
        OpenAIMessage("user", "Why?")
      )
    ).toDF("messages")

    val completion = new AIFoundryChatCompletion()
      .setCustomServiceName(aiFoundryServiceName)
      .setModel(aiFoundryModelName)
      .setApiVersion("2024-05-01-preview")
      .setMaxTokens(500)
      .setOutputCol("out")
      .setMessagesCol("messages")
      .setTemperature(0)
      .setSubscriptionKey(aiFoundryAPIKey)
      .setResponseFormat("json_object")

    testCompletion(completion, goodDf)
  }

  def testCompletion(completion: AIFoundryChatCompletion, df: DataFrame, requiredLength: Int = 10): Unit = {
    val fromRow = ChatCompletionResponse.makeFromRowConverter
    completion.transform(df).collect().foreach(r =>
      fromRow(r.getAs[Row]("out")).choices.foreach(c =>
        assert(c.message.content.length > requiredLength)))
  }

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    super.assertDFEq(df1.drop("out"), df2.drop("out"))(eq)
  }

  override def testObjects(): Seq[TestObject[AIFoundryChatCompletion]] =
    Seq(new TestObject(completion, goodDf))

  override def reader: MLReadable[_] = AIFoundryChatCompletion

}
