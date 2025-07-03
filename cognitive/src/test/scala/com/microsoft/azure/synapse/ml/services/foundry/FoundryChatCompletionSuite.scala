// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.foundry

import com.microsoft.azure.synapse.ml.Secrets
import com.microsoft.azure.synapse.ml.core.test.base.Flaky
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.services.openai.{ChatCompletionResponse, OpenAIMessage}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row}
import org.scalactic.Equality

trait FoundryAPIKey {
  lazy val foundryAPIKey: String = sys.env.getOrElse("FOUNDRY_API_KEY", Secrets.FoundryApiKey)
  lazy val foundryServiceName: String = sys.env.getOrElse("FOUNDRY_SERVICE_NAME", "synapseml-ai-foundry-resource")
  lazy val modelName: String = "Phi-4-mini-instruct"
}

class FoundryChatCompletionSuite extends TransformerFuzzing[FoundryChatCompletion] with FoundryAPIKey with Flaky {

  import spark.implicits._

  lazy val completion: FoundryChatCompletion = new FoundryChatCompletion()
    .setCustomServiceName(foundryServiceName)
    .setApiVersion("2024-05-01-preview")
    .setMaxTokens(2048)
    .setOutputCol("out")
    .setMessagesCol("messages")
    .setTemperature(0)
    .setTopP(0.1)
    .setPresencePenalty(0.0)
    .setFrequencyPenalty(0.0)
    .setModel(modelName)
    .setSubscriptionKey(foundryAPIKey)


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

  test("Basic Usage") {
    testCompletion(completion, goodDf)
  }

  test("Robustness to bad inputs") {
    val results = completion.transform(badDf).collect()
    assert(Option(results.head.getAs[Row](completion.getErrorCol)).isDefined)
    assert(Option(results.apply(1).getAs[Row](completion.getErrorCol)).isDefined)
    assert(Option(results.apply(2).getAs[Row](completion.getErrorCol)).isEmpty)
    assert(Option(results.apply(2).getAs[Row]("out")).isEmpty)
  }


  def testCompletion(completion: FoundryChatCompletion, df: DataFrame, requiredLength: Int = 10): Unit = {
    val fromRow = ChatCompletionResponse.makeFromRowConverter
    completion.transform(df).collect().foreach(r =>
      fromRow(r.getAs[Row]("out")).choices.foreach(c =>
        assert(c.message.content.length > requiredLength)))
    completion.transform(df).collect().foreach(r =>
      fromRow(r.getAs[Row]("out")).choices.foreach(c =>
        println(c.message.content)))
  }

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    super.assertDFEq(df1.drop("out"), df2.drop("out"))(eq)
  }

  override def testObjects(): Seq[TestObject[FoundryChatCompletion]] =
    Seq(new TestObject(completion, goodDf))

  override def reader: MLReadable[_] = FoundryChatCompletion

}
