// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.Flaky
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row}
import org.scalactic.Equality

class OpenAIChatCompletionSuite extends TransformerFuzzing[OpenAIChatCompletion] with OpenAIAPIKey with Flaky {

  import spark.implicits._

  lazy val completion: OpenAIChatCompletion = new OpenAIChatCompletion()
    .setDeploymentName(deploymentName)
    .setCustomServiceName(openAIServiceName)
    .setMaxTokens(200)
    .setOutputCol("out")
    .setMessagesCol("messages")
    .setSubscriptionKey(openAIAPIKey)


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
      OpenAIMessage("system", "You are very excited"),
      OpenAIMessage("user", "How are you today"),
      OpenAIMessage("system", "Better than ever"),
      OpenAIMessage("user", "Why?")
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

  def testCompletion(completion: OpenAIChatCompletion, df: DataFrame, requiredLength: Int = 10): Unit = {
    val fromRow = ChatCompletionResponse.makeFromRowConverter
    completion.transform(df).collect().foreach(r =>
      fromRow(r.getAs[Row]("out")).choices.foreach(c =>
        assert(c.message.content.length > requiredLength)))
  }

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    super.assertDFEq(df1.drop("out"), df2.drop("out"))(eq)
  }

  override def testObjects(): Seq[TestObject[OpenAIChatCompletion]] =
    Seq(new TestObject(completion, goodDf))

  override def reader: MLReadable[_] = OpenAIChatCompletion

}
