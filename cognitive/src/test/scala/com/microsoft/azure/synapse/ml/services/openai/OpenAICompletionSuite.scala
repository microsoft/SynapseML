// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.Secrets
import com.microsoft.azure.synapse.ml.Secrets.getAccessToken
import com.microsoft.azure.synapse.ml.core.test.base.Flaky
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row}
import org.scalactic.Equality

class OpenAICompletionSuite extends TransformerFuzzing[OpenAICompletion] with OpenAIAPIKey with Flaky {

  import spark.implicits._

  override def beforeAll(): Unit = {
    val aadToken = getAccessToken("https://cognitiveservices.azure.com/")
    println(s"Triggering token creation early ${aadToken.length}")
    super.beforeAll()
  }

  def newCompletion: OpenAICompletion = new OpenAICompletion()
    .setDeploymentName(deploymentName)
    .setCustomServiceName(openAIServiceName)
    .setMaxTokens(200)
    .setOutputCol("out")
    .setSubscriptionKey(openAIAPIKey)

  lazy val promptCompletion: OpenAICompletion = newCompletion.setPromptCol("prompt")
  lazy val batchPromptCompletion: OpenAICompletion = newCompletion.setBatchPromptCol("batchPrompt")

  lazy val df: DataFrame = Seq(
    "Once upon a time",
    "Best programming language award goes to",
    "SynapseML is "
  ).toDF("prompt")

  lazy val promptDF: DataFrame = Seq(
    "Once upon a time",
    "Best programming language award goes to",
    "SynapseML is "
  ).toDF("prompt")

  lazy val batchPromptDF: DataFrame = Seq(
    Seq(
      "This is a test",
      "Now is the time",
      "Knock, knock")
  ).toDF("batchPrompt")

  ignore("Basic Usage") {
    testCompletion(promptCompletion, promptDF)
  }

  ignore("Basic usage with AAD auth") {
    val aadToken = getAccessToken("https://cognitiveservices.azure.com/")

    val completion = new OpenAICompletion()
      .setAADToken(aadToken)
      .setDeploymentName(deploymentName)
      .setCustomServiceName(openAIServiceName)
      .setPromptCol("prompt")
      .setOutputCol("out")

    testCompletion(completion, promptDF)
  }

  ignore("Batch Prompt") {
    testCompletion(batchPromptCompletion, batchPromptDF)
  }

  def testCompletion(completion: OpenAICompletion, df: DataFrame, requiredLength: Int = 10): Unit = {
    val fromRow = CompletionResponse.makeFromRowConverter
    completion.transform(df).collect().foreach(r =>
      fromRow(r.getAs[Row]("out")).choices.foreach(c =>
        assert(c.text.length > requiredLength)))
  }


  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    super.assertDFEq(df1.drop("out"), df2.drop("out"))(eq)
  }

  override def testObjects(): Seq[TestObject[OpenAICompletion]] =
    Seq(new TestObject(newCompletion, df))

  override def reader: MLReadable[_] = OpenAICompletion

}
