// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive.openai

import com.microsoft.azure.synapse.ml.Secrets
import com.microsoft.azure.synapse.ml.core.test.base.Flaky
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.nbtest.SynapseUtilities.getAccessToken
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row}
import org.scalactic.Equality

trait OpenAIAPIKey {
  lazy val openAIAPIKey: String = sys.env.getOrElse("OPENAI_API_KEY", Secrets.OpenAIApiKey)
  lazy val openAIServiceName: String = "synapseml-openai"
}

class OpenAICompletionSuite extends TransformerFuzzing[OpenAICompletion] with OpenAIAPIKey with Flaky {

  import spark.implicits._

  lazy val completion: OpenAICompletion = new OpenAICompletion()
    .setSubscriptionKey(openAIAPIKey)
    .setDeploymentName("text-davinci-001")
    .setModel("text-davinci-003")
    .setCustomServiceName(openAIServiceName)
    .setMaxTokens(20)
    .setLogProbs(5)
    .setPromptCol("prompt")
    .setOutputCol("out")

  lazy val promptCompletion: OpenAICompletion = newCompletion.setPromptCol("prompt")
  lazy val batchPromptCompletion: OpenAICompletion = newCompletion.setBatchPromptCol("batchPrompt")
  lazy val indexPromptCompletion: OpenAICompletion = newCompletion.setIndexPromptCol("indexPrompt")
  lazy val batchIndexPromptCompletion: OpenAICompletion = newCompletion.setBatchIndexPromptCol("batchIndexPrompt")


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

  lazy val indexPromptDF: DataFrame = Seq(
    Seq(3, 1, 5, 4)
  ).toDF("indexPrompt")

  lazy val batchIndexPromptDF: DataFrame = Seq(
    Seq(
      Seq(1, 8, 4, 2),
      Seq(7, 3, 8, 5, 9),
      Seq(8, 0, 11, 3, 14, 1))
  ).toDF("batchIndexPrompt")

  test("Basic Usage") {
    testCompletion(promptCompletion, promptDF)
  }

  test("Basic usage with AAD auth") {
    val aadToken = getAccessToken(Secrets.ServicePrincipalClientId,
      Secrets.ServiceConnectionSecret,
      "https://cognitiveservices.azure.com/")
    val completion = new OpenAICompletion()
      .setAADToken(aadToken)
      .setDeploymentName("text-davinci-001")
      .setModel("text-davinci-003")
      .setCustomServiceName(openAIServiceName)
      .setMaxTokens(20)
      .setLogProbs(5)
      .setPromptCol("prompt")
      .setOutputCol("out")

    testCompletion(completion, promptDF)
  }

  ignore("Batch Prompt") {
    testCompletion(batchPromptCompletion, batchPromptDF)
  }

  // TODO: see if data type failure here is due to a change on the OpenAI side of things
  ignore("Index Prompt") {
    testCompletion(indexPromptCompletion, indexPromptDF)
  }

  ignore("Batch Index Prompt") {
    testCompletion(batchIndexPromptCompletion, batchIndexPromptDF)
  }

  def testCompletion(completion: OpenAICompletion, df: DataFrame, requiredLength: Int = 10): Unit = {
    val fromRow = CompletionResponse.makeFromRowConverter
    completion.transform(df).collect().foreach(r =>
      fromRow(r.getAs[Row]("out")).choices.foreach(c =>
        assert(c.text.length > requiredLength)))
  }

  def newCompletion: OpenAICompletion = {
    new OpenAICompletion()
      .setSubscriptionKey(openAIAPIKey)
      .setDeploymentName("text-davinci-001")
      .setCustomServiceName(openAIServiceName)
      .setMaxTokens(20)
      .setLogProbs(5)
      .setOutputCol("out")
  }

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    super.assertDFEq(df1.drop("out"), df2.drop("out"))(eq)
  }

  override def testObjects(): Seq[TestObject[OpenAICompletion]] =
    Seq(new TestObject(completion, df))

  override def reader: MLReadable[_] = OpenAICompletion

}
