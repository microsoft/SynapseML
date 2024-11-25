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
    .setDeploymentName(deploymentNameGpt4)
    .setCustomServiceName(openAIServiceName)
    .setApiVersion("2023-05-15")
    .setMaxTokens(5000)
    .setOutputCol("out")
    .setMessagesCol("messages")
    .setTemperature(0)
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
    Seq(
      OpenAIMessage("system", "You are very excited"),
      OpenAIMessage("user", "")
      ),
    Seq(OpenAIMessage("system", "You are very excited")),
    null //scalastyle:ignore null
  ).toDF("messages")

  lazy val slowDf: DataFrame = Seq(
    Seq(
      OpenAIMessage("system", "You help summarize content"),
      OpenAIMessage("user",
        """
        Given the following list of Article and their descriptions:
            Article ID: 13d98f40-517a-4c95-bd84-27df6ea6a2a1
            Article Description: Pride and Prejudice
            Article ID: 6a8a8c95-ef38-4e7b-99b4-81756f602c36
            Article Description: Romeo and Juliet
            Article ID: 4b7b3c82-6c3e-45cf-9f6b-982a45f1dbad
            Article Description: Calculus Made Easy
            Article ID: 8f9d1f15-1aef-4a35-8301-ec8aa869f7b3
            Article Description: Moby Dick; Or, The Whale
            Article ID: 2d8a6e74-7b58-4a63-bc8d-3f5e90ab5b19
            Article Description: The Scarlet Letter
            Article ID: f8c0cfea-42e6-49e2-9f67-dc5ea20197c5
            Article Description: A Christmas Carol in Prose
            Article ID: 1a743f5b-98b3-43b5-b27e-9f631e7c625e
            Article Description: Alice's Adventures in Wonderland
            Article ID: 649da1a1-4df4-41e8-b4c2-943d8d1c47b7
            Article Description: The Eyes Have It
            Article ID: d35b7e0b-6fc5-4bf5-9d27-96d4894950b9
            Article Description: Dracula
            Article ID: a32faffd-d3f3-4f79-b9a6-47c1f6f8e299
            Article Description: The Great Gatsby
            Article ID: 9ea09862-0e44-4a58-81c1-c3449c7644c0
            Article Description: A Doll's House : a play
            Article ID: 3a6f64b8-3aa4-4f6e-bc6c-7df16ff0ce28
            Article Description: The Picture of Dorian Gray
            Article ID: 815b62c6-4c5f-43a1-844c-65d73a32162a
            Article Description: A Modest Proposal
            Article ID: d8e51b1a-1ea8-4a71-8ea3-ff4a2b2a00cf
            Article Description: The Importance of Being Earnest: A Trivial Comedy for Serious People
            Article ID: 682cfd7c-4a6e-4c87-88ae-9a2e7a61bf6b
            Article Description: Metamorphosis
            Article ID: 9a26b5c7-7438-45f3-b9da-8fc7ef3dd84c
            Article Description: The Complete Works of William Shakespeare
            Article ID: 6b2e7e26-2540-4a3a-810c-7d72fc4ec2cf
            Article Description: The Strange Case of Dr. Jekyll and Mr. Hyde
            Article ID: f7112843-719d-4a20-81a2-8767d7a18f15
            Article Description: Middlemarch
            Article ID: b25e40b0-5634-48da-b4fc-15dbbb3a20db
            Article Description: A Room with a View
            Article ID: a13b5f45-8cf2-4f16-8edf-751b3b4b29b8
            Article Description: A Tale of Two Cities
            Article ID: c8c34f96-b2c4-4dab-8d6d-b042e8c9031f
            Article Description: The Yellow Wallpaper
            Article ID: 45d8d855-e129-4c10-8690-34d5fbcff2d9
            Article Description: Little Women; Or, Meg, Jo, Beth, and Amy
            Article ID: 9a887890-1c48-4be0-b796-c542bfb7f3db
            Article Description: The Adventures of Sherlock Holmes
            Article ID: f26c78da-1e57-4d5e-a7dd-42e64d90f804
            Article Description: Jane Eyre: An Autobiography
            Article ID: b1d14760-1c6d-4b91-b58c-48a907f891b3
            Article Description: Great Expectations
            Article ID: 57396190-8a5d-4a2b-90ab-9c3bc02e7fb6
            Article Description: The Enchanted April
            Article ID: 704132cb-0bf4-46de-8b04-2d8c2ad4a409
            Article Description: Adventures of Huckleberry Finn
            Article ID: bcf666b2-803b-45a2-a36b-38a8f5641e20
            Article Description: The Blue Castle: a novel
            Article ID: 0c13bf68-7d10-4a02-8d1a-24290b623db4
            Article Description: The Prince
            Article ID: e13d8a92-7152-4c0c-8e45-1ff6485e3f92
            Article Description: Cranford
            Classify the articles into one of the article classes listed below:
            Fantasy, Historical Fiction, Thriller, Romance, Science Fiction,
            Mystery, Poetry, Drama, Classics, Humor, Religion, Philosophy, Psychology, Business, Other
            For each article, please ensure that you do the following:
            Ignore the numbers in the description.
            Provide a rating between 0 and 10 of how confident you are with the classification
            as well as a short justification.
            Please provide your response in pure JSON syntax for all articles in the list as shown below
            [
                {"article_id": value,
                "article_name": value,
                "article_class": value,
                "article_description": value,
                "confidence": value,
                "justification": value
                }
            ]
            Please do not include any other contextual words
            other than proper JSON object for each article.""".stripMargin)
    )
  ).toDF("messages")

  test("Basic Usage") {
    testCompletion(completion, goodDf)
    testCompletion(completion, slowDf)
  }

  test("Robustness to bad inputs") {
    val completion: OpenAIChatCompletion = new OpenAIChatCompletion()
      // by having an invalid deployment name, we should get error for all rows
      .setDeploymentName("invalid_deployment")
      .setCustomServiceName(openAIServiceName)
      .setApiVersion("2023-05-15")
      .setMaxTokens(5000)
      .setOutputCol("out")
      .setMessagesCol("messages")
      .setTemperature(0)
      .setSubscriptionKey(openAIAPIKey)
    val rows = completion.transform(goodDf).collect()
    assert(rows.length == goodDf.count())
    rows.foreach { row =>
      assert(Option(row.getAs[Row]("out")).isEmpty)
      assert(Option(row.getAs[Row](completion.getErrorCol)).isDefined)
      assert(Option(row.getAs[Row](completion.getErrorCol)).nonEmpty)
    }
  }

  ignore("Custom EndPoint") {
    lazy val accessToken: String = sys.env.getOrElse("CUSTOM_ACCESS_TOKEN", "")
    lazy val customRootUrlValue: String = sys.env.getOrElse("CUSTOM_ROOT_URL", "")
    lazy val customHeadersValues: Map[String, String] = Map("X-ModelType" -> "gpt-4-turbo-chat-completions")

    val customEndpointCompletion = new OpenAIChatCompletion()
      .setCustomUrlRoot(customRootUrlValue)
      .setOutputCol("out")
      .setMessagesCol("messages")
      .setTemperature(0)

    if (accessToken.isEmpty) {
      customEndpointCompletion.setSubscriptionKey(openAIAPIKey)
        .setDeploymentName(deploymentNameGpt4)
        .setCustomServiceName(openAIServiceName)
    } else {
      customEndpointCompletion.setAADToken(accessToken)
        .setCustomHeaders(customHeadersValues)
    }

    testCompletion(customEndpointCompletion, goodDf)
  }

  test("shouldSkip method for Good and Bad Data") {
    shouldSkipRowHelper(goodDf.collect(), expectedToBeSkipped = false)
    shouldSkipRowHelper(slowDf.collect(), expectedToBeSkipped = false)
    shouldSkipRowHelper(badDf.collect(), expectedToBeSkipped = true)

    val dfWithSomeEmptyAndNonEmptyUserMessage = Seq(
      Seq(
        OpenAIMessage("system", "You are an AI chatbot with red as your favorite color"),
        OpenAIMessage("user", ""),
        OpenAIMessage("user", "Whats your favorite color")
      )
    ).toDF("messages")
    shouldSkipRowHelper(dfWithSomeEmptyAndNonEmptyUserMessage.collect(), expectedToBeSkipped = false)

    val dfWithEmptySystemMessage = Seq(
      Seq(
        OpenAIMessage("system", ""),
        OpenAIMessage("user", "Whats your favorite color")
      ),
      Seq(
        OpenAIMessage("system", null), //scalastyle:ignore null
        OpenAIMessage("user", "Whats your favorite color")
      ),
      Seq(OpenAIMessage("user", "Whats your favorite color"))
    ).toDF("messages")
    shouldSkipRowHelper(dfWithEmptySystemMessage.collect(), expectedToBeSkipped = false)

    val dfWithAssistantValidMessages = Seq(
      Seq(
        OpenAIMessage("system", "You are an AI chatbot with red as your favorite color"),
        OpenAIMessage("assistant", "Whats your favorite color")
      )
    ).toDF("messages")
    shouldSkipRowHelper(dfWithAssistantValidMessages.collect(), expectedToBeSkipped = false)
  }

  def shouldSkipRowHelper(rows: Seq[Row], expectedToBeSkipped: Boolean): Unit = {
    val completion = new OpenAIChatCompletion()
      .setMessagesCol("messages")
    for (r <- rows) {
      assert(completion.shouldSkip(r) == expectedToBeSkipped)
    }
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
