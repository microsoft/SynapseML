// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive.split1

import com.microsoft.azure.synapse.ml.Secrets
import com.microsoft.azure.synapse.ml.cognitive._
import com.microsoft.azure.synapse.ml.core.test.base.{Flaky, TestBase}
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row}
import org.scalactic.Equality

trait OpenAIAPIKey {
  lazy val openAIAPIKey: String = sys.env.getOrElse("OPENAI_API_KEY", Secrets.OpenAIApiKey)
  lazy val openAIServiceName: String = "bugbashtest6"
}

class OpenAICompletionSuite extends TransformerFuzzing[OpenAICompletion] with OpenAIAPIKey with Flaky {

  import spark.implicits._

  lazy val promptCompletion: OpenAICompletion = newCompletion.setPromptCol("prompt")
  lazy val bulkPromptCompletion: OpenAICompletion = newCompletion.setBulkPromptCol("bulkPrompt")
  lazy val indexCompletion: OpenAICompletion = newCompletion.setIndexPromptCol("index")
  lazy val bulkIndexCompletion: OpenAICompletion = newCompletion.setBulkIndexPromptCol("bulkIndex")

  lazy val promptDF: DataFrame = Seq(
    "Once upon a time",
    "Best programming language award goes to",
    "SynapseML is "
  ).toDF("prompt")

  lazy val bulkPromptDF: DataFrame = Seq(
    Seq(
      "Now is the time",
      "Knock, knock",
      "Ask not")
  ).toDF("bulkPrompt")

  lazy val indexDF: DataFrame = Seq(
    Seq(1212, 318, 247, 1332)
  ).toDF("index")

  lazy val bulkIndexDF: DataFrame = Seq(
    Seq(
      Seq(1212, 318, 257, 1332),
      Seq(1334, 259, 320, 1214))
  ).toDF("bulkIndex")

  test("Basic Usage") {
    testCompletion(promptCompletion, promptDF, 10)
  }

  test("Bulk Prompt") {
    testCompletion(bulkPromptCompletion, bulkPromptDF, 20)
  }

  test("Indexes") {
    testCompletion(indexCompletion, indexDF, 10)
  }

  test("Bulk Indexes") {
    testCompletion(bulkIndexCompletion, bulkIndexDF, 20)
  }

  def testCompletion(completion: OpenAICompletion, df: DataFrame, requiredLength: Int): Unit = {
    val fromRow = CompletionResponse.makeFromRowConverter
    val transformed = completion.transform(df)
    //transformed.show(truncate=false) // uncomment for debugging
    transformed.collect().map(r =>
      assert(fromRow(r.getAs[Row]("out")).choices.head.text.length > requiredLength))
  }

  def newCompletion(): OpenAICompletion = {
    new OpenAICompletion()
      .setSubscriptionKey(openAIAPIKey)
      .setDeploymentName("text-davinci-001")
      .setServiceName(openAIServiceName)
      .setMaxTokens(20)
      .setLogProbs(5)
      .setOutputCol("out")
  }

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    super.assertDFEq(df1.drop("out"), df2.drop("out"))(eq)
  }

  override def testObjects(): Seq[TestObject[OpenAICompletion]] = Seq(
    new TestObject(promptCompletion, promptDF),
    new TestObject(bulkPromptCompletion, bulkPromptDF),
    new TestObject(indexCompletion, indexDF),
    new TestObject(bulkIndexCompletion, bulkIndexDF))

  override def reader: MLReadable[_] = OpenAICompletion

}


