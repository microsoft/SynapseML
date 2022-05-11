// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive.split1

import com.microsoft.azure.synapse.ml.Secrets
import com.microsoft.azure.synapse.ml.cognitive._
import com.microsoft.azure.synapse.ml.core.test.base.{Flaky, TestBase}
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalactic.Equality

trait OpenAIAPIKey {
  lazy val openAIAPIKey: String = sys.env.getOrElse("OPENAI_API_KEY", Secrets.OpenAIApiKey)
  lazy val openAIServiceName: String = "bugbashtest6"
}

class OpenAICompletionSuite extends TransformerFuzzing[OpenAICompletion] with OpenAIAPIKey with Flaky {

  import spark.implicits._

  lazy val promptCompletion: OpenAICompletion = newCompletion("prompt")
  lazy val bulkPromptsCompletion: OpenAICompletion = newCompletion("bulkPrompts")
  lazy val indexesCompletion: OpenAICompletion = newCompletion("indexes")
  lazy val bulkIndexesCompletion: OpenAICompletion = newCompletion("bulkIndexes")

  lazy val promptDF: DataFrame = Seq(
    "Once upon a time",
    "Best programming language award goes to",
    "SynapseML is "
  ).toDF("prompt")

  lazy val bulkPromptsDF: DataFrame = Seq(
    Seq(
      "Now is the time",
      "Knock, knock",
      "Ask not")
  ).toDF("bulkPrompts")

  lazy val indexesDF: DataFrame = Seq(
    Seq(1212, 318, 247, 1332)
  ).toDF("indexes")

  lazy val bulkIndexesDF: DataFrame = Seq(
    Seq(
      Seq(1212, 318, 257, 1332),
      Seq(1334, 259, 320, 1214))
  ).toDF("bulkIndexes")

  test("Basic Usage") {
    val fromRow = CompletionResponse.makeFromRowConverter
    val transformed = promptCompletion.transform(promptDF)
    transformed.show(truncate=false)
    transformed.collect().map(r =>
      assert(fromRow(r.getAs[Row]("out")).choices.head.text.length > 10))
  }

  test("Bulk Prompts") {
    val fromRow = CompletionResponse.makeFromRowConverter
    val transformed = bulkPromptsCompletion.transform(bulkPromptsDF)
    transformed.show(truncate=false)
    transformed.collect().map(r =>
      assert(fromRow(r.getAs[Row]("out")).choices.head.text.length > 20))
  }

  test("Indexes") {
    val fromRow = CompletionResponse.makeFromRowConverter
    val transformed = indexesCompletion.transform(indexesDF)
    transformed.show(truncate=false)
    transformed.collect().map(r =>
      assert(fromRow(r.getAs[Row]("out")).choices.head.text.length > 10))
  }

  test("Bulk Indexes") {
    val fromRow = CompletionResponse.makeFromRowConverter
    val transformed = bulkIndexesCompletion.transform(bulkIndexesDF)
    transformed.show(truncate=false)
    transformed.collect().map(r =>
      assert(fromRow(r.getAs[Row]("out")).choices.head.text.length > 20))
  }

  def newCompletion(prompt: String): OpenAICompletion = {
    new OpenAICompletion()
      .setSubscriptionKey(openAIAPIKey)
      .setDeploymentName("text-davinci-001")
      .setServiceName(openAIServiceName)
      .setMaxTokens(20)
      .setLogProbs(5)
      .setPromptCol(prompt)
      .setOutputCol("out")
  }

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    super.assertDFEq(df1.drop("out"), df2.drop("out"))(eq)
  }

  override def testObjects(): Seq[TestObject[OpenAICompletion]] = Seq(
    new TestObject(promptCompletion, promptDF),
    new TestObject(bulkPromptsCompletion, bulkPromptsDF),
    new TestObject(indexesCompletion, indexesDF),
    new TestObject(bulkIndexesCompletion, bulkIndexesDF))

  override def reader: MLReadable[_] = OpenAICompletion

}


