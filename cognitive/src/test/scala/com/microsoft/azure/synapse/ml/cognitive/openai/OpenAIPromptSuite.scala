// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive.openai

import com.microsoft.azure.synapse.ml.core.test.base.Flaky
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.linalg.Vector

class OpenAIPromptSuite extends TransformerFuzzing[OpenAIPrompt] with OpenAIAPIKey with Flaky {

  import spark.implicits._

  lazy val prompt: OpenAIPrompt = new OpenAIPrompt()
    .setSubscriptionKey(openAIAPIKey)
    .setDeploymentName("text-davinci-001")
    .setModel("text-davinci-003")
    .setCustomServiceName(openAIServiceName)
    .setOutputCol("out")
    .setParsedOutputCol("outParsed")

  lazy val df: DataFrame = Seq(
    ("apple", "fruits"),
    ("mercedes", "cars"),
    ("cake", "dishes")
  ).toDF("text", "category")

  test("Basic Usage") {
    prompt
      .setPromptTemplate("here is a comma separated list of 5 {category}: {text}, ")
      .setPostProcessing("csv")
      .transform(df)
      .show(5, 200)
  }

  override def testObjects(): Seq[TestObject[OpenAIPrompt]] =
    Seq(new TestObject(prompt, df))

  override def reader: MLReadable[_] = OpenAIPrompt

}
