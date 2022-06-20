// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive.split1

import com.microsoft.azure.synapse.ml.Secrets
import com.microsoft.azure.synapse.ml.cognitive._
import com.microsoft.azure.synapse.ml.core.test.base.Flaky
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row}
import org.scalactic.Equality

trait OpenAIAPIKey {
  lazy val openAIAPIKey: String = sys.env.getOrElse("OPENAI_API_KEY", Secrets.OpenAIApiKey)
  lazy val openAIServiceName: String = "m3test11"
}

class OpenAICompletionSuite extends TransformerFuzzing[OpenAICompletion] with OpenAIAPIKey with Flaky {

  import spark.implicits._

  lazy val completion: OpenAICompletion = new OpenAICompletion()
    .setSubscriptionKey(openAIAPIKey)
    .setDeploymentName("text-davinci-001")
    .setServiceName(openAIServiceName)
    .setMaxTokens(20)
    .setLogProbs(5)
    .setPromptCol("prompt")
    .setOutputCol("out")

  lazy val df: DataFrame = Seq(
    "Once upon a time",
    "Best programming language award goes to",
    "SynapseML is "
  ).toDF("prompt")

  test("Basic Usage") {
    val fromRow = CompletionResponse.makeFromRowConverter
    completion.transform(df).collect().map(r =>
      fromRow(r.getAs[Row]("out")).choices.head.text.length > 10)
  }

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    super.assertDFEq(df1.drop("out"), df2.drop("out"))(eq)
  }

  override def testObjects(): Seq[TestObject[OpenAICompletion]] =
    Seq(new TestObject(completion, df))

  override def reader: MLReadable[_] = OpenAICompletion

}
