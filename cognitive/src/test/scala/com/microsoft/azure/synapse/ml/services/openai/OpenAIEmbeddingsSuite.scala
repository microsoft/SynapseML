// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.Flaky
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.linalg.Vector

class OpenAIEmbeddingsSuite extends TransformerFuzzing[OpenAIEmbedding] with OpenAIAPIKey with Flaky {

  import spark.implicits._

  lazy val embedding: OpenAIEmbedding = new OpenAIEmbedding()
    .setSubscriptionKey(openAIAPIKey)
    .setDeploymentName("text-embedding-ada-002")
    .setCustomServiceName(openAIServiceName)
    .setTextCol("text")
    .setOutputCol("out")

  lazy val df: DataFrame = Seq(
    "Once upon a time",
    "Best programming language award goes to",
    "SynapseML is "
  ).toDF("text")

  test("Basic Usage") {
    embedding.transform(df).collect().foreach(r => {
      val v = r.getAs[Vector]("out")
      assert(v.size > 0)
    })
  }

  override def testObjects(): Seq[TestObject[OpenAIEmbedding]] =
    Seq(new TestObject(embedding, df))

  override def reader: MLReadable[_] = OpenAIEmbedding

}
