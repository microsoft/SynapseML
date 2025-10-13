// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.Flaky
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{ TestObject, TransformerFuzzing }
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.linalg.Vector
import org.scalactic.Equality

class OpenAIEmbeddingsSuite extends TransformerFuzzing[OpenAIEmbedding] with OpenAIAPIKey with Flaky {

  import spark.implicits._

  private var originalEmbeddingDefault: Option[String] = None

  override def beforeAll(): Unit = {
    // Ensure a global embedding deployment is available for tests that don't set per-instance deployment
    originalEmbeddingDefault = OpenAIDefaults.getEmbeddingDeploymentName
    OpenAIDefaults.setEmbeddingDeploymentName("text-embedding-ada-002")
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    // Restore prior global state to avoid contaminating other tests
    originalEmbeddingDefault match {
      case Some(v) => OpenAIDefaults.setEmbeddingDeploymentName(v)
      case None => OpenAIDefaults.resetEmbeddingDeploymentName()
    }
    super.afterAll()
  }

  lazy val embedding: OpenAIEmbedding = new OpenAIEmbedding()
    .setSubscriptionKey(openAIAPIKey)
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

  test("Embedding uses global embedding deployment name when per-instance is not set") {
    val originalEmbeddingDeploymentName = OpenAIDefaults.getEmbeddingDeploymentName

    OpenAIDefaults.setEmbeddingDeploymentName("text-embedding-ada-002")

    val t = new OpenAIEmbedding()
      .setSubscriptionKey(openAIAPIKey)
      .setCustomServiceName(openAIServiceName)
      .setTextCol("text")
      .setOutputCol("out")

    t.transform(df).collect().foreach(r => {
      val v = r.getAs[Vector]("out")
      assert(v.size > 0)
    })

    // Reset global defaults to avoid cross-test contamination
    if (originalEmbeddingDeploymentName.isDefined) {
      OpenAIDefaults.setEmbeddingDeploymentName(originalEmbeddingDeploymentName.get)
    } else {
      OpenAIDefaults.resetEmbeddingDeploymentName()
    }
  }

  test("Embedding ignores general deployment default when embedding default is set") {
    val originalGeneral = OpenAIDefaults.getDeploymentName
    val originalEmbedding = OpenAIDefaults.getEmbeddingDeploymentName

    // Set a general default that is not an embedding model and a valid embedding default
    OpenAIDefaults.setDeploymentName("gpt-4.1-mini")
    OpenAIDefaults.setEmbeddingDeploymentName("text-embedding-ada-002")

    val t = new OpenAIEmbedding()
      .setSubscriptionKey(openAIAPIKey)
      .setCustomServiceName(openAIServiceName)
      .setTextCol("text")
      .setOutputCol("out")

    t.transform(df).collect().foreach(r => {
      val v = r.getAs[Vector]("out")
      assert(v.size > 0)
    })

    // Reset global defaults to avoid cross-test contamination
    if (originalGeneral.isDefined) OpenAIDefaults.setDeploymentName(originalGeneral.get)
    else OpenAIDefaults.resetDeploymentName()
    if (originalEmbedding.isDefined) OpenAIDefaults.setEmbeddingDeploymentName(originalEmbedding.get)
    else OpenAIDefaults.resetEmbeddingDeploymentName()
  }

  lazy val embeddingExtra: OpenAIEmbedding = new OpenAIEmbedding()
    .setSubscriptionKey(openAIAPIKey)
    .setApiVersion("2024-03-01-preview")
    .setDimensions(100)
    .setUser("testUser")
    .setCustomServiceName(openAIServiceName)
    .setTextCol("text")
    .setOutputCol("out")

  test("Extra Params Usage") {
    embeddingExtra.transform(df).collect().foreach(r => {
      val v = r.getAs[Vector]("out")
      assert(v.size == 100)
    })
  }


  override def testObjects(): Seq[TestObject[OpenAIEmbedding]] =
    Seq(new TestObject(embedding, df))

  override def reader: MLReadable[_] = OpenAIEmbedding

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    super.assertDFEq(df1.drop("out"), df2.drop("out"))(eq)
  }
}
