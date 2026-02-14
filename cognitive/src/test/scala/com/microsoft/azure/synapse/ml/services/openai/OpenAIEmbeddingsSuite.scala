// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.Flaky
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.ml.linalg.{Vector, SQLDataTypes}
import org.apache.spark.sql.types.StructType
import org.scalactic.Equality

class OpenAIEmbeddingsSuite extends TransformerFuzzing[OpenAIEmbedding] with OpenAIAPIKey with Flaky {
  override val compareDataInSerializationTest: Boolean = false


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
    .setDeploymentName("text-embedding-ada-002")
    .setTextCol("text")
    .setOutputCol("out")

  lazy val df: DataFrame = Seq(
    "Once upon a time",
    "Best programming language award goes to",
    "SynapseML is "
  ).toDF("text")

  private def usageEmbedding(outputCol: String): OpenAIEmbedding =
    new OpenAIEmbedding()
      .setSubscriptionKey(openAIAPIKey)
      .setCustomServiceName(openAIServiceName)
      .setDeploymentName("text-embedding-ada-002")
      .setTextCol("text")
      .setOutputCol(outputCol)

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
    .setUser("testUser")
    .setCustomServiceName(openAIServiceName)
    .setDeploymentName("text-embedding-ada-002")
    .setTextCol("text")
    .setOutputCol("out")

  test("Extra Params Usage") {
    embeddingExtra.transform(df).collect().foreach(r => {
      val v = r.getAs[Vector]("out")
      assert(v != null && v.size > 0)
    })
  }

  test("usageCol not set keeps vector output") {
    val e = usageEmbedding("embeddings")
    val schema = e.transformSchema(df.schema)
    assert(schema(e.getOutputCol).dataType == SQLDataTypes.VectorType)
    assert(!schema.fieldNames.contains("usage"))

    val row = e.transform(df.limit(1)).select("embeddings").collect().head
    val vector = row.getAs[Vector](0)
    assert(vector.size > 0)
  }

  test("usageCol set emits vector in outputCol and usage in separate column") {
    val e = usageEmbedding("embeddings").setUsageCol("usage")
    val schema = e.transformSchema(df.schema)
    assert(schema(e.getOutputCol).dataType == SQLDataTypes.VectorType)
    assert(schema.fieldNames.contains("usage"))

    val result = e.transform(df.limit(1))
    val row = result.select("embeddings", "usage").collect().head

    // Vector is in embeddings column
    val vector = row.getAs[Vector]("embeddings")
    assert(vector.size > 0)

    // Usage is in separate usage column
    val usageRow = row.getAs[Row]("usage")
    assert(usageRow != null)
    val usageFields = usageRow.schema.fieldNames.toSet
    assert(usageFields.contains("input_tokens"))
    assert(usageFields.contains("total_tokens"))
    val inputTokens = usageRow.getAs[java.lang.Long]("input_tokens")
    assert(inputTokens == null || inputTokens.longValue() >= 0)
    val inputDetails = usageRow.getAs[scala.collection.Map[String, Long]]("input_token_details")
    assert(inputDetails != null)
    val outputDetails = usageRow.getAs[scala.collection.Map[String, Long]]("output_token_details")
    assert(outputDetails != null)
  }

  test("null input returns null output without usageCol") {
    val dfWithNull = Seq(
      Some("Once upon a time"),
      None,
      Some("SynapseML is ")
    ).toDF("text")

    val e = usageEmbedding("embeddings")
    val results = e.transform(dfWithNull).collect()

    assert(results(0).getAs[Vector]("embeddings") != null)
    assert(results(1).getAs[Vector]("embeddings") == null)
    assert(results(2).getAs[Vector]("embeddings") != null)
  }

  test("null input returns null output with usageCol set") {
    val dfWithNull = Seq(
      Some("Once upon a time"),
      None,
      Some("SynapseML is ")
    ).toDF("text")

    val e = usageEmbedding("embeddings").setUsageCol("usage")
    val results = e.transform(dfWithNull).collect()

    // First row: both vector and usage should be present
    assert(results(0).getAs[Vector]("embeddings") != null)
    assert(results(0).getAs[Row]("usage") != null)

    // Second row: null input should result in null outputs
    assert(results(1).getAs[Vector]("embeddings") == null)
    assert(results(1).getAs[Row]("usage") == null)

    // Third row: both vector and usage should be present
    assert(results(2).getAs[Vector]("embeddings") != null)
    assert(results(2).getAs[Row]("usage") != null)
  }


  override def testObjects(): Seq[TestObject[OpenAIEmbedding]] =
    Seq(new TestObject(embedding, df))

  override def reader: MLReadable[_] = OpenAIEmbedding

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    super.assertDFEq(df1.drop("out"), df2.drop("out"))(eq)
  }
}
