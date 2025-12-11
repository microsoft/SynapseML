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

  test("returnUsage default (unset) keeps vector output") {
    val e = usageEmbedding("usage_default_vec")
    val schema = e.transformSchema(df.schema)
    assert(schema(e.getOutputCol).dataType == SQLDataTypes.VectorType)

    val row = e.transform(df.limit(1)).select("usage_default_vec").collect().head
    val vector = row.getAs[Vector](0)
    assert(vector.size > 0)
  }

  test("returnUsage set to false keeps vector output") {
    val e = usageEmbedding("usage_false_vec").setReturnUsage(false)
    val schema = e.transformSchema(df.schema)
    assert(schema(e.getOutputCol).dataType == SQLDataTypes.VectorType)

    val row = e.transform(df.limit(1)).select("usage_false_vec").collect().head
    val vector = row.getAs[Vector](0)
    assert(vector.size > 0)
  }

  test("returnUsage true emits structured response and usage map") {
    val e = usageEmbedding("usage_true_struct").setReturnUsage(true)
    val schema = e.transformSchema(df.schema)
    val structType = schema(e.getOutputCol).dataType.asInstanceOf[StructType]
    assert(structType.fieldNames.contains("response"))
    assert(structType.fieldNames.contains("usage"))

    val outputRow = e.transform(df.limit(1)).select("usage_true_struct").collect().head
    val struct = outputRow.getAs[Row](0)
    assert(struct != null)
    val vector = struct.getAs[Vector]("response")
    assert(vector.size > 0)
    val usageRowOpt = Option(struct.getAs[Row]("usage"))
    assert(usageRowOpt.isDefined)
    val usageRow = usageRowOpt.get
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

  test("null input returns null output with returnUsage false") {
    val dfWithNull = Seq(
      Some("Once upon a time"),
      None,
      Some("SynapseML is ")
    ).toDF("text")

    val e = usageEmbedding("null_test_vec")
    val results = e.transform(dfWithNull).collect()

    assert(results(0).getAs[Vector]("null_test_vec") != null)
    assert(results(1).getAs[Vector]("null_test_vec") == null)
    assert(results(2).getAs[Vector]("null_test_vec") != null)
  }

  test("null input returns null output with returnUsage true") {
    val dfWithNull = Seq(
      Some("Once upon a time"),
      None,
      Some("SynapseML is ")
    ).toDF("text")

    val e = usageEmbedding("null_test_usage").setReturnUsage(true)
    val results = e.transform(dfWithNull).collect()

    assert(results(0).getAs[Row]("null_test_usage") != null)
    assert(results(1).getAs[Row]("null_test_usage") == null)
    assert(results(2).getAs[Row]("null_test_usage") != null)
  }


  override def testObjects(): Seq[TestObject[OpenAIEmbedding]] =
    Seq(new TestObject(embedding, df))

  override def reader: MLReadable[_] = OpenAIEmbedding

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    super.assertDFEq(df1.drop("out"), df2.drop("out"))(eq)
  }
}
