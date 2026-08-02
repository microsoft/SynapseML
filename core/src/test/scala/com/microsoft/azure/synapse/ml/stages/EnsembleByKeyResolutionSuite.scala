// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.SparkException
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.linalg.{SQLDataTypes, Vector}
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.types.{DoubleType, IntegerType, Metadata, StringType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row}

/** Covers the duplicate attribute resolution rules that EnsembleByKey mirrors from Spark's
  * `AttributeSeq.resolve`.
  */
class EnsembleByKeyResolutionSuite extends TestBase {

  private val duplicateKey = "__is_duplicate"

  test("custom stage identifiers should not affect internal column resolution") {
    val input = spark.createDataFrame(Seq(("group", 1.0), ("group", 3.0))).toDF("key", "score")
    Seq("ensemble.by.key", "ensemble`by`key").foreach { uid =>
      val transformer = new EnsembleByKey(uid)
        .setKey("key").setCol("score").setCollapseGroup(false)
      assert(transformer.transformSchema(input.schema) === transformer.transform(input).schema)
    }
  }

  test("non-collapsed output should retain rows with null grouping keys") {
    val schema = StructType(Array(
      StructField("id", IntegerType, nullable = false),
      StructField("key", StringType),
      StructField("score", DoubleType, nullable = false)))
    val missingKey = Option.empty[String].orNull
    val input = spark.createDataFrame(java.util.Arrays.asList(
      Row(0, missingKey, 1.0),
      Row(1, missingKey, 3.0),
      Row(2, "group", 5.0)), schema)
    val transformed = new EnsembleByKey()
      .setKey("key").setCol("score").setCollapseGroup(false).transform(input)

    assert(transformed.orderBy("id").collect().map(row =>
      (row.getInt(1), Option(row.getString(0)), row.getDouble(3))) ===
      Array((0, None, 2.0), (1, None, 2.0), (2, Some("group"), 5.0)))
  }

  test("vector mean schema should match Spark for all-null inputs") {
    val schema = StructType(Array(
      StructField("key", StringType, nullable = false),
      StructField("features", SQLDataTypes.VectorType)))
    val missingVector = Option.empty[Vector].orNull
    val input = spark.createDataFrame(java.util.Arrays.asList(
      Row("group", missingVector),
      Row("group", missingVector)), schema)
    val transformer = new EnsembleByKey().setKey("key").setCol("features")
    val transformed = transformer.transform(input)

    assert(transformer.transformSchema(input.schema) === transformed.schema)
    assert(!transformed.schema("mean(features)").nullable)
    intercept[SparkException](transformed.collect())
  }

  test("duplicated qualifier attributes should follow Spark expression identity") {
    val base = spark.createDataFrame(Seq(("top", "nested", 1.0), ("top", "nested", 3.0)))
      .toDF("group", "nestedGroup", "score")
    val nestedGroup = struct(col("nestedGroup").alias("group")).alias("dup")
    val shared = base.select(col("group"), col("group"), nestedGroup, col("score"))
    val transformer = new EnsembleByKey().setKey("dup.group").setCol("score")

    assert(distinctExpressions(shared, "group") === 1)
    Seq("dup" -> "top", "other" -> "nested").foreach { case (alias, expected) =>
      val transformed = assertSchemaAgrees(transformer, shared.as(alias))
      withClue(s"$alias: ") {
        assert(transformed.head().getString(0) === expected)
        assert(transformed.select("mean(score)").head().getDouble(0) === 2.0)
      }
    }

    val ambiguous = base.select(col("group"), nestedGroup, col("score")).as("dup")
      .crossJoin(spark.createDataFrame(Seq(Tuple1("side"))).toDF("group").as("dup"))
    assert(distinctExpressions(ambiguous, "group") === 2)
    intercept[AnalysisException](ambiguous.select("dup.group"))
    val error = intercept[IllegalArgumentException](transformer.transform(ambiguous))
    assert(error.getMessage.contains("dup.group is ambiguous"))
  }

  test("duplicated unqualified attributes sharing one expression should aggregate") {
    val base = spark.createDataFrame(Seq(("group", 1.0), ("group", 3.0))).toDF("key", "score")
    val duplicated = base.select(col("key"), col("score"), col("score"))
    val transformer = new EnsembleByKey().setKey("key").setCol("score")

    assert(duplicated.schema.fieldNames === Array("key", "score", "score"))
    assert(distinctExpressions(duplicated, "score") === 1)
    assert(duplicated.select("score").columns === Array("score"))

    val transformed = transformer.transform(duplicated)
    assert(transformed.schema.fieldNames === Array("key", "mean(score)"))
    assert(transformed.schema("mean(score)") === StructField("mean(score)", DoubleType))
    assert(transformed.head().getDouble(1) === 2.0)

    assert(transformer.transformSchema(duplicated.schema) === transformed.schema)
    val pipelineModel = new Pipeline().setStages(Array(transformer)).fit(duplicated)
    assert(pipelineModel.transform(duplicated).collect() === transformed.collect())
  }

  test("union duplicate attributes should follow Spark duplicate pruning") {
    val base = spark.createDataFrame(Seq(("group", 1.0), ("group", 3.0))).toDF("key", "score")
    val duplicated = base.select(col("key"), col("score"), col("score"))
    val unioned = duplicated.union(duplicated)
    assert(unioned.schema.fieldNames === Array("key", "score", "score"))
    assert(unioned.schema.fields.last.metadata.contains(duplicateKey))
    assert(distinctExpressions(unioned, "score") === 2)
    assert(unioned.select("score").columns === Array("score"))

    val transformed = assertSchemaAgrees(new EnsembleByKey().setKey("key").setCol("score"), unioned)
    assert(transformed.schema.fieldNames === Array("key", "mean(score)"))
    assert(transformed.head().getDouble(1) === 2.0)

    val qualified = assertSchemaAgrees(
      new EnsembleByKey().setKey("key").setCol("u.score"), unioned.as("u"))
    assert(qualified.schema.fieldNames === Array("key", "mean(u.score)"))
    assert(qualified.head().getDouble(1) === 2.0)
  }

  test("duplicate pruning should not override qualifier selection") {
    // The only `group` attribute of `u` carries Spark's duplicate marker while `v.group` does not,
    // so pruning before qualifier selection would silently resolve `u.group` to `v.group`.
    val base = spark.createDataFrame(Seq(("u", 1.0), ("u", 3.0))).toDF("group", "score")
    val duplicated = base.select(col("group"), col("group"), col("score"))
    val tagged = duplicated.union(duplicated).toDF("other", "group", "score")
    assert(tagged.schema("group").metadata.contains(duplicateKey))
    assert(!tagged.schema("other").metadata.contains(duplicateKey))

    val untagged = spark.createDataFrame(Seq(Tuple1("v"))).toDF("group")
    val joined = tagged.as("u").crossJoin(untagged.as("v"))
    assert(joined.schema.fieldNames === Array("other", "group", "score", "group"))
    assert(joined.select("u.group").head().getString(0) === "u")

    val transformed = assertSchemaAgrees(
      new EnsembleByKey().setKey("u.group").setCol("score"), joined)
    assert(transformed.schema.fieldNames === Array("group", "mean(score)"))
    assert(transformed.schema("group").metadata === Metadata.empty)
    assert(transformed.head().getString(0) === "u")
    assert(transformed.head().getDouble(1) === 2.0)

    val nonCollapsed = new EnsembleByKey().setKey("u.group").setCol("score").setCollapseGroup(false)
    val schemaError = intercept[IllegalArgumentException](nonCollapsed.transformSchema(joined.schema))
    val transformError = intercept[IllegalArgumentException](nonCollapsed.transform(joined))
    assert(schemaError.getMessage.contains("multiple columns are named group"))
    assert(transformError.getMessage.contains("multiple columns are named group"))
  }

  private def distinctExpressions(input: DataFrame, name: String): Int = {
    input.queryExecution.analyzed.output.filter(_.name == name).map(_.exprId).distinct.length
  }

  private def assertSchemaAgrees(transformer: EnsembleByKey, input: DataFrame): DataFrame = {
    val transformed = transformer.transform(input)
    assert(transformer.transformSchema(input.schema) === transformed.schema)
    transformed
  }
}
