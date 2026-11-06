// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.split1

import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.io.http.{HandlingUtils, JSONOutputParser, SimpleHTTPTransformer, StatusLineData}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{lit, struct}
import org.apache.spark.sql.types.{StringType, StructType}

class SimpleHTTPTransformerSuite
  extends TransformerFuzzing[SimpleHTTPTransformer] with WithServer {

  import spark.implicits._

  lazy val df: DataFrame = sc.parallelize((1 to 10).map(Tuple1(_))).toDF("data")

  def simpleTransformer: SimpleHTTPTransformer =
    new SimpleHTTPTransformer()
      .setInputCol("data")
      .setOutputParser(new JSONOutputParser()
        .setDataType(new StructType().add("blah", StringType)))
      .setUrl(url)
      .setOutputCol("results")

  test("HttpTransformerTest") {
    val results = simpleTransformer.transform(df).collect
    assert(results.length == 10)
    results.foreach(r =>
      assert(r.getStruct(2).getString(0) === "more blah"))
    assert(results(0).schema.fields.length == 3)
  }

  test("schema planning does not create a Spark session or overwrite internal column names") {
    val active = SparkSession.getActiveSession
    val default = SparkSession.getDefaultSession
    val input = new StructType().add("data", StringType)
    for (error <- Seq("parsedInput", "unparsedOutput")) {
      val transformer = simpleTransformer.setErrorCol(error)
      try {
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
        val planned = transformer.transformSchema(input)
        assert(SparkSession.getActiveSession.isEmpty)
        assert(SparkSession.getDefaultSession.isEmpty)
        assert(planned.fieldNames.toSeq == Seq("data", error, "results"))
      } finally {
        active.foreach(SparkSession.setActiveSession)
        default.foreach(SparkSession.setDefaultSession)
      }
      val frame = Seq("request").toDF("data")
      val output = transformer.transform(frame)
      assert(transformer.transformSchema(frame.schema) == output.schema)
      val row = output.head()
      assert(row.isNullAt(row.fieldIndex(error)))
      assert(row.getAs[org.apache.spark.sql.Row]("results").getString(0) == "more blah")
    }
  }

  test("HttpTransformerTest with Flaky Connection") {
    lazy val df2: DataFrame = sc.parallelize((1 to 5).map(Tuple1(_))).toDF("data")
    val results = simpleTransformer
      .setUrl(url + "/flaky")
      .setTimeout(1)
      .transform(df2).collect
    assert(results.length == 5)
  }

  test("schema planning preserves an existing nonnullable preparation error") {
    val frame = Seq("request").toDF("data").withColumn("error", struct(
      lit("preparation failed").as("response"), lit(None.orNull).cast(StatusLineData.schema).as("status")))
    val transformer = simpleTransformer.setErrorCol("error")
    val output = transformer.transform(frame)
    assert(transformer.transformSchema(frame.schema) == output.schema)
    val row = output.head()
    assert(row.getAs[org.apache.spark.sql.Row]("error").getString(0) == "preparation failed")
    assert(row.isNullAt(row.fieldIndex("results")))
  }

  test("schema planning matches case-only error column replacement in both resolver modes") {
    val previous = spark.conf.get("spark.sql.caseSensitive")
    try {
      for (sensitive <- Seq(false, true)) {
        spark.conf.set("spark.sql.caseSensitive", sensitive.toString)
        val frame = Seq("request").toDF("data").withColumn("ERROR", lit("original"))
        val transformer = simpleTransformer.setErrorCol("error")
        val output = transformer.transform(frame)
        assert(transformer.transformSchema(frame.schema) == output.schema)
        assert(output.columns.contains("ERROR") == sensitive)
        assert(output.head().isNullAt(output.schema.fieldIndex("error")))
        if (!sensitive) {
          val ambiguous = new StructType().add("data", StringType)
            .add("ERROR", StringType).add("Error", StringType)
          intercept[IllegalArgumentException](transformer.transformSchema(ambiguous))
        }
      }
    } finally {
      spark.conf.set("spark.sql.caseSensitive", previous)
    }
  }

  test("schema planning rejects incompatible existing error types") {
    val frame = Seq(("request", "invalid error")).toDF("data", "error")
    intercept[IllegalArgumentException] {
      simpleTransformer.setErrorCol("error").transformSchema(frame.schema)
    }
  }

  test("schema planning matches nullable and coerced preparation error types") {
    val previous = spark.conf.get("spark.sql.ansi.enabled")
    try {
      for (ansi <- Seq(false, true); error <- Seq(lit(None.orNull),
           struct(lit(42).as("response"), lit(None.orNull).cast(StatusLineData.schema).as("status")))) {
        spark.conf.set("spark.sql.ansi.enabled", ansi.toString)
        val frame = Seq("request").toDF("data").withColumn("error", error)
        val transformer = simpleTransformer.setErrorCol("error")
        val output = transformer.transform(frame)
        assert(transformer.transformSchema(frame.schema) == output.schema)
        assert(output.collect().length == 1)
      }
    } finally {
      spark.conf.set("spark.sql.ansi.enabled", previous)
    }
  }

  test("Basic Handling") {
    val results = simpleTransformer
      .setHandler(HandlingUtils.basic _)
      .transform(df).collect
    assert(results.length == 10)
    results.foreach(r =>
      assert(r.getStruct(2).getString(0) === "more blah"))
    assert(results(0).schema.fields.length == 3)
  }

  test("Concurrent HttpTransformerTest") {
    val results =
      new SimpleHTTPTransformer()
        .setInputCol("data")
        .setOutputParser(new JSONOutputParser()
          .setDataType(new StructType().add("blah", StringType)))
        .setUrl(url)
        .setOutputCol("results")
        .setConcurrency(3)
        .transform(df)
        .collect
    assert(results.length == 10)
    assert(results.forall(_.getStruct(2).getString(0) == "more blah"))
    assert(results(0).schema.fields.length == 3)
  }

  override def testObjects(): Seq[TestObject[SimpleHTTPTransformer]] =
    Seq(new TestObject(simpleTransformer, df))

  override def reader: MLReadable[_] = SimpleHTTPTransformer

}
