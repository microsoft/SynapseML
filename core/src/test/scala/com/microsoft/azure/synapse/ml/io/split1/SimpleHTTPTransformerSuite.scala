// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.split1

import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.io.http.{HandlingUtils, JSONOutputParser, SimpleHTTPTransformer}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
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

  test("HttpTransformerTest with Flaky Connection") {
    lazy val df2: DataFrame = sc.parallelize((1 to 5).map(Tuple1(_))).toDF("data")
    val results = simpleTransformer
      .setUrl(url + "/flaky")
      .setTimeout(1)
      .transform(df2).collect
    assert(results.length == 5)
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
