// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.spark

import com.microsoft.azure.synapse.ml.core.spark.FluentAPI._
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.feature.{StringIndexer, Tokenizer}

class VerifyFluentAPI extends TestBase {

  import spark.implicits._

  private lazy val testDF =
    spark.createDataFrame(Seq(("hello world", 1), ("foo bar", 2))).toDF("text", "label")

  test("mlTransform applies a single transformer") {
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("tokens")
    val result = new DataFrameSugars(testDF).mlTransform(tokenizer)
    assert(result.columns.contains("tokens"))
    assert(result.count() === 2)
  }

  test("mlTransform with varargs applies multiple transformers in sequence") {
    val tok1 = new Tokenizer().setInputCol("text").setOutputCol("tokens1")
    val tok2 = new Tokenizer().setInputCol("text").setOutputCol("tokens2")
    val result = new DataFrameSugars(testDF).mlTransform(tok1, tok2)
    assert(result.columns.contains("tokens1"))
    assert(result.columns.contains("tokens2"))
    assert(result.count() === 2)
  }

  test("mlFit fits an estimator and returns a model") {
    val indexer = new StringIndexer().setInputCol("text").setOutputCol("textIndex")
    val model = new DataFrameSugars(testDF).mlFit(indexer)
    val result = model.transform(testDF)
    assert(result.columns.contains("textIndex"))
  }

  test("implicit conversion toSugaredDF works") {
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("tokens")
    val result: org.apache.spark.sql.DataFrame = testDF.mlTransform(tokenizer)
    assert(result.columns.contains("tokens"))
    assert(result.count() === 2)
  }
}
