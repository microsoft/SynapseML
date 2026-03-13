// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.sql.types.{StringType, StructField}

class VerifyTextPreprocessor extends TestBase {
  import spark.implicits._

  test("replaces matched substrings in DataFrame column") {
    val df = Seq("hello world").toDF("text")
    val tp = new TextPreprocessor()
      .setInputCol("text")
      .setOutputCol("out")
      .setMap(Map("hello" -> "hi"))
    val result = tp.transform(df).select("out").collect()
    assert(result.head.getString(0) === "hi world")
  }

  test("no matches returns original text") {
    val df = Seq("hello world").toDF("text")
    val tp = new TextPreprocessor()
      .setInputCol("text")
      .setOutputCol("out")
      .setMap(Map("xyz" -> "abc"))
    val result = tp.transform(df).select("out").collect()
    assert(result.head.getString(0) === "hello world")
  }

  test("multiple replacements in same text") {
    val df = Seq("hello world.").toDF("text")
    val tp = new TextPreprocessor()
      .setInputCol("text")
      .setOutputCol("out")
      .setMap(Map("hello" -> "hi", "world" -> "earth"))
    val result = tp.transform(df).select("out").collect()
    assert(result.head.getString(0) === "hi earth.")
  }

  test("transformSchema adds output column") {
    val df = Seq("a").toDF("text")
    val tp = new TextPreprocessor()
      .setInputCol("text")
      .setOutputCol("out")
    val schema = tp.transformSchema(df.schema)
    assert(schema.fieldNames.contains("out"))
    assert(schema(schema.fieldIndex("out")) === StructField("out", StringType))
  }
}
