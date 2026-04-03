// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.sql.types.{StringType, StructField}

class VerifyUnicodeNormalize extends TestBase {
  import spark.implicits._

  test("normalizes unicode text") {
    // e + combining acute accent (decomposed) vs precomposed e-acute
    val decomposed = "cafe\u0301"
    val df = Seq(decomposed).toDF("text")
    val normalizer = new UnicodeNormalize()
      .setInputCol("text")
      .setOutputCol("normalized")
      .setForm("NFC")
      .setLower(false)
    val result = normalizer.transform(df).select("normalized").collect()
    assert(result.head.getString(0) === "caf\u00e9")
  }

  test("lower=true lowercases output") {
    val df = Seq("HELLO").toDF("text")
    val normalizer = new UnicodeNormalize()
      .setInputCol("text")
      .setOutputCol("out")
      .setLower(true)
    val result = normalizer.transform(df).select("out").collect()
    assert(result.head.getString(0) === "hello")
  }

  test("lower=false preserves case") {
    val df = Seq("Hello").toDF("text")
    val normalizer = new UnicodeNormalize()
      .setInputCol("text")
      .setOutputCol("out")
      .setLower(false)
    val result = normalizer.transform(df).select("out").collect()
    assert(result.head.getString(0).contains("H"))
  }

  test("default form is NFKD") {
    val normalizer = new UnicodeNormalize()
    assert(normalizer.getForm === "NFKD")
  }

  test("transformSchema adds output column") {
    val df = Seq("a").toDF("text")
    val normalizer = new UnicodeNormalize()
      .setInputCol("text")
      .setOutputCol("out")
    val schema = normalizer.transformSchema(df.schema)
    assert(schema.fieldNames.contains("out"))
    assert(schema(schema.fieldIndex("out")) === StructField("out", StringType))
  }
}
