// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class VerifyRowUtils extends TestBase {

  import RowUtils.RowCanGetAsDouble

  test("getAsDouble converts Byte to Double") {
    val schema = StructType(Seq(StructField("value", ByteType)))
    val row = Row(10.toByte)
    val result = row.getAsDouble(0)
    assert(result === 10.0)
  }

  test("getAsDouble converts Short to Double") {
    val schema = StructType(Seq(StructField("value", ShortType)))
    val row = Row(100.toShort)
    val result = row.getAsDouble(0)
    assert(result === 100.0)
  }

  test("getAsDouble converts Int to Double") {
    val schema = StructType(Seq(StructField("value", IntegerType)))
    val row = Row(42)
    val result = row.getAsDouble(0)
    assert(result === 42.0)
  }

  test("getAsDouble converts Long to Double") {
    val schema = StructType(Seq(StructField("value", LongType)))
    val row = Row(1000L)
    val result = row.getAsDouble(0)
    assert(result === 1000.0)
  }

  test("getAsDouble converts Float to Double") {
    val schema = StructType(Seq(StructField("value", FloatType)))
    val row = Row(3.14f)
    val result = row.getAsDouble(0)
    assert(Math.abs(result - 3.14) < 0.001)
  }

  test("getAsDouble returns Double unchanged") {
    val schema = StructType(Seq(StructField("value", DoubleType)))
    val row = Row(2.718)
    val result = row.getAsDouble(0)
    assert(result === 2.718)
  }

  test("getAsDouble by column name") {
    val schema = StructType(Seq(
      StructField("other", StringType),
      StructField("value", IntegerType)
    ))
    // Create a row that knows about its schema
    val row = new org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema(
      Array("test", 42),
      schema
    )
    val result = row.getAsDouble("value")
    assert(result === 42.0)
  }

  test("getAsDouble throws for unsupported types") {
    val schema = StructType(Seq(StructField("value", StringType)))
    val row = Row("not a number")
    assertThrows[Exception] {
      row.getAsDouble(0)
    }
  }

  test("getAsDouble handles negative numbers") {
    val schema = StructType(Seq(StructField("value", IntegerType)))
    val row = Row(-100)
    val result = row.getAsDouble(0)
    assert(result === -100.0)
  }

  test("getAsDouble handles zero") {
    val schema = StructType(Seq(StructField("value", IntegerType)))
    val row = Row(0)
    val result = row.getAsDouble(0)
    assert(result === 0.0)
  }

  test("getAsDouble handles large Long values") {
    val schema = StructType(Seq(StructField("value", LongType)))
    val largeValue = Long.MaxValue / 2
    val row = Row(largeValue)
    val result = row.getAsDouble(0)
    // Note: some precision loss expected for very large longs
    assert(result > 0)
  }
}
