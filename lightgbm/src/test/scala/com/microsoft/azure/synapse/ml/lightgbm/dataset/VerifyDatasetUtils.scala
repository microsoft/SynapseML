// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.dataset

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.sql.types._

// scalastyle:off magic.number
class VerifyDatasetUtils extends TestBase {

  test("countCardinality with all same values") {
    val result = DatasetUtils.countCardinality(Seq(1, 1, 1))
    assert(result === Array(3))
  }

  test("countCardinality with all different values") {
    val result = DatasetUtils.countCardinality(Seq(1, 2, 3))
    assert(result === Array(1, 1, 1))
  }

  test("countCardinality with grouped values") {
    val result = DatasetUtils.countCardinality(Seq(1, 1, 2, 2, 2, 3))
    assert(result === Array(2, 3, 1))
  }

  test("countCardinality with empty sequence") {
    val result = DatasetUtils.countCardinality(Seq.empty[Int])
    assert(result === Array(0))
  }

  test("getArrayType with sparse returns true") {
    val iter = Iterator.empty
    val (_, isSparse) = DatasetUtils.getArrayType(iter, "sparse", "features")
    assert(isSparse)
  }

  test("getArrayType with dense returns false") {
    val iter = Iterator.empty
    val (_, isSparse) = DatasetUtils.getArrayType(iter, "dense", "features")
    assert(!isSparse)
  }

  test("getArrayType with invalid type throws") {
    intercept[Exception] {
      DatasetUtils.getArrayType(Iterator.empty, "invalid", "features")
    }
  }

  test("validateGroupColumn throws for unsupported types") {
    val schema = StructType(Seq(StructField("g", DoubleType)))
    intercept[IllegalArgumentException] {
      DatasetUtils.validateGroupColumn("g", schema)
    }
  }

  test("validateGroupColumn passes for IntegerType") {
    val schema = StructType(Seq(StructField("g", IntegerType)))
    DatasetUtils.validateGroupColumn("g", schema)
  }

  test("validateGroupColumn passes for LongType") {
    val schema = StructType(Seq(StructField("g", LongType)))
    DatasetUtils.validateGroupColumn("g", schema)
  }

  test("validateGroupColumn passes for StringType") {
    val schema = StructType(Seq(StructField("g", StringType)))
    DatasetUtils.validateGroupColumn("g", schema)
  }
}
// scalastyle:on magic.number
