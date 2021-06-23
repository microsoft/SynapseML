// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.core.utils

import com.microsoft.ml.spark.core.test.base.TestBase
import org.apache.spark.SparkException
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType}
import org.scalatest.Matchers.{a, thrownBy}

class SlicerFunctionsSuite extends TestBase {
  test("SlicerFunctions UDFs can handle different types of inputs") {
    import spark.implicits._

    val df = Seq(
      (Array(1, 2, 3), Vectors.dense(1, 2, 3), Map(0 -> 1f, 1 -> 2f, 2 -> 3f))
    ) toDF("label1", "label2", "label3")

    val ind = Array(0, 2)
    // array of Int
    val udf1 = SlicerFunctions.arraySlicer(IntegerType)
    val Tuple1(v1) = df.select(udf1(col("label1"), lit(ind))).as[Tuple1[Vector]].head
    assert(v1 == Vectors.dense(1d, 3d))

    // vector
    val udf2 = SlicerFunctions.vectorSlicer

    val Tuple1(v2) = df.select(udf2(col("label2"), lit(ind))).as[Tuple1[Vector]].head
    assert(v2 == Vectors.dense(1d, 3d))

    // Map of Int -> Float
    val udf3 = SlicerFunctions.mapSlicer(FloatType)
    val Tuple1(v3) = df.select(udf3(col("label3"), lit(ind))).as[Tuple1[Vector]].head
    assert(v3 == Vectors.dense(1d, 3d))

    a[SparkException] shouldBe thrownBy {
      val udf4 = SlicerFunctions.arraySlicer(FloatType) // Type does not match label1 column.
      df.select(udf4(col("label1"), lit(ind))).as[Tuple1[Vector]].head
    }
  }
}
