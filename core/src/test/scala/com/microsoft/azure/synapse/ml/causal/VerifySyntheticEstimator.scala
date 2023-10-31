// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import com.microsoft.azure.synapse.ml.causal.linalg.{MatrixEntry, VectorEntry}
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}

class VerifySyntheticEstimator extends TestBase {
  import spark.implicits._

  private val unitIdxCol = "Unit_Idx"
  private val timeIdxCol = "Time_Idx"

  test("imputeTimeSeries") {
    val df = Seq(
      (0, 3, 1.0),
      (0, 5, 2.0),
      (0, 8, 5.0)
    ) toDF (unitIdxCol, timeIdxCol, "Outcome")

    val result = SyntheticEstimator.imputeTimeSeries(df, 10, "Outcome", unitIdxCol, timeIdxCol)

    val expected = Seq(
      (0, 0, 1.0),
      (0, 1, 1.0),
      (0, 2, 1.0),
      (0, 3, 1.0),
      (0, 4, 1.5),
      (0, 5, 2.0),
      (0, 6, 3.5),
      (0, 7, 3.5),
      (0, 8, 5.0),
      (0, 9, 5.0)
    ) toDF (unitIdxCol, timeIdxCol, "Outcome")

    super.verifyResult(expected, result)
  }

  test("imputeMissingValues") {
    val testData = Map(3 -> 1.0, 5 -> 2.0, 8 -> 5.0)
    val imputed = SyntheticEstimator.imputeMissingValues(10)(testData).toMap
    assert(
      imputed === Map(
        0 -> 1.0,
        1 -> 1.0,
        2 -> 1.0,
        3 -> 1.0,
        4 -> 1.5,
        5 -> 2.0,
        6 -> 3.5,
        7 -> 3.5,
        8 -> 5.0,
        9 -> 5.0
      )
    )
  }

  test("convertToBDM") {
    val bdm = SyntheticEstimator.convertToBDM(Array(
      MatrixEntry(0, 0, 3.0),
      MatrixEntry(1, 1, 3.0),
      MatrixEntry(2, 2, 3.0)
    ), (3, 3))

    assert(
      bdm === (BDM.eye[Double](3) *:* 3d)
    )
  }

  test("convertToBDV") {
    val bdv = SyntheticEstimator.convertToBDV(Array(
      VectorEntry(0, 3.0),
      VectorEntry(1, 3.0),
      VectorEntry(2, 3.0)
    ), 3)

    assert(
      bdv === (BDV.ones[Double](3) *:* 3d)
    )
  }

  test("createIndex") {
    val df = super.makeBasicDF()
    val index = SyntheticEstimator.createIndex(df, "numbers", "index")
    val expected = Seq(
      (0, 0),
      (1, 1),
      (2, 2)
    ) toDF("index", "numbers")
    super.verifyResult(expected, index)
  }
}
