// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers.split1

import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.explainers.LocalExplainer
import org.apache.spark.ml.linalg.{Vector, Vectors}

class HasExplainTargetSuite extends TestBase {
  test("getExplainTarget can handle different types of targets") {
    import spark.implicits._

    val df = Seq(
      (Array(1, 2, 3), Vectors.dense(1, 2, 3), Map(0 -> 1f, 1 -> 2f, 2 -> 3f), Array(0, 2))
    ) toDF("label1", "label2", "label3", "targets")

    // array of Int
    val target1 = LocalExplainer.LIME.vector
      .setTargetCol("label1")
      .extractTarget(df.schema, "targets")

    val Tuple1(v1) = df.select(target1).as[Tuple1[Vector]].head
    assert(v1 == Vectors.dense(1d, 3d))

    // vector
    val target2 = LocalExplainer.LIME.vector
      .setTargetCol("label2")
      .extractTarget(df.schema, "targets")

    val Tuple1(v2) = df.select(target2).as[Tuple1[Vector]].head
    assert(v2 == Vectors.dense(1d, 3d))

    // Map of Int -> Float
    val target3 = LocalExplainer.LIME.vector
      .setTargetCol("label3")
      .extractTarget(df.schema, "targets")

    val Tuple1(v3) = df.select(target3).as[Tuple1[Vector]].head
    assert(v3 == Vectors.dense(1d, 3d))
  }
}
