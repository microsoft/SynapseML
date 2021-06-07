// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers

import com.microsoft.ml.spark.core.test.base.TestBase
import org.apache.spark.ml.linalg.{Vectors => SVS, Vector => SV}

class HasExplainTargetSuite extends TestBase {
  test("getExplainTarget can handle different types of targets") {
    import spark.implicits._

    val df = Seq(
      (Array(1, 2, 3), SVS.dense(1, 2, 3), Map(0 -> 1f, 1 -> 2f, 2 -> 3f), Array(0, 2))
    ) toDF("label1", "label2", "label3", "targets")

    // array of Int
    val target1 = LocalExplainer.LIME.vector
      .setTargetCol("label1")
      .setTargetClasses(Array(0, 2))
      .getExplainTarget(df.schema)

    val Tuple1(v1) = df.select(target1).as[Tuple1[SV]].head
    assert(v1 == SVS.dense(1d, 3d))

    // vector
    val target2 = LocalExplainer.LIME.vector
      .setTargetCol("label2")
      .setTargetClasses(Array(0, 2))
      .getExplainTarget(df.schema)

    val Tuple1(v2) = df.select(target2).as[Tuple1[SV]].head
    assert(v2 == SVS.dense(1d, 3d))

    // Map of Int -> Float
    val target3 = LocalExplainer.LIME.vector
      .setTargetCol("label3")
      .setTargetClasses(Array(0, 2))
      .getExplainTarget(df.schema)

    val Tuple1(v3) = df.select(target3).as[Tuple1[SV]].head
    assert(v3 == SVS.dense(1d, 3d))

    // Dynamically retrieve targets
    val target4 = LocalExplainer.LIME.vector
      .setTargetCol("label2")
      .setTargetClassesCol("targets")
      .getExplainTarget(df.schema)

    val Tuple1(v4) = df.select(target4).as[Tuple1[SV]].head
    assert(v4 == SVS.dense(1d, 3d))
  }
}
