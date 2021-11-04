// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.featurize

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject, TransformerFuzzing}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vectors}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql._

trait VerifyCountSelectorShared extends TestBase {
  import spark.implicits._

  lazy val df: DataFrame = Seq(
    (Vectors.sparse(3, Seq((0, 1.0), (2, 2.0))), Vectors.dense(1.0, 0.1, 0)),
    (Vectors.sparse(3, Seq((0, 1.0), (2, 2.0))), Vectors.dense(1.0, 0.1, 0))
  ).toDF("col1", "col2")

}

class VerifyCountSelector extends EstimatorFuzzing[CountSelector] with VerifyCountSelectorShared {

  test("Basic Usage") {
    val result1 = new CountSelector().setInputCol("col1").setOutputCol("col3").fit(df).transform(df)
    assert(result1.head().getAs[SparseVector]("col3").size === 2)

    val result2 = new CountSelector().setInputCol("col2").setOutputCol("col4").fit(df).transform(df)
    assert(result2.head().getAs[DenseVector]("col4").size === 2)
  }

  override def testObjects(): List[TestObject[CountSelector]] = List(new TestObject(
    new CountSelector().setInputCol("col1").setOutputCol("col3"), df))

  override def reader: MLReadable[_] = CountSelector

  override def modelReader: MLReadable[_] = CountSelectorModel

}

class VerifyCountSelectorModel extends TransformerFuzzing[CountSelectorModel] with VerifyCountSelectorShared {

  override def testObjects(): List[TestObject[CountSelectorModel]] = List(new TestObject(
    new CountSelectorModel().setIndices(Array(0)).setInputCol("col1").setOutputCol("col3"), df))

  override def reader: MLReadable[_] = CountSelectorModel
}
