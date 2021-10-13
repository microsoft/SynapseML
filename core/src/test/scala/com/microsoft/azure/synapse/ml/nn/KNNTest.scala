// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nn

import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row}
import org.scalactic.Equality

class KNNTest extends EstimatorFuzzing[KNN] with BallTreeTestBase {

  test("matches non spark result") {
    val results = new KNN().setOutputCol("matches")
      .fit(df).transform(testDF)
      .select("matches").collect()
    val resultsSparse = new KNN().setOutputCol("matches")
      .fit(dfSparse).transform(testDFSparse)
      .select("matches").collect()
    val sparkResults = List(results, resultsSparse).map(_.map(r =>
      r.getSeq[Row](0).map(mr => mr.getDouble(1))
    ))
    val tree = BallTree(uniformData, uniformData.indices)
    val nonSparkResults = uniformData.take(5).map(
      point => tree.findMaximumInnerProducts(point, 5)
    )
    sparkResults.map(_.zip(nonSparkResults).foreach { case (sr, nsr) =>
      assert(sr === nsr.map(_.distance))
    })
  }

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    super.assertDFEq(
      df1.select("features", "values", "matches.distance"),
      df2.select("features", "values", "matches.distance")
    )(eq)
  }

  override def testObjects(): Seq[TestObject[KNN]] =
    List(
      new TestObject(new KNN().setOutputCol("matches"), df, testDF),
      new TestObject(new KNN().setOutputCol("matches"), dfSparse, testDFSparse))

  override def reader: MLReadable[_] = KNN

  override def modelReader: MLReadable[_] = KNNModel
}

class ConditionalKNNTest extends EstimatorFuzzing[ConditionalKNN] with BallTreeTestBase {

  test("matches non spark result") {
    val results = new ConditionalKNN().setOutputCol("matches")
      .fit(df).transform(testDF)
      .select("matches").collect()
    val resultsSparse = new ConditionalKNN().setOutputCol("matches")
      .fit(dfSparse).transform(testDFSparse)
      .select("matches").collect()

    val sparkResults = List(results, resultsSparse).map(_.map(r =>
      r.getSeq[Row](0).map(mr => (mr.getDouble(1), mr.getInt(2)))
    ))
    val tree = ConditionalBallTree(uniformData, uniformData.indices, uniformLabels)
    val nonSparkResults = uniformData.take(5).map(
      point => tree.findMaximumInnerProducts(point, Set(0, 1), 5)
    )

    sparkResults.map(_.zip(nonSparkResults).foreach { case (sr, nsr) =>
      assert(sr.map(_._1) === nsr.map(_.distance))
      assert(sr.forall(p => Set(1, 0)(p._2)))
    })
  }

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    super.assertDFEq(
      df1.select("features", "values", "matches.distance"),
      df2.select("features", "values", "matches.distance")
    )(eq)
  }

  override def testObjects(): Seq[TestObject[ConditionalKNN]] =
    List(
      new TestObject(new ConditionalKNN().setOutputCol("matches"), df, testDF),
      new TestObject(new ConditionalKNN().setOutputCol("matches"), dfSparse, testDFSparse))

  override def reader: MLReadable[_] = ConditionalKNN

  override def modelReader: MLReadable[_] = ConditionalKNNModel
}
