// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.DenseVector

class EnsembleByKeySuite extends TestBase {

  test("Should work on Dataframes doubles or vectors") {
    val scoreDF = session.createDataFrame(Seq(
      (0, "foo", 1.0, .1),
      (1, "bar", 4.0, -2.0),
      (1, "bar", 0.0, -3.0)))
      .toDF("label1", "label2", "score1", "score2")

    val va = new VectorAssembler().setInputCols(Array("score1", "score2")).setOutputCol("v1")
    val scoreDF2 = va.transform(scoreDF)

    val t = new EnsembleByKey().setKey("label1").setCol("score1")
    val df1 = t.transform(scoreDF2)
    df1.printSchema()
    assert(df1.collect().map(r => (r.getInt(0), r.getDouble(1))).toSet === Set((1, 2.0), (0, 1.0)))

    val t2 = new EnsembleByKey().setKeys("label1", "label2").setCols("score1", "score2", "v1")
    val df2 = t2.transform(scoreDF2)
    val res2 = df2.select("avg(score1)", "avg(v1)").collect().map(r => (r.getDouble(0), r.getAs[DenseVector](1)))
    val true2 = Set(
      (2.0, new DenseVector(Array(2.0, -2.5))),
      (1.0, new DenseVector(Array(1.0, 0.1))))
    assert(res2.toSet === true2)
  }

  test("should support collapsing or not") {
    val scoreDF = session.createDataFrame(Seq(
      (0, "foo", 1.0, .1),
      (1, "bar", 4.0, -2.0),
      (1, "bar", 0.0, -3.0)))
      .toDF("label1", "label2", "score1", "score2")

    val va = new VectorAssembler().setInputCols(Array("score1", "score2")).setOutputCol("v1")
    val scoreDF2 = va.transform(scoreDF)

    val t = new EnsembleByKey().setKey("label1").setCol("score1").setCollapseGroup(false)
    val df1 = t.transform(scoreDF2)

    assert(df1.collect().map(r => (r.getInt(0), r.getDouble(5))).toSet === Set((1, 2.0), (0, 1.0)))
    assert(df1.count() == scoreDF.count())
    df1.show()
  }

}
