// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.ml.util.MLReadable

class VerifyVowpalWabbitInteractions extends TestBase with TransformerFuzzing[VowpalWabbitInteractions] {

  case class Data(val v1: Vector, val v2: Vector, val v3: Vector)

  val df = session.createDataFrame(Seq(Data(
    Vectors.dense(Array(1.0, 2.0, 3.0)),
    Vectors.sparse(8, Array(5), Array(4.0)),
    Vectors.sparse(11, Array(8, 9), Array(7.0, 8.0))
  )))

  private def verifyValues(actual: SparseVector, expected: Array[Double]) = {
    assert(actual.numNonzeros == expected.length)

    (actual.values.sorted zip expected.sorted).forall{ case (x,y) => x == y }
  }

  test("Verify VowpalWabbit Interactions 3-dense x 1-sparse") {
    val interactions = new VowpalWabbitInteractions()
      .setInputCols(Array("v1", "v2"))
      .setOutputCol("features")

    val v = interactions.transform(df).head().getAs[SparseVector]("features")

    verifyValues(v, Array(4.0, 8, 12.0))
  }

  test("Verify VowpalWabbit Interactions 1-sparse x 2-sparse") {
    val interactions = new VowpalWabbitInteractions()
      .setInputCols(Array("v2", "v3"))
      .setOutputCol("features")

    val v = interactions.transform(df).head().getAs[SparseVector]("features")

    verifyValues(v, Array(28.0, 32.0))
  }

  test("Verify VowpalWabbit Interactions 3-dense x 1-sparse x 2-sparse") {
    val interactions = new VowpalWabbitInteractions()
      .setInputCols(Array("v1", "v2", "v3"))
      .setOutputCol("features")

    val v = interactions.transform(df).head().getAs[SparseVector]("features")

    verifyValues(v, Array(
      1.0 * 5 * 7, 1 * 5 * 8.0,
      2.0 * 5 * 7, 2 * 5 * 8.0,
      3.0 * 5 * 7, 3 * 5 * 8.0
    ))
  }

  def testObjects(): Seq[TestObject[VowpalWabbitInteractions]] = List(new TestObject(
    new VowpalWabbitInteractions().setInputCols(Array("words")).setOutputCol("out"), makeBasicDF()))

  override def reader: MLReadable[_] = VowpalWabbitInteractions
}
