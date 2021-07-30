// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lime

import breeze.linalg.{*, DenseMatrix}
import breeze.stats.distributions.Rand
import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.core.test.fuzzing.{EstimatorFuzzing, TestObject, TransformerFuzzing}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.param.DataFrameEquality
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.util.MLReadable

@deprecated("Please use 'com.microsoft.ml.spark.explainers.VectorLIME'.", since="1.0.0-RC3")
trait LimeTestBase extends TestBase {

  import spark.implicits._

  lazy val nRows = 100
  lazy val d1 = 3
  lazy val d2 = 1

  lazy val m: DenseMatrix[Double] = new DenseMatrix(d1, d2, Array(1.0, -1.0, 2.0))
  lazy val x: DenseMatrix[Double] = DenseMatrix.rand(nRows, d1, Rand.gaussian)
  lazy val noise: DenseMatrix[Double] = DenseMatrix.rand(nRows, d2, Rand.gaussian) * 0.1
  lazy val y = x * m //+ noise

  lazy val xRows = x(*, ::).iterator.toSeq.map(dv => new DenseVector(dv.toArray))
  lazy val yRows = y(*, ::).iterator.toSeq.map(dv => dv(0))
  lazy val df = xRows.zip(yRows).toDF("features", "label")

  lazy val model = new LinearRegression().fit(df)

  lazy val lime = new TabularLIME()
    .setModel(model)
    .setInputCol("features")
    .setPredictionCol(model.getPredictionCol)
    .setOutputCol("out")
    .setNSamples(1000)

  lazy val limeModel = lime.fit(df)
}

@deprecated("Please use 'com.microsoft.ml.spark.explainers.TabularLIME'.", since="1.0.0-RC3")
class TabularLIMESuite extends EstimatorFuzzing[TabularLIME] with
  DataFrameEquality with LimeTestBase {

  test("text lime usage test check") {
    val results = limeModel.transform(df).select("out")
      .collect().map(_.getAs[DenseVector](0))
    results.foreach(result => assert(result === new DenseVector(m.data)))
  }

  override def testObjects(): Seq[TestObject[TabularLIME]] = Seq(new TestObject(lime, df))

  override def reader: MLReadable[_] = TabularLIME

  override def modelReader: MLReadable[_] = TabularLIMEModel
}

@deprecated("Please use 'com.microsoft.ml.spark.explainers.TextLIME'.", since="1.0.0-RC3")
class TabularLIMEModelSuite extends TransformerFuzzing[TabularLIMEModel] with
  DataFrameEquality with LimeTestBase {

  override def testObjects(): Seq[TestObject[TabularLIMEModel]] = Seq(new TestObject(limeModel, df))

  override def reader: MLReadable[_] = TabularLIMEModel
}
