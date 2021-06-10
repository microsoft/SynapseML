// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers

import breeze.linalg.{*, norm, DenseMatrix => BDM}
import breeze.stats.distributions.Rand
import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.core.test.fuzzing.{ExperimentFuzzing, PyTestFuzzing, TestObject}
import com.microsoft.ml.spark.explainers.BreezeUtils._
import org.apache.spark.ml.linalg.{Vector => SV, Vectors => SVS}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.DataFrame

class VectorLIMEExplainerSuite extends TestBase
  with ExperimentFuzzing[VectorLIME]
  with PyTestFuzzing[VectorLIME] {

  import spark.implicits._

  val d1 = 3
  val d2 = 1

  val coefficients: BDM[Double] = new BDM(d1, d2, Array(1.0, -1.0, 2.0))

  val df: DataFrame = {
    val nRows = 100
    val intercept: Double = math.random()

    val x: BDM[Double] = BDM.rand(nRows, d1, Rand.gaussian)
    val y = x * coefficients + intercept

    val xRows = x(*, ::).iterator.toSeq.map(dv => SVS.dense(dv.toArray))
    val yRows = y(*, ::).iterator.toSeq.map(dv => dv(0))
    xRows.zip(yRows).toDF("features", "label")
  }

  val model: LinearRegressionModel = new LinearRegression().fit(df)

  val lime: VectorLIME = LocalExplainer.LIME.vector
    .setModel(model)
    .setInputCol("features")
    .setTargetCol(model.getPredictionCol)
    .setOutputCol("weights")
    .setNumSamples(1000)

  test("VectorLIME can explain a model locally") {

    val predicted = model.transform(df)
    val weights = lime.transform(predicted).select("weights", "r2").as[(Seq[SV], SV)].collect().map {
      case (m, _) => m.head.toBreeze
    }

    val weightsMatrix = BDM(weights: _*)
    weightsMatrix(*, ::).foreach {
      row =>
        assert(norm(row - coefficients(::, 0)) < 1e-6)
    }
  }

  private lazy val testObjects = Seq(new TestObject(lime, df))

  override def experimentTestObjects(): Seq[TestObject[VectorLIME]] = testObjects

  override def pyTestObjects(): Seq[TestObject[VectorLIME]] = testObjects
}
