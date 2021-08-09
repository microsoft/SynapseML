// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers.split1

import breeze.linalg.{*, DenseMatrix => BDM, DenseVector => BDV}
import breeze.stats.distributions.Rand
import com.microsoft.ml.spark.core.test.base.{Flaky, TestBase}
import com.microsoft.ml.spark.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.ml.spark.explainers.BreezeUtils._
import com.microsoft.ml.spark.explainers.{LocalExplainer, VectorLIME}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.scalactic.Equality

class VectorLIMEExplainerSuite extends TestBase with Flaky
  with TransformerFuzzing[VectorLIME] {

  import spark.implicits._

  implicit val vectorEquality: Equality[BDV[Double]] = breezeVectorEq(1E-6)

  override val sortInDataframeEquality = true

  val d1 = 3
  val d2 = 1

  val coefficients: BDM[Double] = new BDM(d1, d2, Array(1.0, -1.0, 2.0))

  val df: DataFrame = {
    val nRows = 100
    val intercept: Double = math.random()

    val x: BDM[Double] = BDM.rand(nRows, d1, Rand.gaussian)
    val y = x * coefficients + intercept

    val xRows = x(*, ::).iterator.toSeq.map(dv => Vectors.dense(dv.toArray))
    val yRows = y(*, ::).iterator.toSeq.map(dv => dv(0))
    xRows.zip(yRows).toDF("features", "label")
  }

  val model: LinearRegressionModel = new LinearRegression().fit(df)

  val lime: VectorLIME = LocalExplainer.LIME.vector
    .setModel(model)
    .setBackgroundData(df)
    .setInputCol("features")
    .setTargetCol(model.getPredictionCol)
    .setOutputCol("weights")
    .setNumSamples(1000)

  test("VectorLIME can explain a model locally") {

    val predicted = model.transform(df)
    val weights = lime
      .transform(predicted)
      .select("weights", "r2")
      .as[(Seq[Vector], Vector)]
      .collect()
      .map {
        case (m, _) => m.head.toBreeze
      }

    val weightsMatrix = BDM(weights: _*)
    weightsMatrix(*, ::).foreach {
      row =>
        assert(row === coefficients(::, 0))
    }
  }

  override def testObjects(): Seq[TestObject[VectorLIME]] = Seq(new TestObject(lime, df))

  override def reader: MLReadable[_] = VectorLIME
}
