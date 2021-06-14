// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers

import breeze.linalg.{*, DenseMatrix => BDM, DenseVector => BDV}
import breeze.stats.distributions.RandBasis
import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.ml.spark.explainers.BreezeUtils._
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.avg
import org.scalactic.{Equality, TolerantNumerics}

class VectorSHAPExplainerSuite extends TestBase
  with TransformerFuzzing[VectorSHAP] {

  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1E-3)

  override val sortInDataframeEquality = true

  import spark.implicits._

  private val randBasis = RandBasis.withSeed(123)
  private val m: BDM[Double] = BDM.rand[Double](1000, 5, randBasis.gaussian)
  private val l: BDV[Double] = m(*, ::).map {
    row =>
      if (row(2) + row(3) > 0.5) 1d else 0d
  }

  val data: DataFrame = m(*, ::).iterator.zip(l.valuesIterator).map {
    case (f, l) => (f.toSpark, l)
  }.toSeq.toDF("features", "label")

  val model: LogisticRegressionModel = new LogisticRegression()
    .setFeaturesCol("features")
    .setLabelCol("label")
    .fit(data)

  // println(model.coefficients)

  val infer: DataFrame = Seq(
    Tuple1(Vectors.dense(1d, 1d, 1d, 1d, 1d))
  ) toDF "features"

  val kernelShap: VectorSHAP = LocalExplainer.KernelSHAP.vector
    .setInputCol("features")
    .setOutputCol("shapValues")
    .setBackgroundData(data)
    .setNumSamples(1000)
    .setModel(model)
    .setTargetCol("probability")
    .setTargetClasses(Array(1))

  test("VectorKernelSHAP can explain a model locally") {
    val predicted = model.transform(infer)
    val (probability, shapValues, r2) = kernelShap
      .transform(predicted)
      .select("probability", "shapValues", "r2").as[(Vector, Seq[Vector], Vector)]
      .head

    // println((probability, shapValues, r2))

    val shapBz = shapValues.head.toBreeze
    val avgLabel = model.transform(data).select(avg("prediction")).as[Double].head

    // Base value (weightsBz(0)) should match average label from background data set.
    assert(shapBz(0) === avgLabel)

    // Sum of shap values should match prediction
    assert(probability(1) === breeze.linalg.sum(shapBz))

    // Null feature (col2) should have zero shap values
    assert(shapBz(1) === 0d)
    assert(shapBz(2) === 0d)
    assert(shapBz(5) === 0d)

    // R-squared of the underlying regression should be close to 1.
    assert(r2(0) === 1d)
  }

  override def testObjects(): Seq[TestObject[VectorSHAP]] = Seq(new TestObject(kernelShap, infer))

  override def reader: MLReadable[_] = VectorSHAP
}
