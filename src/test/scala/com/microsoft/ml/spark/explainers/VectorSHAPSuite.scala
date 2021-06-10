// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers

import breeze.linalg.{*, DenseVector => BDV, DenseMatrix => BDM}
import breeze.stats.distributions.RandBasis
import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.core.test.fuzzing.{ExperimentFuzzing, PyTestFuzzing, TestObject}
import org.apache.spark.ml.linalg.{Vector => SV, Vectors => SVS}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.avg
import com.microsoft.ml.spark.explainers.BreezeUtils._

class VectorSHAPSuite extends TestBase
  with ExperimentFuzzing[VectorSHAP]
  with PyTestFuzzing[VectorSHAP] {

  import spark.implicits._

  private val randBasis = RandBasis.withSeed(123)
  private val m: BDM[Double] = BDM.rand[Double](100, 5, randBasis.gaussian)
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
    Tuple1(SVS.dense(1d, 1d, 1d, 1d, 1d))
  ) toDF "features"

  val kernelShap: VectorSHAP = LocalExplainer.KernelSHAP.vector
    .setInputCol("features")
    .setOutputCol("shapValues")
    .setBackgroundDataset(data)
    .setNumSamples(1000)
    .setModel(model)
    .setTargetCol("probability")
    .setTargetClasses(Array(1))

  test("VectorKernelSHAP can explain a model locally") {
    val predicted = model.transform(infer)
    val (probability, shapValues, r2) = kernelShap
      .transform(predicted)
      .select("probability", "shapValues", "r2").as[(SV, Seq[SV], SV)]
      .head

    // println((probability, shapValues, r2))

    val shapBz = shapValues.head.toBreeze
    val avgLabel = model.transform(data).select(avg("prediction")).as[Double].head

    // Base value (weightsBz(0)) should match average label from background data set.
    assert(math.abs(shapBz(0) - avgLabel) < 1E-5)

    // Sum of shap values should match prediction
    assert(math.abs(probability(1) - breeze.linalg.sum(shapBz)) < 1E-5)

    // Null feature (col2) should have zero shap values
    assert(math.abs(shapBz(1)) < 1E-2)
    assert(math.abs(shapBz(2)) < 1E-2)
    assert(math.abs(shapBz(5)) < 1E-2)

    // R-squared of the underlying regression should be close to 1.
    assert(math.abs(r2(0) - 1d) < 1E-5)
  }

  private val testObjects = Seq(new TestObject(kernelShap, infer))

  override def experimentTestObjects(): Seq[TestObject[VectorSHAP]] = testObjects

  override def pyTestObjects(): Seq[TestObject[VectorSHAP]] = testObjects
}
