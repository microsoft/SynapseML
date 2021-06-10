// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers

import breeze.linalg.{norm, DenseVector => BDV}
import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.core.test.fuzzing.{ExperimentFuzzing, PyTestFuzzing, TestObject}
import com.microsoft.ml.spark.explainers.BreezeUtils._
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.linalg.{Vector => SV}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame

class TabularLIMESuite extends TestBase
  with ExperimentFuzzing[TabularLIME]
  with PyTestFuzzing[TabularLIME] {

  import spark.implicits._

  val data: DataFrame = Seq(
    (-6.0, 0),
    (-5.0, 0),
    (5.0, 1),
    (6.0, 1)
  ) toDF("col1", "label")

  val model: PipelineModel = {
    val vecAssembler = new VectorAssembler().setInputCols(Array("col1")).setOutputCol("features")
    val classifier = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")

    val pipeline = new Pipeline().setStages(Array(vecAssembler, classifier))
    pipeline.fit(data)
  }

  val infer: DataFrame = Seq(
    Tuple1(0.0)
  ) toDF "col1"

  val lime: TabularLIME = LocalExplainer.LIME.tabular
    .setInputCols(Array("col1"))
    .setOutputCol("weights")
    .setBackgroundDataset(data)
    .setKernelWidth(0.001)
    .setNumSamples(1000)
    .setModel(model)
    .setTargetCol("probability")
    .setTargetClasses(Array(0, 1))

  test("TabularLIME can explain a simple logistic model locally with one variable") {

    // coefficient should be around 3.61594667
    val coefficient = model.stages(1).asInstanceOf[LogisticRegressionModel].coefficients.toArray.head
    assert(math.abs(coefficient - 3.61594667) < 1e-5)

    val predicted = model.transform(infer)

    val (weights, r2) = lime.transform(predicted).select("weights", "r2").as[(Seq[SV], SV)].head
    assert(weights.size == 2)
    assert(r2.size == 2)

    val weightsBz0 = weights.head.toBreeze
    val weightsBz1 = weights(1).toBreeze

    // The derivative of the logistic function with coefficient k at x = 0, simplifies to k/4.
    // We set the kernel width to a very small value so we only consider a very close neighborhood
    // for regression, and set L1 regularization to zero so it does not affect the fit coefficient.
    // Therefore, the coefficient of the lasso regression should approximately match the derivative.
    assert(norm(weightsBz0 + BDV(coefficient / 4)) < 1e-2)
    assert(math.abs(r2(0) - 1d) < 1e-6, "R-squared of the fit should be close to 1.")

    assert(norm(weightsBz1 - BDV(coefficient / 4)) < 1e-2)
    assert(math.abs(r2(1) - 1d) < 1e-6, "R-squared of the fit should be close to 1.")
  }

  test("TabularLIME can explain a simple logistic model locally with multiple variables") {
    val data = Seq(
      (-6, 1.0, 0),
      (-5, -3.0, 0),
      (5, -1.0, 1),
      (6, 3.0, 1)
    ) toDF("col1", "col2", "label")

    val vecAssembler = new VectorAssembler().setInputCols(Array("col1", "col2")).setOutputCol("features")
    val classifier = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")

    val pipeline = new Pipeline().setStages(Array(vecAssembler, classifier))
    val model = pipeline.fit(data)

    val coefficients = model.stages(1).asInstanceOf[LogisticRegressionModel].coefficients.toArray

    assert(math.abs(coefficients(0) - 3.5279868) < 1e-5)
    assert(math.abs(coefficients(1) - 0.5962254) < 1e-5)

    val infer = Seq(
      (0.0, 0.0)
    ) toDF("col1", "col2")

    val predicted = model.transform(infer)

    val lime = LocalExplainer.LIME.tabular
      .setInputCols(Array("col1", "col2"))
      .setOutputCol("weights")
      .setBackgroundDataset(data)
      .setKernelWidth(0.05)
      .setNumSamples(1000)
      .setRegularization(0.01)
      .setModel(model)
      .setTargetCol("probability")
      .setTargetClasses(Array(1))

    val (weights, _) = lime.transform(predicted).select("weights", "r2").as[(Seq[SV], SV)].head
    assert(weights.size == 1)

    val weightsBz = weights.head

    // println(weightsBz)

    // With 0.01 L1 regularization, the second feature gets removed due to low importance.
    assert(weightsBz(0) > 0.0)
    assert(weightsBz(1) == 0.0)
  }

  test("TabularLIME can explain a model locally with categorical variables") {
    val data = Seq(
      (1, 0),
      (2, 0),
      (3, 1),
      (4, 1)
    ) toDF("col1", "label")

    val encoder = new OneHotEncoder().setInputCol("col1").setOutputCol("col1_enc")
    val classifier = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("col1_enc")

    val pipeline = new Pipeline().setStages(Array(encoder, classifier))
    val model = pipeline.fit(data)

    val infer = Seq(
      Tuple1(1),
      Tuple1(2),
      Tuple1(3),
      Tuple1(4)
    ) toDF "col1"

    val predicted = model.transform(infer)

    val lime = LocalExplainer.LIME.tabular
      .setInputCols(Array("col1"))
      .setCategoricalFeatures(Array("col1"))
      .setOutputCol("weights")
      .setBackgroundDataset(data)
      .setNumSamples(1000)
      .setModel(model)
      .setTargetCol("probability")
      .setTargetClasses(Array(1))

    val weights = lime.transform(predicted)

    // weights.show(false)

    val results = weights.select("col1", "weights").as[(Int, Seq[SV])].collect()
      .map(r => (r._1, r._2.head.toBreeze(0)))
      .toMap

    // Assuming local linear behavior:
    // col1 == 1 reduces the model output by more than 60% compared to model not knowing about col1
    assert(results(1) < -0.6)
    // col1 == 2 reduces the model output by more than 60% compared to model not knowing about col1
    assert(results(2) < -0.6)
    // col1 == 3 increases the model output by more than 60% compared to model not knowing about col1
    assert(results(3) > 0.6)
    // col1 == 4 increases the model output by more than 60% compared to model not knowing about col1
    assert(results(3) > 0.6)
  }

  private lazy val testObjects = Seq(new TestObject(lime, infer))

  override def experimentTestObjects(): Seq[TestObject[TabularLIME]] = testObjects

  override def pyTestObjects(): Seq[TestObject[TabularLIME]] = testObjects
}
