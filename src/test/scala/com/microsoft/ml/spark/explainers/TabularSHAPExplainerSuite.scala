// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers

import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.core.test.fuzzing.{ExperimentFuzzing, PyTestFuzzing, TestObject}
import org.apache.spark.ml.linalg.{Vector => SV}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.avg
import com.microsoft.ml.spark.explainers.BreezeUtils._

class TabularSHAPExplainerSuite extends TestBase
  // Excluding SerializationFuzzing here due to error caused by randomness in explanation after deserialization.
  with ExperimentFuzzing[TabularSHAP]
  with PyTestFuzzing[TabularSHAP] {

  import spark.implicits._

  val data: DataFrame = (1 to 100).flatMap(_ => Seq(
    (-5d, "a", -5d, 0),
    (-5d, "b", -5d, 0),
    (5d, "a", 5d, 1),
    (5d, "b", 5d, 1)
  )).toDF("col1", "col2", "col3", "label")

  val pipeline: Pipeline = new Pipeline().setStages(Array(
    new StringIndexer().setInputCol("col2").setOutputCol("col2_ind"),
    new OneHotEncoder().setInputCol("col2_ind").setOutputCol("col2_enc"),
    new VectorAssembler().setInputCols(Array("col1", "col2_enc", "col3")).setOutputCol("features"),
    new LogisticRegression().setLabelCol("label").setFeaturesCol("features")
  ))

  val model: PipelineModel = pipeline.fit(data)

  val kernelShap: TabularSHAP = LocalExplainer.KernelSHAP.tabular
    .setInputCols(Array("col1", "col2", "col3"))
    .setOutputCol("shapValues")
    .setBackgroundData(data)
    .setNumSamples(1000)
    .setModel(model)
    .setTargetCol("probability")
    .setTargetClasses(Array(1))

  val infer: DataFrame = Seq(
    (3d, "a", 3d)
  ) toDF("col1", "col2", "col3")

  test("TabularKernelSHAP can explain a model locally") {
    val coefficients = model.stages.last.asInstanceOf[LogisticRegressionModel].coefficients.toBreeze

    assert(math.abs(coefficients(0) - 1.9099146508533622) < 1e-5)
    assert(math.abs(coefficients(1)) < 1e-5)
    assert(math.abs(coefficients(2) - 1.9099146508533622) < 1e-5)

    val predicted = model.transform(infer)

    val (probability, shapValues, r2) = kernelShap
      .transform(predicted)
      .select("probability", "shapValues", "r2").as[(SV, Seq[SV], SV)]
      .head

    val shapBz = shapValues.head.toBreeze
    val avgLabel = model.transform(data).select(avg("prediction")).as[Double].head

    // Base value (weightsBz(0)) should match average label from background data set.
    assert(math.abs(shapBz(0) - avgLabel) < 1E-5)

    // Sum of shap values should match prediction
    assert(math.abs(probability(1) - breeze.linalg.sum(shapBz)) < 1E-5)

    // Null feature (col2) should have zero shap values
    assert(math.abs(shapBz(2)) < 1E-5)

    // col1 and col3 are symmetric so they should have same shap values.
    assert(math.abs(shapBz(1) - shapBz(3)) < 1E-5)

    // R-squared of the underlying regression should be close to 1.
    assert(math.abs(r2(0) - 1d) < 1E-5)
  }

  private lazy val testObjects: Seq[TestObject[TabularSHAP]] = Seq(new TestObject(kernelShap, infer))

  override def experimentTestObjects(): Seq[TestObject[TabularSHAP]] = testObjects

  override def pyTestObjects(): Seq[TestObject[TabularSHAP]] = testObjects
}
