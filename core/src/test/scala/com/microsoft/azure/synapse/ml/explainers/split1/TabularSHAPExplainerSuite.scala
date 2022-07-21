// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers.split1

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.core.utils.BreezeUtils._
import com.microsoft.azure.synapse.ml.explainers.LocalExplainer.KernelSHAP
import com.microsoft.azure.synapse.ml.explainers.TabularSHAP
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.avg
import org.scalactic.{Equality, TolerantNumerics}

class TabularSHAPExplainerSuite extends TestBase
  with TransformerFuzzing[TabularSHAP] {

  import spark.implicits._

  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1E-5)

  override val sortInDataframeEquality = true

  lazy val data: DataFrame = (1 to 100).flatMap(_ => Seq(
    (-5d, "a", -5d, 0),
    (-5d, "b", -5d, 0),
    (5d, "a", 5d, 1),
    (5d, "b", 5d, 1)
  )).toDF("col1", "col2", "col3", "label")

  lazy val pipeline: Pipeline = new Pipeline().setStages(Array(
    new StringIndexer().setInputCol("col2").setOutputCol("col2_ind"),
    new OneHotEncoder().setInputCol("col2_ind").setOutputCol("col2_enc"),
    new VectorAssembler().setInputCols(Array("col1", "col2_enc", "col3")).setOutputCol("features"),
    new LogisticRegression().setLabelCol("label").setFeaturesCol("features")
  ))

  lazy val model: PipelineModel = pipeline.fit(data)

  lazy val kernelShap: TabularSHAP = KernelSHAP.tabular
    .setInputCols(Array("col1", "col2", "col3"))
    .setOutputCol("shapValues")
    .setBackgroundData(data)
    .setNumSamples(1000)
    .setModel(model)
    .setTargetCol("probability")
    .setTargetClasses(Array(1))

  lazy val infer: DataFrame = Seq(
    (3d, "a", 3d)
  ) toDF("col1", "col2", "col3")

  test("TabularKernelSHAP can explain a model locally") {
    val coefficients = model.stages.last.asInstanceOf[LogisticRegressionModel].coefficients.toBreeze

    assert(coefficients(0) === 1.9099146508533622)
    assert(coefficients(1) === 0d)
    assert(coefficients(2) === 1.9099146508533622)

    val predicted = model.transform(infer)

    val (probability, shapValues, r2) = kernelShap
      .transform(predicted)
      .select("probability", "shapValues", "r2").as[(Vector, Seq[Vector], Vector)]
      .head

    val shapBz = shapValues.head.toBreeze
    val avgLabel = model.transform(data).select(avg("prediction")).as[Double].head

    // Base value (weightsBz(0)) should match average label from background data set.
    assert(shapBz(0) === avgLabel)

    // Sum of shap values should match prediction
    assert(probability(1) === breeze.linalg.sum(shapBz))

    // Null feature (col2) should have zero shap values
    assert(Math.abs(shapBz(2)) < 1E-5)

    // col1 and col3 are symmetric so they should have same shap values.
    assert(shapBz(1) === shapBz(3))

    // R-squared of the underlying regression should be close to 1.
    assert(r2(0) === 1d)
  }

  override def testObjects(): Seq[TestObject[TabularSHAP]] = Seq(new TestObject(kernelShap, infer))

  override def reader: MLReadable[_] = TabularSHAP
}
