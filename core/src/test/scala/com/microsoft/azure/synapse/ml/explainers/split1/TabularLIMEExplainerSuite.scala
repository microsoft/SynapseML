// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers.split1

import breeze.linalg.{DenseVector => BDV}
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.core.utils.BreezeUtils._
import com.microsoft.azure.synapse.ml.explainers.LocalExplainer.LIME
import com.microsoft.azure.synapse.ml.explainers.{LocalExplainer, TabularLIME}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame
import org.scalactic.{Equality, TolerantNumerics}

class TabularLIMEExplainerSuite extends TestBase
  with TransformerFuzzing[TabularLIME] {

  import spark.implicits._
  implicit val vectorEquality: Equality[BDV[Double]] = breezeVectorEq(1E-2)
  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1E-5)

  override val sortInDataframeEquality = true

  lazy val data: DataFrame = Seq(
    (-6.0, 0),
    (-5.0, 0),
    (5.0, 1),
    (6.0, 1)
  ) toDF("col1", "label")

  lazy val model: PipelineModel = {
    val vecAssembler = new VectorAssembler().setInputCols(Array("col1")).setOutputCol("features")
    val classifier = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")

    val pipeline = new Pipeline().setStages(Array(vecAssembler, classifier))
    pipeline.fit(data)
  }

  lazy val infer: DataFrame = Seq(
    Tuple1(0.0)
  ) toDF "col1"

  lazy val lime: TabularLIME = LIME.tabular
    .setInputCols(Array("col1"))
    .setOutputCol("weights")
    .setBackgroundData(data)
    .setKernelWidth(0.001)
    .setNumSamples(1000)
    .setModel(model)
    .setTargetCol("probability")
    .setTargetClasses(Array(0, 1))

  test("TabularLIME can explain a simple logistic model locally with one variable") {

    // coefficient should be around 3.61594667
    val coefficient = model.stages(1).asInstanceOf[LogisticRegressionModel].coefficients.toArray.head
    assert(coefficient === 3.61594667)

    val predicted = model.transform(infer)

    val (weights, r2) = lime
      .transform(predicted)
      .select("weights", "r2")
      .as[(Seq[Vector], Vector)]
      .head

    assert(weights.size == 2)
    assert(r2.size == 2)

    val weightsBz0 = weights.head.toBreeze
    val weightsBz1 = weights(1).toBreeze

    // The derivative of the logistic function with coefficient k at x = 0, simplifies to k/4.
    // We set the kernel width to a very small value so we only consider a very close neighborhood
    // for regression, and set L1 regularization to zero so it does not affect the fit coefficient.
    // Therefore, the coefficient of the lasso regression should approximately match the derivative.
    assert(weightsBz0 === BDV(-coefficient / 4))
    assert(r2(0) === 1d, "R-squared of the fit should be close to 1.")

    assert(weightsBz1 === BDV(coefficient / 4))
    assert(r2(1) === 1d, "R-squared of the fit should be close to 1.")
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

    assert(coefficients(0) === 3.5279868)
    assert(coefficients(1) === 0.5962254)

    val infer = Seq(
      (0.0, 0.0)
    ) toDF("col1", "col2")

    val predicted = model.transform(infer)

    val lime = LocalExplainer.LIME.tabular
      .setInputCols(Array("col1", "col2"))
      .setOutputCol("weights")
      .setBackgroundData(data)
      .setKernelWidth(0.05)
      .setNumSamples(1000)
      .setRegularization(0.01)
      .setModel(model)
      .setTargetCol("probability")
      .setTargetClasses(Array(1))

    val (weights, _) = lime.transform(predicted).select("weights", "r2").as[(Seq[Vector], Vector)].head
    assert(weights.size == 1)

    val weightsBz = weights.head

    // println(weightsBz)

    // With 0.01 L1 regularization, the second feature gets removed due to low importance.
    assert(weightsBz(0) > 0.0)
    assert(weightsBz(1) == 0.0)
  }

  test("TabularLIME can explain a model locally with categorical variables") {
    val data = Seq(
      ("US", "M", 0),
      ("US", "F", 0),
      ("GB", "M", 1),
      ("GB", "F", 1)
    ) toDF("col1", "col2", "label")

    val indexer = new StringIndexer().setInputCols(Array("col1", "col2")).setOutputCols(Array("col1_idx", "col2_idx"))
    val encoder = new OneHotEncoder()
      .setInputCols(Array("col1_idx", "col2_idx"))
      .setOutputCols(Array("col1_enc", "col2_enc"))
    val assembler = new VectorAssembler().setInputCols(Array("col1_enc", "col2_enc")).setOutputCol("features")
    val classifier = new LogisticRegression().setLabelCol("label").setFeaturesCol("features")

    val pipeline = new Pipeline().setStages(Array(indexer, encoder, assembler, classifier))
    val model = pipeline.fit(data)

    val infer = Seq(
      ("GB", "M"),
      ("US", "F")
    ) toDF ("col1", "col2")

    val lime = LocalExplainer.LIME.tabular
      .setInputCols(Array("col1", "col2"))
      .setCategoricalFeatures(Array("col1", "col2"))
      .setOutputCol("weights")
      .setBackgroundData(data)
      .setNumSamples(1000)
      .setModel(model)
      .setTargetCol("probability")
      .setTargetClasses(Array(1))

    val weights = lime.transform(infer)

    val results = weights.select("col1", "col2", "weights").as[(String, String, Seq[Vector])].collect()

    val col1Results = results.map(r => (r._1, r._3.head.toBreeze(0))).toMap
    val col2Results = results.map(r => (r._2, r._3.head.toBreeze(1))).toMap

    assert(col1Results("GB") === 1.0)
    assert(col1Results("US") === -1.0)
    assert(col2Results("M") === 0.0)
    assert(col2Results("F") === 0.0)
  }


  override def testObjects(): Seq[TestObject[TabularLIME]] = Seq(new TestObject(lime, infer))

  override def reader: MLReadable[_] = TabularLIME
}
