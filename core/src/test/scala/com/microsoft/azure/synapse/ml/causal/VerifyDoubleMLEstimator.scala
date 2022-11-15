// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.util.MLReadable

class VerifyDoubleMLEstimator extends EstimatorFuzzing[DoubleMLEstimator] {

  val mockLabelColumn = "Label"

  val cat = "Cat"
  val dog = "Dog"
  val bird = "Bird"
  private lazy val mockDataset = spark.createDataFrame(Seq(
    (0, 1, 0.50, 0.60, dog, cat),
    (1, 0, 0.40, 0.50, cat, dog),
    (0, 1, 0.78, 0.99, dog, bird),
    (1, 0, 0.12, 0.34, cat, dog),
    (0, 1, 0.50, 0.60, dog, bird),
    (1, 0, 0.40, 0.50, bird, dog),
    (0, 1, 0.78, 0.99, dog, cat),
    (1, 1, 0.12, 0.34, cat, dog),
    (0, 0, 0.50, 0.60, dog, cat),
    (1, 1, 0.40, 0.50, bird, dog),
    (0, 0, 0.78, 0.99, dog, bird),
    (1, 1, 0.12, 0.34, cat, dog),
    (0, 1, 0.55, 0.69, dog, bird),
    (1, 0, 0.38, 0.40, bird, dog),
    (0, 1, 0.69, 0.88, dog, cat),
    (1, 1, 0.16, 0.35, cat, dog),
    (0, 0, 0.48, 0.58, dog, cat)))
    .toDF(mockLabelColumn, "col1", "col2", "col3", "col4", "col5")


  test("Get treatment effects") {
    val ldml = new DoubleMLEstimator()
      .setTreatmentModel(new LogisticRegression())
      .setTreatmentCol(mockLabelColumn)
      .setOutcomeModel(new LinearRegression())
      .setOutcomeCol("col2")

    var ldmlModel = ldml.fit(mockDataset)
    assert(ldmlModel.getAvgTreatmentEffect != 0.0)
    assert(ldmlModel.getConfidenceInterval.length == 2)
  }

  test("Get treatment effects with weight column") {
    val ldml = new DoubleMLEstimator()
      .setTreatmentModel(new LogisticRegression())
      .setTreatmentCol(mockLabelColumn)
      .setOutcomeModel(new LogisticRegression())
      .setOutcomeCol("col1")
      .setWeightCol("col3")

    var ldmlModel = ldml.fit(mockDataset)
    assert(ldmlModel.getAvgTreatmentEffect != 0.0)
  }

  test("Get treatment effects and confidence intervals") {
    val ldml = new DoubleMLEstimator()
      .setTreatmentModel(new LogisticRegression())
      .setTreatmentCol(mockLabelColumn)
      .setOutcomeModel(new LinearRegression())
      .setOutcomeCol("col2")
      .setMaxIter(10)

    var ldmlModel = ldml.fit(mockDataset)
    assert(ldmlModel.getConfidenceInterval.length == 2
      && ldmlModel.getConfidenceInterval(0) != ldmlModel.getConfidenceInterval(1))
  }


  override def testObjects(): Seq[TestObject[DoubleMLEstimator]] =
    Seq(new TestObject(new DoubleMLEstimator()
      .setTreatmentModel(new LogisticRegression())
      .setTreatmentCol(mockLabelColumn)
      .setOutcomeModel(new LinearRegression())
      .setOutcomeCol("col2"),
    mockDataset))

  override def reader: MLReadable[_] = DoubleMLEstimator

  override def modelReader: MLReadable[_] = DoubleMLModel
}
