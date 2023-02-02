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
    (0, 1, 50, 0.60, dog, cat),
    (1, 0, 140, 0.50, cat, dog),
    (0, 1, 78, 0.99, dog, bird),
    (1, 0, 92, 0.34, cat, dog),
    (0, 1, 50, 0.60, dog, bird),
    (1, 0, 80, 0.50, bird, dog),
    (0, 1, 78, 0.99, dog, cat),
    (1, 0, 101, 0.28, cat, bird),
    (0, 1, 40, 0.58, dog, cat),
    (0, 1, 42, 0.53, cat, dog),
    (1, 0, 128, 0.99, dog, bird),
    (0, 1, 12, 0.34, cat, dog),
    (1, 1, 55, 0.69, cat, bird),
    (0, 1, 32, 0.48, cat, dog),
    (1, 1, 62, 0.78, dog, bird),
    (0, 1, 19, 0.48, cat, bird),
    (0, 1, 11, 0.32, cat, bird),
    (1, 1, 43, 0.63, dog, cat),
    (1, 1, 138, 0.73, cat, dog),
    (1, 0, 98, 0.89, dog, bird),
    (0, 1, 22, 0.39, cat, dog),
    (0, 0, 47, 0.72, cat, bird),
    (1, 0, 95, 0.49, cat, dog),
    (0, 1, 66, 0.71, dog, bird),
    (0, 1, 21, 0.45, cat, bird),
    (1, 1, 72, 0.34, cat, dog),
    (0, 0, 50, 0.60, dog, cat),
    (1, 1, 72, 0.51, bird, dog),
    (0, 0, 22, 0.91, cat, bird),
    (1, 1, 133, 0.31, cat, dog),
    (0, 1, 55, 0.69, dog, bird),
    (1, 0, 58, 0.40, bird, dog),
    (0, 1, 69, 0.88, dog, cat),
    (1, 1, 136, 0.35, cat, dog),
    (0, 0, 48, 0.58, dog, cat)))
    .toDF(mockLabelColumn, "col1", "col2", "col3", "col4", "col5")


  test("Get treatment effects") {
    val ldml = new DoubleMLEstimator()
      .setTreatmentModel(new LogisticRegression())
      .setTreatmentCol(mockLabelColumn)
      .setOutcomeModel(new LinearRegression())
      .setOutcomeCol("col2")

    val ldmlModel = ldml.fit(mockDataset)
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

    val ldmlModel = ldml.fit(mockDataset)
    assert(ldmlModel.getAvgTreatmentEffect != 0.0)
  }

  test("Get confidence intervals with multiple iterations") {
    val ldml = new DoubleMLEstimator()
      .setTreatmentModel(new LogisticRegression())
      .setTreatmentCol(mockLabelColumn)
      .setOutcomeModel(new LinearRegression())
      .setOutcomeCol("col2")
      .setMaxIter(10)

    val ldmlModel = ldml.fit(mockDataset)
    assert(ldmlModel.getConfidenceInterval.length == 2)
    val (ateLow, ateHigh) = (ldmlModel.getConfidenceInterval(0), ldmlModel.getConfidenceInterval(1))
    assert(ateLow < ateHigh && ateLow > 0 && ateHigh < 150)
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
