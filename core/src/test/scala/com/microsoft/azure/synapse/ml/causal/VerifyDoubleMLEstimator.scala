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
    (0, 1, 50, 0.60, cat),
    (1, 0, 140, 0.50, dog),
    (0, 1, 78, 0.99, bird),
    (1, 0, 92, 0.34, dog),
    (0, 1, 50, 0.60, bird),
    (1, 0, 80, 0.50, dog),
    (0, 1, 78, 0.99, cat),
    (1, 0, 101, 0.28, bird),
    (0, 1, 40, 0.58, cat),
    (0, 1, 42, 0.53, dog),
    (1, 0, 128, 0.99, bird),
    (0, 1, 12, 0.34, dog),
    (1, 1, 55, 0.69, bird),
    (0, 1, 32, 0.48, dog),
    (1, 1, 62, 0.78, bird),
    (0, 1, 19, 0.48, bird),
    (0, 1, 11, 0.32, bird),
    (1, 1, 43, 0.63, cat),
    (1, 1, 138, 0.73, dog),
    (1, 0, 98, 0.89, bird),
    (0, 1, 22, 0.39, dog),
    (0, 0, 47, 0.72, bird),
    (1, 0, 95, 0.49, dog),
    (0, 1, 66, 0.71, bird),
    (0, 1, 21, 0.45, bird),
    (1, 1, 72, 0.34, dog),
    (0, 0, 50, 0.60, cat),
    (1, 1, 72, 0.51, dog),
    (0, 0, 22, 0.91, bird),
    (1, 1, 133, 0.31, dog),
    (0, 1, 55, 0.69, bird),
    (1, 0, 58, 0.40, dog),
    (0, 1, 69, 0.88, cat),
    (1, 1, 136, 0.35, dog),
    (0, 0, 48, 0.58, cat)))
    .toDF(mockLabelColumn, "col1", "col2", "col3", "col4")


  test("Get treatment effects") {
    val ldml = new DoubleMLEstimator()
      .setTreatmentModel(new LogisticRegression())
      .setTreatmentCol(mockLabelColumn)
      .setOutcomeModel(new LinearRegression())
      .setOutcomeCol("col2")

    val ldmlModel = ldml.fit(mockDataset)
    ldmlModel.getAvgTreatmentEffect
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
    ldmlModel.getAvgTreatmentEffect
  }

  test("Get confidence intervals with multiple iterations") {
    val ldml = new DoubleMLEstimator()
      .setTreatmentModel(new LogisticRegression())
      .setTreatmentCol(mockLabelColumn)
      .setOutcomeModel(new LinearRegression())
      .setOutcomeCol("col2")
      .setMaxIter(30)

    val ldmlModel = ldml.fit(mockDataset)
    assert(ldmlModel.getConfidenceInterval.length == 2)
    val (ateLow, ateHigh) = (ldmlModel.getConfidenceInterval(0), ldmlModel.getConfidenceInterval(1))
    assert(ateLow < ateHigh && ateLow > -130 && ateHigh < 130)
  }

  test("Mismatch treatment model and treatment column will throw exception.") {
    assertThrows[Exception] {
      val ldml = new DoubleMLEstimator()
        .setTreatmentModel(new LinearRegression())
        .setTreatmentCol(mockLabelColumn)
        .setOutcomeModel(new LinearRegression())
        .setOutcomeCol("col2")
        .setMaxIter(20)

      ldml.fit(mockDataset)
    }
  }

  test("Mismatch outcome model and outcome column will throw exception.") {
    assertThrows[Exception] {
      val ldml = new DoubleMLEstimator()
        .setTreatmentModel(new LogisticRegression())
        .setTreatmentCol(mockLabelColumn)
        .setOutcomeModel(new LinearRegression())
        .setOutcomeCol("col1")
        .setMaxIter(5)

      val dmlModel = ldml.fit(mockDataset)
      dmlModel.getAvgTreatmentEffect
    }
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
