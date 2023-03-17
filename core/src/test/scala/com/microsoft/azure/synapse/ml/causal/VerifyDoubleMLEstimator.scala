// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StringType, StructField, StructType}

class VerifyDoubleMLEstimator extends EstimatorFuzzing[DoubleMLEstimator] {
  val cat = "Cat"
  val dog = "Dog"
  val bird = "Bird"
  val schema = StructType(Array(StructField("discount", BooleanType, nullable = false),
    StructField("support", BooleanType, nullable = false),
    StructField("price", IntegerType, nullable = false),
    StructField("weight", DoubleType, nullable = false),
    StructField("category", StringType, nullable = true)
  ))

  import scala.collection.JavaConverters._
  private lazy val mockDataset = spark.createDataFrame(
    Seq(Row(false, true, 50, 0.60, cat),
      Row(true, false, 140, 0.50, dog),
      Row(false, true, 78, 0.99, bird),
      Row(true, false, 92, 0.34, dog),
      Row(false, true, 50, 0.60, bird),
      Row(true, false, 80, 0.50, dog),
      Row(false, true, 78, 0.99, cat),
      Row(true, false, 101, 0.28, bird),
      Row(false, true, 40, 0.58, cat),
      Row(false, true, 42, 0.53, dog),
      Row(true, false, 128, 0.99, bird),
      Row(false, true, 12, 0.34, dog),
      Row(true, true, 55, 0.69, bird),
      Row(false, true, 32, 0.48, dog),
      Row(true, true, 62, 0.78, bird),
      Row(false, true, 19, 0.48, bird),
      Row(false, true, 11, 0.32, bird),
      Row(true, true, 43, 0.63, cat),
      Row(true, true, 138, 0.73, dog),
      Row(true, false, 98, 0.89, bird),
      Row(false, true, 22, 0.39, dog),
      Row(false, false, 47, 0.72, bird),
      Row(true, false, 95, 0.49, dog),
      Row(false, true, 66, 0.71, bird),
      Row(false, true, 21, 0.45, bird),
      Row(true, true, 72, 0.34, dog),
      Row(false, false, 50, 0.60, cat),
      Row(true, true, 72, 0.51, dog),
      Row(false, false, 22, 0.91, bird),
      Row(true, true, 133, 0.31, dog),
      Row(false, true, 55, 0.69, bird),
      Row(true, false, 58, 0.40, dog),
      Row(false, true, 69, 0.88, cat),
      Row(true, true, 136, 0.35, dog),
      Row(false, false, 48, 0.58, cat)).asJava
    , schema)

  test("Get treatment effects") {
    val ldml = new DoubleMLEstimator()
      .setTreatmentModel(new LogisticRegression())
      .setTreatmentCol("discount")
      .setOutcomeModel(new LinearRegression())
      .setOutcomeCol("price")

    val ldmlModel = ldml.fit(mockDataset)
    ldmlModel.getAvgTreatmentEffect
    assert(ldmlModel.getConfidenceInterval.length == 2)
  }

  test("Get treatment effects with weight column") {
    val ldml = new DoubleMLEstimator()
      .setTreatmentModel(new LogisticRegression())
      .setTreatmentCol("discount")
      .setOutcomeModel(new LinearRegression())
      .setOutcomeCol("price")
      .setWeightCol("weight")

    val ldmlModel = ldml.fit(mockDataset)
    ldmlModel.getAvgTreatmentEffect
  }

  test("Get confidence intervals with multiple iterations") {
    val ldml = new DoubleMLEstimator()
      .setTreatmentModel(new LogisticRegression())
      .setTreatmentCol("discount")
      .setOutcomeModel(new LinearRegression())
      .setOutcomeCol("price")
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
        .setTreatmentCol("discount")
        .setOutcomeModel(new LinearRegression())
        .setOutcomeCol("price")
        .setMaxIter(20)

      ldml.fit(mockDataset)
    }
  }

  test("Mismatch outcome model and outcome column will throw exception.") {
    assertThrows[Exception] {
      val ldml = new DoubleMLEstimator()
        .setTreatmentModel(new LogisticRegression())
        .setTreatmentCol("discount")
        .setOutcomeModel(new LogisticRegression())
        .setOutcomeCol("price")
        .setMaxIter(5)

      val dmlModel = ldml.fit(mockDataset)
      dmlModel.getAvgTreatmentEffect
    }
  }

  override def testObjects(): Seq[TestObject[DoubleMLEstimator]] =
    Seq(new TestObject(new DoubleMLEstimator()
      .setTreatmentModel(new LogisticRegression())
      .setTreatmentCol("discount")
      .setOutcomeModel(new LinearRegression())
      .setOutcomeCol("price"),
      mockDataset))

  override def reader: MLReadable[_] = DoubleMLEstimator

  override def modelReader: MLReadable[_] = DoubleMLModel
}
