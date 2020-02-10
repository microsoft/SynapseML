// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.isolationforest

import com.microsoft.ml.spark.build.BuildInfo
import com.microsoft.ml.spark.core.env.FileUtilities
import com.microsoft.ml.spark.core.metrics.MetricConstants
import com.microsoft.ml.spark.core.test.benchmarks.Benchmarks
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row}
import com.microsoft.ml.spark.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.scalactic.Tolerance._
import com.microsoft.ml.spark.train.ComputeModelStatistics

case class MammographyRecord(feature0: Double, feature1: Double, feature2: Double, feature3: Double,
                             feature4: Double, feature5: Double, label: Double)
case class ScoringResult(features: Vector, label: Double, predictedLabel: Double, outlierScore: Double)

class VerifyIsolationForest extends Benchmarks with EstimatorFuzzing[IsolationForest] {
  test ("Verify isolationForestMammographyDataTest") {
    import session.implicits._

    val data = loadMammographyData

    // Train a new isolation forest model
    val contamination = 0.02
    val isolationForest = new IsolationForest()
      .setNumEstimators(100)
      .setBootstrap(false)
      .setMaxSamples(256)
      .setMaxFeatures(1.0)
      .setFeaturesCol("features")
      .setPredictionCol("predictedLabel")
      .setScoreCol("outlierScore")
      .setContamination(0.02)
      .setContaminationError(contamination * 0.01)
      .setRandomSeed(1)

    // Score all training data instances using the new model
    val isolationForestModel = isolationForest.fit(data)

    // Calculate area under ROC curve and assert
    val scores = isolationForestModel.transform(data).as[ScoringResult]
    val metrics = new ComputeModelStatistics()
      .setEvaluationMetric(MetricConstants.AucSparkMetric)
      .setLabelCol("label")
      .setScoredLabelsCol("predictedLabel")
      .setScoresCol("outlierScore")
      .transform(scores)

    // Expectation from results in the 2008 "Isolation Forest" paper by F. T. Liu, et al.
    val aurocExpectation = 0.86
    val uncert = 0.02
    val auroc = metrics.first().getDouble(1)
    assert(auroc === aurocExpectation +- uncert, "expected area under ROC =" +
        s" $aurocExpectation +/- $uncert, but observed $auroc")
  }

  def loadMammographyData(): DataFrame = {

    import session.implicits._

    val mammographyRecordSchema = Encoders.product[MammographyRecord].schema

    val fileLocation = FileUtilities.join(BuildInfo.datasetDir,"IsolationForest", "mammography.csv").toString

    // Open source dataset from http://odds.cs.stonybrook.edu/mammography-dataset/
    val rawData = session.read
      .format("csv")
      .option("comment", "#")
      .option("header", "false")
      .schema(mammographyRecordSchema)
      .load(fileLocation)

    val assembler = new VectorAssembler()
      .setInputCols(Array("feature0", "feature1", "feature2", "feature3", "feature4", "feature5"))
      .setOutputCol("features")

    val data = assembler
      .transform(rawData)
      .select("features", "label")

    data
  }

  override def reader: MLReadable[_] = IsolationForest
  override def modelReader: MLReadable[_] = IsolationForestModel

  override def testObjects(): Seq[TestObject[IsolationForest]] = {
    val dataset = loadMammographyData.toDF

    Seq(new TestObject(
      new IsolationForest(),
      dataset))
  }
}
