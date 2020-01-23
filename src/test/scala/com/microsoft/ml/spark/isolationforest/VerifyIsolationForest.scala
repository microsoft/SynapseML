// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.isolationforest

import java.io.File
import java.nio.file.Files

import com.microsoft.ml.spark.build.BuildInfo
import com.microsoft.ml.spark.core.env.FileUtilities
import com.microsoft.ml.spark.core.test.benchmarks.{Benchmarks, DatasetUtils}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row}
import com.microsoft.ml.spark.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import org.apache.spark.TaskContext
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.scalactic.Tolerance._

case class LabeledDataPointVector(features: Vector, label: Double)
case class MammographyRecord(feature0: Double, feature1: Double, feature2: Double, feature3: Double,
                             feature4: Double, feature5: Double, label: Double)
case class ScoringResult(features: Vector, label: Double, predictedLabel: Double, outlierScore: Double)
case class ShuttleRecord(feature0: Double, feature1: Double, feature2: Double, feature3: Double,
                         feature4: Double, feature5: Double, feature6: Double, feature7: Double,
                         feature8: Double, label: Double)

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
    val metrics = new BinaryClassificationMetrics(scores.rdd.map(x => (x.outlierScore, x.label)))

    // Expectation from results in the 2008 "Isolation Forest" paper by F. T. Liu, et al.
    val aurocExpectation = 0.86
    val uncert = 0.02
    val auroc = metrics.areaUnderROC()
    assert(auroc === aurocExpectation +- uncert, "expected area under ROC =" +
        s" $aurocExpectation +/- $uncert, but observed $auroc")
  }

  def loadMammographyData(): Dataset[LabeledDataPointVector] = {

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
      .as[MammographyRecord]

    val assembler = new VectorAssembler()
      .setInputCols(Array("feature0", "feature1", "feature2", "feature3", "feature4", "feature5"))
      .setOutputCol("features")

    val data = assembler
      .transform(rawData)
      .select("features", "label")
      .as[LabeledDataPointVector]

    data
  }

  def loadShuttleData(): Dataset[LabeledDataPointVector] = {

    import session.implicits._

    val shuttleRecordSchema = Encoders.product[ShuttleRecord].schema

    val fileLocation = FileUtilities.join(BuildInfo.datasetDir,"IsolationForest", "shuttle.csv").toString

    // Open source dataset from http://odds.cs.stonybrook.edu/shuttle-dataset/
    val rawData = session.read
      .format("csv")
      .option("comment", "#")
      .option("header", "false")
      .schema(shuttleRecordSchema)
      .load(fileLocation)
      .as[ShuttleRecord]

    val assembler = new VectorAssembler()
      .setInputCols(Array("feature0", "feature1", "feature2", "feature3", "feature4", "feature5",
        "feature6", "feature7", "feature8"))
      .setOutputCol("features")

    val data = assembler
      .transform(rawData)
      .select("features", "label")
      .as[LabeledDataPointVector]

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
