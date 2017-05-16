// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azureml

import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, Row}

class MetaEstimatorSpec extends ModulesTestBase {

  // Prepare training data from a list of (id, text, label) tuples.
  val trainDF: DataFrame = session.createDataFrame(Seq(
    (0L, "a b c d e spark", 1.0),
    (1L, "b d", 0.0),
    (2L, "spark f g h", 1.0),
    (3L, "hadoop mapreduce", 0.0),
    (4L, "b spark who", 1.0),
    (5L, "g d a y", 0.0),
    (6L, "spark fly", 1.0),
    (7L, "was mapreduce", 0.0),
    (8L, "e spark program", 1.0),
    (9L, "a e c l", 0.0),
    (10L, "spark compile", 1.0),
    (11L, "hadoop software", 0.0)
  )).toDF("id", "text", "label")

  // Prepare test documents, which are unlabeled (id, text) tuples.
  val testDF: DataFrame = session.createDataFrame(Seq(
    (4L, "spark i j k"),
    (5L, "l m n"),
    (6L, "mapreduce spark"),
    (7L, "apache hadoop")
  )).toDF("id", "text")

  test(" a meta estimator should work like a regular estimator") {
    val tok: Tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    val meta: MetaEstimator = new MetaEstimator().setBaseEstimator(tok)

    // Typed mapping for convenience
    val meta2 = meta.mapBaseEstimator[Tokenizer](_.setOutputCol("String2"))

    // Untyped mapping for generality
    val meta3 = meta.mapBaseEstimator[PipelineStage](t => t.set(t.getParam("outputCol"),"string2"))

    val tokResults = tok.transform(testDF).collect().toSet
    val metaResults = meta.fit(testDF).transform(testDF).collect().toSet
    val meta2Results = meta2.fit(testDF).transform(testDF).collect().toSet

    assert(meta2Results == metaResults)
    assert(tokResults == metaResults)
  }

  test(" a meta estimator should work with spark cross validation") {
    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    val tokenizer: Tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    val hashingTF: HashingTF = new HashingTF()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val lr: LogisticRegression = new LogisticRegression()
      .setMaxIter(10)
    val rf: RandomForestClassifier = new RandomForestClassifier()
    val meta = new MetaEstimator()

    val pipeline: Pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, meta))

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
    // this grid will have 2 x 2 = 6 parameter settings for CrossValidator to choose from.
    val paramGrid1: Array[ParamMap] = new ParamGridBuilder()
      .baseOn(meta.baseEstimator -> lr)
      .addGrid(hashingTF.numFeatures, Array(10, 100))
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .build()

    val paramGrid2: Array[ParamMap] = new ParamGridBuilder()
      .baseOn(meta.baseEstimator -> rf, hashingTF.numFeatures -> 10)
      .addGrid(rf.numTrees, Array(2, 5, 10))
      .build()

    val paramGrid: Array[ParamMap] = paramGrid1 ++ paramGrid2

    val cv: CrossValidator = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2) // Use 3+ in practice

    // Run cross-validation, and choose the best set of parameters.
    val cvModel: CrossValidatorModel = cv.fit(trainDF)

    assert(cvModel.avgMetrics.length == paramGrid1.length + paramGrid2.length)

    // Make predictions on test documents. cvModel uses the best model found (lrModel).
    cvModel.transform(testDF)
      .select("id", "text", "probability", "prediction")
      .collect()
      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
        assert(prob.size == 2)
      }
  }
}
