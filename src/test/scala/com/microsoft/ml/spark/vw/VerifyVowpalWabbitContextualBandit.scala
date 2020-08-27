// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.vw

import com.microsoft.ml.spark.core.test.base.TestBase
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.ParamGridBuilder

class VerifyVowpalWabbitContextualBandit extends TestBase {
  test("Verify VerifyVowpalWabbitContextualBandit can be run on toy dataset") {
    import session.implicits._

    val df = Seq(
      ("shared_f", "action1_f", "action2_f", "action2_f2=0", 1, 1, 0.8),
      ("shared_f", "action1_f", "action2_f", "action2_f2=1", 2, 1, 0.8),
      ("shared_f", "action1_f", "action2_f", "action2_f2=1", 2, 4, 0.8),
      ("shared_f", "action1_f", "action2_f", "action2_f2=0", 1, 0, 0.8)
    ).toDF("shared", "action1", "action2_feat1", "action2_feat2", "chosen_action", "cost", "prob")
      .coalesce(1)
      .cache()

    val sharedFeaturizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("shared"))
      .setOutputCol("shared_features")

    val actionOneFeaturizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action1"))
      .setOutputCol("action1_features")

    val actionTwoFeaturizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action2_feat1", "action2_feat2"))
      .setOutputCol("action2_features")

    val actionMerger = new VectorZipper()
      .setInputCols(Array("action1_features", "action2_features"))
      .setOutputCol("action_features")

    val pipeline = new Pipeline()
      .setStages(Array(sharedFeaturizer, actionOneFeaturizer, actionTwoFeaturizer, actionMerger))
    val model = pipeline.fit(df)
    val transformedDf = model.transform(df)
      .select("chosen_action", "cost", "prob", "shared_features", "action_features")

    val cb = new VowpalWabbitContextualBandit()
      .setArgs("--cb_explore_adf --epsilon 0.2 --quiet")
      .setLabelCol("cost")
      .setProbabilityCol("prob")
      .setChosenActionCol("chosen_action")
      .setSharedCol("shared_features")
      .setFeaturesCol("action_features")
      .setUseBarrierExecutionMode(false)

    val m = cb.fit(transformedDf)
    assert(m.getPerformanceStatistics.select("ipsEstimate").first.getDouble(0) > 0)
    assert(m.getPerformanceStatistics.select("snipsEstimate").first.getDouble(0) > 0)
  }

  test("Verify VectorZipper can merge two columns") {
    import session.implicits._

    val df = Seq(
      ("thing1", "thing2")
    ).toDF("col1", "col2")

    val sequencer = new VectorZipper()
      .setInputCols(Array("col1", "col2"))
      .setOutputCol("combined_things")
    val transformedDf = sequencer.transform(df)
    val result = transformedDf.collect.head.getAs[Seq[String]]("combined_things")
    assert(result == Seq("thing1", "thing2"))
  }

  test("Verify columns are correct data type") {
    import session.implicits._

    val cbWithIncorrectActionCol = new VowpalWabbitContextualBandit()
      .setArgs("--quiet")
      .setEpsilon(0.2)
      .setLabelCol("cost")
      .setProbabilityCol("prob")
      .setChosenActionCol("chosen_action")
      .setSharedCol("shared")
      .setFeaturesCol("action1")
      .setUseBarrierExecutionMode(false)

    val untransformedDf = Seq(
      ("shared_f", "action1_f", 1, 1, 0.8)
    ).toDF("shared", "action1", "chosen_action", "cost", "prob")

    assertThrows[AssertionError](cbWithIncorrectActionCol.fit(untransformedDf))

    val sharedFeaturizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("shared"))
      .setOutputCol("shared_features")
    val actionOneFeaturizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action1"))
      .setOutputCol("action1_features")
    val actionMerger = new VectorZipper()
      .setInputCols(Array("action1_features"))
      .setOutputCol("action_features")
    val cb = new VowpalWabbitContextualBandit()
      .setArgs("--cb_explore_adf --epsilon 0.2 --quiet")
      .setLabelCol("cost")
      .setProbabilityCol("prob")
      .setChosenActionCol("chosen_action")
      .setSharedCol("shared_features")
      .setFeaturesCol("action_features")
      .setUseBarrierExecutionMode(false)

    val pipeline = new Pipeline()
      .setStages(Array(sharedFeaturizer, actionOneFeaturizer, actionMerger, cb))
    pipeline.fit(untransformedDf)

    pipeline.fit(untransformedDf, Array(new ParamMap()))

    val untransformedDfWithDecimalChosenAction = Seq(
      ("shared_f", "action1_f", 1.5, 1, 0.8)
    ).toDF("shared", "action1", "chosen_action", "cost", "prob")

    assertThrows[AssertionError](pipeline.fit(untransformedDfWithDecimalChosenAction))

    val untransformedDfWithStringProb = Seq(
      ("shared_f", "action1_f", 1.5, 1, "I'm a prob!")
    ).toDF("shared", "action1", "chosen_action", "cost", "prob")

    assertThrows[AssertionError](pipeline.fit(untransformedDfWithStringProb))
  }

  test("Verify raises exception when used with incompatible args") {
    val df = CBDatasetHelper.getCBDataset(session)
    assertThrows[NotImplementedError](
      new VowpalWabbitContextualBandit().setArgs("--quiet --cb --episilon 0.5").fit(df))
    assertThrows[NotImplementedError](
      new VowpalWabbitContextualBandit().setArgs("--quiet --cb_explore").fit(df))
    assertThrows[NotImplementedError](
      new VowpalWabbitContextualBandit().setArgs("--quiet --cb_adf").fit(df))
    new VowpalWabbitContextualBandit().setArgs("--quiet --cb_explore_adf").fit(df)
  }

  test("Verify can use paralellized fit") {
    val df = CBDatasetHelper.getCBDataset(session)
      .coalesce(1)
      .cache()

    val cb = new VowpalWabbitContextualBandit()
      .setArgs("--quiet")
      .setParallelismForParamListFit(4)
      .setUseBarrierExecutionMode(false)

    val paramGrid = new ParamGridBuilder()
      .addGrid(cb.learningRate, Array(0.5, 0.05, 0.001))
      .addGrid(cb.epsilon, Array(0.1, 0.2, 0.4))
      .build()

    val m = cb.fit(df, paramGrid)
      .map(model => model.getPerformanceStatistics)
      .reduce((a, b) => a.union(b))
    assert(m.select("ipsEstimate").first.getDouble(0) < 0)
  }

  test("Verify can supply additional namespaces") {
    import session.implicits._

    val df = Seq(
      ("shared_f", "shared_f2", "action1_f", "action1_ns2", "action2_f", "action2_f2=0", "action2_ns2", 1, 1, 0.8),
      ("shared_f", "shared_f2", "action1_f", "action1_ns2", "action2_f", "action2_f2=1", "action2_ns2", 2, 1, 0.8),
      ("shared_f", "shared_f2", "action1_f", "action1_ns2", "action2_f", "action2_f2=1", "action2_ns2", 2, 4, 0.8),
      ("shared_f", "shared_f2", "action1_f", "action1_ns2", "action2_f", "action2_f2=0", "action2_ns2", 1, 0, 0.8)
    ).toDF("shared_ns1", "shared_ns2", "action1_ns1", "action1_ns2", "action2_f1_ns1",
      "action2_f2_ns1", "action2_f1_ns2", "chosen_action", "cost", "prob")
      .coalesce(1)
      .cache()

    val sharedFeaturizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("shared_ns1"))
      .setOutputCol("shared_features_ns1")

    val sharedFeaturizer2 = new VowpalWabbitFeaturizer()
      .setInputCols(Array("shared_ns2"))
      .setOutputCol("shared_features_ns2")

    val a1Ns1Featurizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action1_ns1"))
      .setOutputCol("action1_features_ns1")

    val a1Ns2Featurizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action1_ns2"))
      .setOutputCol("action1_features_ns2")

    val a2Ns1Featurizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action2_f1_ns1", "action2_f2_ns1"))
      .setOutputCol("action2_features_ns1")

    val a2Ns2Featurizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action2_f1_ns2"))
      .setOutputCol("action2_features_ns2")

    val actionMergerNs1 = new VectorZipper()
      .setInputCols(Array("action1_features_ns1", "action2_features_ns1"))
      .setOutputCol("action_features_ns1")

    val actionMergerNs2 = new VectorZipper()
      .setInputCols(Array("action1_features_ns2", "action2_features_ns2"))
      .setOutputCol("action_features_ns2")

    val pipeline = new Pipeline()
      .setStages(Array(sharedFeaturizer, sharedFeaturizer2, a1Ns1Featurizer, a1Ns2Featurizer,
        a2Ns1Featurizer, a2Ns2Featurizer, actionMergerNs1, actionMergerNs2))
    val model = pipeline.fit(df)
    val transformedDf = model.transform(df)

    val cb = new VowpalWabbitContextualBandit()
      .setArgs("--cb_explore_adf --epsilon 0.2 --quiet")
      .setLabelCol("cost")
      .setProbabilityCol("prob")
      .setChosenActionCol("chosen_action")
      .setSharedCol("shared_features_ns1")
      .setAdditionalSharedFeatures(Array("shared_features_ns2"))
      .setFeaturesCol("action_features_ns1")
      .setAdditionalFeatures(Array("action_features_ns2"))
      .setUseBarrierExecutionMode(false)

    val m = cb.fit(transformedDf)
    assert(m.getPerformanceStatistics.select("ipsEstimate").first.getDouble(0) > 0)
    assert(m.getPerformanceStatistics.select("snipsEstimate").first.getDouble(0) > 0)
  }

  test("Verify can predict on the resulting model") {
    import session.implicits._

    val df = Seq(
      ("shared_f", "shared_f2", "action1_f", "action1_ns2", "action2_f", "action2_f2=0", "action2_ns2", 1, 1, 0.8),
      ("shared_f", "shared_f2", "action1_f", "action1_ns2", "action2_f", "action2_f2=1", "action2_ns2", 2, 1, 0.8),
      ("shared_f", "shared_f2", "action1_f", "action1_ns2", "action2_f", "action2_f2=1", "action2_ns2", 2, 4, 0.8),
      ("shared_f", "shared_f2", "action1_f", "action1_ns2", "action2_f", "action2_f2=0", "action2_ns2", 1, 0, 0.8)
    ).toDF("shared_ns1", "shared_ns2", "action1_ns1", "action1_ns2", "action2_f1_ns1", "action2_f2_ns1",
      "action2_f1_ns2", "chosen_action", "cost", "prob")
      .coalesce(1)
      .cache()

    val sharedFeaturizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("shared_ns1"))
      .setOutputCol("shared_features_ns1")

    val sharedFeaturizer2 = new VowpalWabbitFeaturizer()
      .setInputCols(Array("shared_ns2"))
      .setOutputCol("shared_features_ns2")

    val a1Ns1Featurizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action1_ns1"))
      .setOutputCol("action1_features_ns1")

    val a1Ns2Featurizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action1_ns2"))
      .setOutputCol("action1_features_ns2")

    val a2Ns1Featurizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action2_f1_ns1", "action2_f2_ns1"))
      .setOutputCol("action2_features_ns1")

    val a2Ns2Featurizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action2_f1_ns2"))
      .setOutputCol("action2_features_ns2")

    val actionMergerNs1 = new VectorZipper()
      .setInputCols(Array("action1_features_ns1", "action2_features_ns1"))
      .setOutputCol("action_features_ns1")

    val actionMergerNs2 = new VectorZipper()
      .setInputCols(Array("action1_features_ns2", "action2_features_ns2"))
      .setOutputCol("action_features_ns2")

    val pipeline = new Pipeline()
      .setStages(Array(sharedFeaturizer, sharedFeaturizer2, a1Ns1Featurizer, a1Ns2Featurizer, a2Ns1Featurizer,
        a2Ns2Featurizer, actionMergerNs1, actionMergerNs2))
    val model = pipeline.fit(df)
    val transformedDf = model.transform(df)

    val cb = new VowpalWabbitContextualBandit()
      .setArgs("--cb_explore_adf --epsilon 0.2 --quiet")
      .setLabelCol("cost")
      .setProbabilityCol("prob")
      .setChosenActionCol("chosen_action")
      .setSharedCol("shared_features_ns1")
      .setAdditionalSharedFeatures(Array("shared_features_ns2"))
      .setFeaturesCol("action_features_ns1")
      .setPredictionCol("output_prediction")
      .setAdditionalFeatures(Array("action_features_ns2"))
      .setUseBarrierExecutionMode(false)

    val m = cb.fit(transformedDf)
    assert(m.getPerformanceStatistics.select("ipsEstimate").first.getDouble(0) > 0)

    val predictSet = Seq(
      ("shared_f", "shared_f2", "action1_f", "action1_ns2", "action2_f", "action2_f2=0", "action2_ns2", 1, 1, 0.8),
      ("shared_f", "shared_f2", "action1_f", "action1_ns2", "action2_f", "action2_f2=1", "action2_ns2", 2, 1, 0.8)
    ).toDF("shared_ns1", "shared_ns2", "action1_ns1", "action1_ns2", "action2_f1_ns1", "action2_f2_ns1",
      "action2_f1_ns2", "chosen_action", "cost", "prob")
      .coalesce(1)
      .cache()

    val transformedPredictSet = model.transform(predictSet)
    val results = m.transform(transformedPredictSet)
    assert(results.collect.head.getAs[Seq[Float]]("output_prediction").length == 2)
  }
}
