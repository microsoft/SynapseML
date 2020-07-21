// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.vw

import com.microsoft.ml.spark.core.test.base.TestBase
import org.apache.spark.ml.Pipeline

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

    val shared_featurizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("shared"))
      .setOutputCol("shared_features")

    val action_one_featurizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action1"))
      .setOutputCol("action1_features")

    val action_two_featurizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action2_feat1", "action2_feat2"))
      .setOutputCol("action2_features")

    val action_merger = new VectorZipper()
      .setInputCols(Array("action1_features", "action2_features"))
      .setOutputCol("action_features")

    val pipeline = new Pipeline()
      .setStages(Array(shared_featurizer, action_one_featurizer, action_two_featurizer, action_merger))
    val model = pipeline.fit(df)
    val transformedDf = model.transform(df).select("chosen_action", "cost", "prob", "shared_features", "action_features")

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

  test("Verify ColumnVectorSequencer can merge two columns") {
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

    val cb_with_incorrect_action_col = new VowpalWabbitContextualBandit()
      .setArgs("--quiet")
      .setEpsilon(0.2)
      .setLabelCol("cost")
      .setProbabilityCol("prob")
      .setChosenActionCol("chosen_action")
      .setSharedCol("shared")
      .setFeaturesCol("action1")
      .setUseBarrierExecutionMode(false)

    val untransformed_df = Seq(
      ("shared_f", "action1_f", 1, 1, 0.8)
    ).toDF("shared", "action1", "chosen_action", "cost", "prob")

    assertThrows[AssertionError](cb_with_incorrect_action_col.fit(untransformed_df))

    val shared_featurizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("shared"))
      .setOutputCol("shared_features")
    val action_one_featurizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action1"))
      .setOutputCol("action1_features")
    val action_merger = new VectorZipper()
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
      .setStages(Array(shared_featurizer, action_one_featurizer, action_merger, cb))
    pipeline.fit(untransformed_df)

    val untransformed_df_with_decimal_chosen_action = Seq(
      ("shared_f", "action1_f", 1.5, 1, 0.8)
    ).toDF("shared", "action1", "chosen_action", "cost", "prob")

    assertThrows[AssertionError](pipeline.fit(untransformed_df_with_decimal_chosen_action))

    val untransformed_df_with_string_prob = Seq(
      ("shared_f", "action1_f", 1.5, 1, "I'm a prob!")
    ).toDF("shared", "action1", "chosen_action", "cost", "prob")

    assertThrows[AssertionError](pipeline.fit(untransformed_df_with_string_prob))
  }
}
