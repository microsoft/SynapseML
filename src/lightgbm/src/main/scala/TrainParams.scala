// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

/** Defines the common Booster parameters passed to the LightGBM learners.
  */
abstract class TrainParams extends Serializable {
  def parallelism: String
  def numIterations: Int
  def learningRate: Double
  def numLeaves: Int

  override def toString(): String = {
    s"is_pre_partition=True boosting_type=gbdt tree_learner=$parallelism num_iterations=$numIterations " +
      s"learning_rate=$learningRate num_leaves=$numLeaves"
  }
}

/** Defines the Booster parameters passed to the LightGBM classifier.
  */
case class ClassifierTrainParams(val parallelism: String, val numIterations: Int, val learningRate: Double,
                                 val numLeaves: Int)
  extends TrainParams {
  override def toString(): String = {
    s"objective=binary metric=binary_logloss,auc ${super.toString()}"
  }
}

/** Defines the Booster parameters passed to the LightGBM regressor.
  */
case class RegressorTrainParams(val parallelism: String, val numIterations: Int, val learningRate: Double,
                           val numLeaves: Int, val application: String)
  extends TrainParams {
  override def toString(): String = {
    s"objective=$application ${super.toString()}"
  }
}
