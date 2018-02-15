// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

/** Defines common parameters across all LightGBM learners.
  */
trait LightGBMParams extends MMLParams {
  val parallelism = StringParam(this, "parallelism",
    "Tree learner parallelism, can be set to data_parallel or voting_parallel", "data_parallel")

  def getParallelism: String = $(parallelism)
  def setParallelism(value: String): this.type = set(parallelism, value)

  val defaultListenPort = IntParam(this, "defaultListenPort",
    "The default listen port on executors, used for testing")

  def getDefaultListenPort: Int = $(defaultListenPort)
  def setDefaultListenPort(value: Int): this.type = set(defaultListenPort, value)

  setDefault(defaultListenPort -> LightGBMConstants.defaultLocalListenPort)

  val numIterations = IntParam(this, "numIterations",
    "Number of iterations, LightGBM constructs num_class * num_iterations trees", 100)

  def getNumIterations: Int = $(numIterations)
  def setNumIterations(value: Int): this.type = set(numIterations, value)

  val learningRate = DoubleParam(this, "learningRate", "Learning rate or shrinkage rate", 0.1)

  def getLearningRate: Double = $(learningRate)
  def setLearningRate(value: Double): this.type = set(learningRate, value)

  val numLeaves = IntParam(this, "numLeaves", "Number of leaves", 31)

  def getNumLeaves: Int = $(numLeaves)
  def setNumLeaves(value: Int): this.type = set(numLeaves, value)
}
