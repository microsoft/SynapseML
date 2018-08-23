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

  val objective = StringParam(this, "objective",
    "The Objective. For regression applications, this can be: " +
    "regression_l2, regression_l1, huber, fair, poisson, quantile, mape, gamma or tweedie. " +
    "For classification applications, this can be: binary, multiclass, or multiclassova. ", "regression")

  def getObjective: String = $(objective)
  def setObjective(value: String): this.type = set(objective, value)

  val maxBin = IntParam(this, "maxBin", "Max bin", 255)

  def getMaxBin: Int = $(maxBin)
  def setMaxBin(value: Int): this.type = set(maxBin, value)

  val baggingFraction = DoubleParam(this, "baggingFraction", "Bagging fraction", 1)

  def getBaggingFraction: Double = $(baggingFraction)
  def setBaggingFraction(value: Double): this.type = set(baggingFraction, value)

  val baggingFreq = IntParam(this, "baggingFreq", "Bagging frequence", 0)

  def getBaggingFreq: Int = $(baggingFreq)
  def setBaggingFreq(value: Int): this.type = set(baggingFreq, value)

  val baggingSeed = IntParam(this, "baggingSeed", "Bagging seed", 3)

  def getBaggingSeed: Int = $(baggingSeed)
  def setBaggingSeed(value: Int): this.type = set(baggingSeed, value)

  val earlyStoppingRound = IntParam(this, "earlyStoppingRound", "Early stopping round", 0)

  def getEarlyStoppingRound: Int = $(earlyStoppingRound)
  def setEarlyStoppingRound(value: Int): this.type = set(earlyStoppingRound, value)

  val featureFraction = DoubleParam(this, "featureFraction", "Feature fraction", 1)

  def getFeatureFraction: Double = $(featureFraction)
  def setFeatureFraction(value: Double): this.type = set(featureFraction, value)

  val maxDepth = IntParam(this, "maxDepth", "Max depth", -1)

  def getMaxDepth: Int = $(maxDepth)
  def setMaxDepth(value: Int): this.type = set(maxDepth, value)

  val minSumHessianInLeaf = DoubleParam(this, "minSumHessianInLeaf", "Minimal sum hessian in one leaf", 1e-3)

  def getMinSumHessianInLeaf: Double = $(minSumHessianInLeaf)
  def setMinSumHessianInLeaf(value: Double): this.type = set(minSumHessianInLeaf, value)

  val timeout = DoubleParam(this, "timeout", "Timeout in seconds", 120)

  def getTimeout: Double = $(timeout)
  def setTimeout(value: Double): this.type = set(timeout, value)

  val modelString = StringParam(this, "modelString", "LightGBM model to retrain", "")

  def getModelString: String = $(modelString)
  def setModelString(value: String): this.type = set(modelString, value)
}
