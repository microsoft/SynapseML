// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm

import com.microsoft.ml.spark.core.contracts.{HasInitScoreCol, HasValidationIndicatorCol, HasWeightCol, Wrappable}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.DefaultParamsWritable

/** Defines common LightGBM execution parameters.
  */
trait LightGBMExecutionParams extends Wrappable {
  val parallelism = new Param[String](this, "parallelism",
    "Tree learner parallelism, can be set to data_parallel or voting_parallel")
  setDefault(parallelism->"data_parallel")

  def getParallelism: String = $(parallelism)
  def setParallelism(value: String): this.type = set(parallelism, value)

  val defaultListenPort = new IntParam(this, "defaultListenPort",
    "The default listen port on executors, used for testing")

  def getDefaultListenPort: Int = $(defaultListenPort)
  def setDefaultListenPort(value: Int): this.type = set(defaultListenPort, value)

  setDefault(defaultListenPort -> LightGBMConstants.DefaultLocalListenPort)

  val timeout = new DoubleParam(this, "timeout", "Timeout in seconds")
  setDefault(timeout -> 1200)

  def getTimeout: Double = $(timeout)
  def setTimeout(value: Double): this.type = set(timeout, value)

  val useBarrierExecutionMode = new BooleanParam(this, "useBarrierExecutionMode",
    "Use new barrier execution mode in Beta testing, off by default.")
  setDefault(useBarrierExecutionMode -> false)

  def getUseBarrierExecutionMode: Boolean = $(useBarrierExecutionMode)
  def setUseBarrierExecutionMode(value: Boolean): this.type = set(useBarrierExecutionMode, value)

  val numBatches = new IntParam(this, "numBatches",
    "If greater than 0, splits data into separate batches during training")
  setDefault(numBatches -> 0)

  def getNumBatches: Int = $(numBatches)
  def setNumBatches(value: Int): this.type = set(numBatches, value)
}

/** Defines common parameters across all LightGBM learners related to learning score evolution.
  */
trait LightGBMLearnerParams extends Wrappable {
  val earlyStoppingRound = new IntParam(this, "earlyStoppingRound", "Early stopping round")
  setDefault(earlyStoppingRound -> 0)

  def getEarlyStoppingRound: Int = $(earlyStoppingRound)
  def setEarlyStoppingRound(value: Int): this.type = set(earlyStoppingRound, value)

  val improvementTolerance = new Param[Double](this, "improvementTolerance",
    "Tolerance to consider improvement in metric")
  setDefault(improvementTolerance -> 0.0)

  def getImprovementTolerance: Double = $(improvementTolerance)
  def setImprovementTolerance(value: Double): this.type = set(improvementTolerance, value)
}

/** Defines common parameters across all LightGBM learners.
  */
trait LightGBMParams extends Wrappable with DefaultParamsWritable with HasWeightCol
  with HasValidationIndicatorCol with HasInitScoreCol with LightGBMExecutionParams with LightGBMLearnerParams {
  val numIterations = new IntParam(this, "numIterations",
    "Number of iterations, LightGBM constructs num_class * num_iterations trees")
  setDefault(numIterations->100)

  def getNumIterations: Int = $(numIterations)
  def setNumIterations(value: Int): this.type = set(numIterations, value)

  val learningRate = new DoubleParam(this, "learningRate", "Learning rate or shrinkage rate")
  setDefault(learningRate -> 0.1)

  def getLearningRate: Double = $(learningRate)
  def setLearningRate(value: Double): this.type = set(learningRate, value)

  val numLeaves = new IntParam(this, "numLeaves", "Number of leaves")
  setDefault(numLeaves -> 31)

  def getNumLeaves: Int = $(numLeaves)
  def setNumLeaves(value: Int): this.type = set(numLeaves, value)

  val objective = new Param[String](this, "objective",
    "The Objective. For regression applications, this can be: " +
    "regression_l2, regression_l1, huber, fair, poisson, quantile, mape, gamma or tweedie. " +
    "For classification applications, this can be: binary, multiclass, or multiclassova. ")
  setDefault(objective -> "regression")

  def getObjective: String = $(objective)
  def setObjective(value: String): this.type = set(objective, value)

  val maxBin = new IntParam(this, "maxBin", "Max bin")
  setDefault(maxBin -> 255)

  def getMaxBin: Int = $(maxBin)
  def setMaxBin(value: Int): this.type = set(maxBin, value)

  val baggingFraction = new DoubleParam(this, "baggingFraction", "Bagging fraction")
  setDefault(baggingFraction->1)

  def getBaggingFraction: Double = $(baggingFraction)
  def setBaggingFraction(value: Double): this.type = set(baggingFraction, value)

  val baggingFreq = new IntParam(this, "baggingFreq", "Bagging frequency")
  setDefault(baggingFreq->0)

  def getBaggingFreq: Int = $(baggingFreq)
  def setBaggingFreq(value: Int): this.type = set(baggingFreq, value)

  val baggingSeed = new IntParam(this, "baggingSeed", "Bagging seed")
  setDefault(baggingSeed->3)

  def getBaggingSeed: Int = $(baggingSeed)
  def setBaggingSeed(value: Int): this.type = set(baggingSeed, value)

  val featureFraction = new DoubleParam(this, "featureFraction", "Feature fraction")
  setDefault(featureFraction->1)

  def getFeatureFraction: Double = $(featureFraction)
  def setFeatureFraction(value: Double): this.type = set(featureFraction, value)

  val maxDepth = new IntParam(this, "maxDepth", "Max depth")
  setDefault(maxDepth-> -1)

  def getMaxDepth: Int = $(maxDepth)
  def setMaxDepth(value: Int): this.type = set(maxDepth, value)

  val minSumHessianInLeaf = new DoubleParam(this, "minSumHessianInLeaf", "Minimal sum hessian in one leaf")
  setDefault(minSumHessianInLeaf -> 1e-3)

  def getMinSumHessianInLeaf: Double = $(minSumHessianInLeaf)
  def setMinSumHessianInLeaf(value: Double): this.type = set(minSumHessianInLeaf, value)

  val modelString = new Param[String](this, "modelString", "LightGBM model to retrain")
  setDefault(modelString -> "")

  def getModelString: String = $(modelString)
  def setModelString(value: String): this.type = set(modelString, value)

  val verbosity = new IntParam(this, "verbosity",
    "Verbosity where lt 0 is Fatal, eq 0 is Error, eq 1 is Info, gt 1 is Debug")
  setDefault(verbosity -> 1)

  def getVerbosity: Int = $(verbosity)
  def setVerbosity(value: Int): this.type = set(verbosity, value)

  val categoricalSlotIndexes = new IntArrayParam(this, "categoricalSlotIndexes",
    "List of categorical column indexes, the slot index in the features column")

  def getCategoricalSlotIndexes: Array[Int] = $(categoricalSlotIndexes)
  def setCategoricalSlotIndexes(value: Array[Int]): this.type = set(categoricalSlotIndexes, value)

  val categoricalSlotNames = new StringArrayParam(this, "categoricalSlotNames",
    "List of categorical column slot names, the slot name in the features column")

  def getCategoricalSlotNames: Array[String] = $(categoricalSlotNames)
  def setCategoricalSlotNames(value: Array[String]): this.type = set(categoricalSlotNames, value)

  val boostFromAverage = new BooleanParam(this, "boostFromAverage",
    "Adjusts initial score to the mean of labels for faster convergence")
  setDefault(boostFromAverage -> true)

  def getBoostFromAverage: Boolean = $(boostFromAverage)
  def setBoostFromAverage(value: Boolean): this.type = set(boostFromAverage, value)

  val boostingType = new Param[String](this, "boostingType",
    "Default gbdt = traditional Gradient Boosting Decision Tree. Options are: " +
    "gbdt, gbrt, rf (Random Forest), random_forest, dart (Dropouts meet Multiple " +
    "Additive Regression Trees), goss (Gradient-based One-Side Sampling). ")
  setDefault(boostingType -> "gbdt")

  def getBoostingType: String = $(boostingType)
  def setBoostingType(value: String): this.type = set(boostingType, value)

  val lambdaL1 = new DoubleParam(this, "lambdaL1", "L1 regularization")
  setDefault(lambdaL1 -> 0.0)

  def getLambdaL1: Double = $(lambdaL1)
  def setLambdaL1(value: Double): this.type = set(lambdaL1, value)

  val lambdaL2 = new DoubleParam(this, "lambdaL2", "L2 regularization")
  setDefault(lambdaL2 -> 0.0)

  def getLambdaL2: Double = $(lambdaL2)
  def setLambdaL2(value: Double): this.type = set(lambdaL2, value)

  val isProvideTrainingMetric = new BooleanParam(this, "isProvideTrainingMetric",
    "Whether output metric result over training dataset.")
  setDefault(isProvideTrainingMetric -> false)

  def getIsProvideTrainingMetric: Boolean = $(isProvideTrainingMetric)
  def setIsProvideTrainingMetric(value: Boolean): this.type = set(isProvideTrainingMetric, value)

  val metric = new Param[String](this, "metric",
    "Metrics to be evaluated on the evaluation data.  Options are: " +
     "empty string or not specified means that metric corresponding to specified " +
      "objective will be used (this is possible only for pre-defined objective functions, " +
      "otherwise no evaluation metric will be added). " +
      "None (string, not a None value) means that no metric will be registered, a" +
      "liases: na, null, custom. " +
      "l1, absolute loss, aliases: mean_absolute_error, mae, regression_l1. " +
      "l2, square loss, aliases: mean_squared_error, mse, regression_l2, regression. " +
      "rmse, root square loss, aliases: root_mean_squared_error, l2_root. " +
      "quantile, Quantile regression. " +
      "mape, MAPE loss, aliases: mean_absolute_percentage_error. " +
      "huber, Huber loss. " +
      "fair, Fair loss. " +
      "poisson, negative log-likelihood for Poisson regression. " +
      "gamma, negative log-likelihood for Gamma regression. " +
      "gamma_deviance, residual deviance for Gamma regression. " +
      "tweedie, negative log-likelihood for Tweedie regression. " +
      "ndcg, NDCG, aliases: lambdarank. " +
      "map, MAP, aliases: mean_average_precision. " +
      "auc, AUC. " +
      "binary_logloss, log loss, aliases: binary. " +
      "binary_error, for one sample: 0 for correct classification, 1 for error classification. " +
      "multi_logloss, log loss for multi-class classification, aliases: multiclass, softmax, " +
      "multiclassova, multiclass_ova, ova, ovr. " +
      "multi_error, error rate for multi-class classification. " +
      "cross_entropy, cross-entropy (with optional linear weights), aliases: xentropy. " +
      "cross_entropy_lambda, intensity-weighted cross-entropy, aliases: xentlambda. " +
      "kullback_leibler, Kullback-Leibler divergence, aliases: kldiv. ")
  setDefault(metric -> "")

  def getMetric: String = $(metric)
  def setMetric(value: String): this.type = set(metric, value)

  val minGainToSplit = new DoubleParam(this, "minGainToSplit",
    "The minimal gain to perform split")
  setDefault(minGainToSplit -> 0.0)

  def getMinGainToSplit: Double = $(minGainToSplit)
  def setMinGainToSplit(value: Double): this.type = set(minGainToSplit, value)

  val maxDeltaStep = new DoubleParam(this, "maxDeltaStep",
    "Used to limit the max output of tree leaves")
  setDefault(maxDeltaStep -> 0.0)

  def getMaxDeltaStep: Double = $(maxDeltaStep)
  def setMaxDeltaStep(value: Double): this.type = set(maxDeltaStep, value)

  val maxBinByFeature = new IntArrayParam(this, "maxBinByFeature",
    "Max number of bins for each feature")
  setDefault(maxBinByFeature -> Array.empty)

  def getMaxBinByFeature: Array[Int] = $(maxBinByFeature)
  def setMaxBinByFeature(value: Array[Int]): this.type = set(maxBinByFeature, value)
}
