// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.params

import com.microsoft.azure.synapse.ml.core.utils.{ParamsStringBuilder, ParamGroup}
import com.microsoft.azure.synapse.ml.lightgbm.LightGBMDelegate

/** Defines the common Booster parameters passed to the LightGBM learners.
  */
abstract class BaseTrainParams extends Serializable {
  def passThroughArgs: Option[String]
  def isProvideTrainingMetric: Option[Boolean]
  def delegate: Option[LightGBMDelegate]
  def generalParams: GeneralParams
  def datasetParams: DatasetParams
  def dartModeParams: DartModeParams
  def executionParams: ExecutionParams
  def objectiveParams: ObjectiveParams
  def seedParams: SeedParams
  def categoricalParams: CategoricalParams

  override def toString: String = {
    // Note: passing `isProvideTrainingMetric` to LightGBM as a config parameter won't work,
    // Fetch and print training metrics in `TrainUtils.scala` through JNI.
    new ParamsStringBuilder(prefix = "", delimiter = "=")
      .append(if (passThroughArgs.isDefined) passThroughArgs.get else "")
      .appendParamValueIfNotThere("is_pre_partition", Option("True"))
      .appendParamGroup(generalParams)
      .appendParamGroup(datasetParams)
      .appendParamGroup(dartModeParams, condition = generalParams.boostingType == "dart")
      .appendParamGroup(objectiveParams)
      .appendParamGroup(executionParams)
      .appendParamGroup(seedParams)
      .appendParamGroup(categoricalParams)
      .appendTrainingSpecificParams()
      .result
  }

  /** Override to add parameters specific to training type.
    */
  def appendSpecializedParams(sb: ParamsStringBuilder): ParamsStringBuilder =
  {
    sb
  }

  implicit class SpecificParamAppender(sb: ParamsStringBuilder) {
    def appendTrainingSpecificParams(): ParamsStringBuilder = {
      appendSpecializedParams(sb)
    }
  }
}

/** Defines the general Booster parameters passed to the LightGBM library.
  */
case class GeneralParams(parallelism: String,
                         topK: Option[Int],
                         numIterations: Int,
                         learningRate: Double,
                         numLeaves: Option[Int],
                         maxBin: Option[Int],
                         binSampleCount: Option[Int],
                         baggingFraction: Option[Double],
                         posBaggingFraction: Option[Double],
                         negBaggingFraction: Option[Double],
                         baggingFreq: Option[Int],
                         baggingSeed: Option[Int],
                         earlyStoppingRound: Int,
                         improvementTolerance: Double,
                         featureFraction: Option[Double],
                         featureFractionByNode: Option[Double],
                         maxDepth: Option[Int],
                         minSumHessianInLeaf: Option[Double],
                         numMachines: Int,
                         modelString: Option[String],
                         categoricalFeatures: Array[Int],
                         verbosity: Int,
                         boostingType: String,
                         lambdaL1: Option[Double],
                         lambdaL2: Option[Double],
                         metric: Option[String],
                         minGainToSplit: Option[Double],
                         maxDeltaStep: Option[Double],
                         maxBinByFeature: Array[Int],
                         minDataPerBin: Option[Int],
                         minDataInLeaf: Option[Int],
                         topRate: Option[Double],
                         otherRate: Option[Double],
                         monotoneConstraints: Array[Int],
                         monotoneConstraintsMethod: Option[String],
                         monotonePenalty: Option[Double],
                         featureNames: Array[String]) extends ParamGroup {
  def appendParams(sb: ParamsStringBuilder): ParamsStringBuilder =
  {
    sb.appendParamValueIfNotThere("is_pre_partition", Option("True"))
      .appendParamValueIfNotThere("boosting_type", Option(boostingType))
      .appendParamValueIfNotThere("tree_learner", Option(parallelism))
      .appendParamValueIfNotThere("top_k", topK)
      .appendParamValueIfNotThere("num_leaves", numLeaves)
      .appendParamValueIfNotThere("max_bin", maxBin)
      .appendParamValueIfNotThere("min_data_per_bin", minDataPerBin)
      .appendParamValueIfNotThere("bagging_fraction", baggingFraction)
      .appendParamValueIfNotThere("pos_bagging_fraction", posBaggingFraction)
      .appendParamValueIfNotThere("neg_bagging_fraction", negBaggingFraction)
      .appendParamValueIfNotThere("bagging_freq", baggingFreq)
      .appendParamValueIfNotThere("feature_fraction", featureFraction)
      .appendParamValueIfNotThere("max_depth", maxDepth)
      .appendParamValueIfNotThere("min_sum_hessian_in_leaf", minSumHessianInLeaf)
      .appendParamValueIfNotThere("lambda_l1", lambdaL1)
      .appendParamValueIfNotThere("lambda_l2", lambdaL2)
      .appendParamValueIfNotThere("metric", metric)
      .appendParamValueIfNotThere("min_gain_to_split", minGainToSplit)
      .appendParamValueIfNotThere("max_delta_step", maxDeltaStep)
      .appendParamValueIfNotThere("min_data_in_leaf", minDataInLeaf)
      .appendParamValueIfNotThere("num_iterations", Option(numIterations))
      .appendParamValueIfNotThere("learning_rate", Option(learningRate))
      .appendParamValueIfNotThere("num_machines", Option(numMachines))
      .appendParamValueIfNotThere("verbosity", Option(verbosity))
      .appendParamValueIfNotThere("early_stopping_round", Option(earlyStoppingRound))
      .appendParamValueIfNotThere("learning_rate", Option(learningRate))
      .appendParamValueIfNotThere("min_data_per_bin", minDataPerBin)
      .appendParamListIfNotThere("categorical_feature", categoricalFeatures)
      .appendParamListIfNotThere("max_bin_by_feature", maxBinByFeature)
      .appendParamValueIfNotThere("top_rate", topRate)
      .appendParamValueIfNotThere("other_rate", otherRate)
      .appendParamListIfNotThere("monotone_constraints", monotoneConstraints)
      .appendParamValueIfNotThere("monotone_constraints_method", monotoneConstraintsMethod)
      .appendParamValueIfNotThere("monotone_penalty", monotonePenalty)
      .appendParamValueIfNotThere("learning_rate", Option(learningRate))
  }
}

/** Defines the Dataset parameters passed to the LightGBM classifier.
  *
  * @param isEnableSparse Used to enable/disable sparse optimization.
  * @param useMissing Set this to false to disable the special handle of missing value.
  * @param zeroAsMissing Set to true to treat all zero as missing values
  *                         (including the unshown values in LibSVM/sparse matrices)
  *                      Set to false to use na for representing missing values.
  */
case class DatasetParams(isEnableSparse: Option[Boolean],
                         useMissing: Option[Boolean],
                         zeroAsMissing: Option[Boolean]) extends ParamGroup {
  def appendParams(sb: ParamsStringBuilder): ParamsStringBuilder =
  {
    sb.appendParamValueIfNotThere("is_enable_sparse", isEnableSparse)
      .appendParamValueIfNotThere("use_missing", useMissing)
      .appendParamValueIfNotThere("zero_as_missing", zeroAsMissing)
  }
}

/** Defines the dart mode parameters passed to the LightGBM learners.
  */
case class DartModeParams(dropRate: Double,
                          maxDrop: Int,
                          skipDrop: Double,
                          xgboostDartMode: Boolean,
                          uniformDrop: Boolean) extends ParamGroup {
  def appendParams(sb: ParamsStringBuilder): ParamsStringBuilder = {
    sb.appendParamValueIfNotThere("drop_rate", Option(dropRate))
      .appendParamValueIfNotThere("max_drop", Option(maxDrop))
      .appendParamValueIfNotThere("skip_drop", Option(skipDrop))
      .appendParamValueIfNotThere("xgboost_dart_mode", Option(xgboostDartMode))
      .appendParamValueIfNotThere("uniform_drop", Option(uniformDrop))
  }
}

/** Defines parameters related to lightgbm execution in spark.
  *
  * @param chunkSize Advanced parameter to specify the chunk size for copying Java data to native.
  * @param matrixType Advanced parameter to specify whether the native lightgbm matrix
  *                   constructed should be sparse or dense.
  * @param numThreads The number of threads to run the native lightgbm training with on each worker.
  * @param executionMode How to execute the LightGBM training.
  * @param microBatchSize The number of elements in a streaming micro-batch.
  * @param useSingleDatasetMode Whether to create only 1 LightGBM Dataset on each worker.
  */
case class ExecutionParams(chunkSize: Int,
                           matrixType: String,
                           numThreads: Int,
                           executionMode: String,
                           microBatchSize: Int,
                           useSingleDatasetMode: Boolean) extends ParamGroup {
  def appendParams(sb: ParamsStringBuilder): ParamsStringBuilder = {
    sb.appendParamValueIfNotThere("num_threads", Option(numThreads))
  }
}

/** Defines parameters related to the lightgbm objective function.
  *
  * @param objective The Objective. For regression applications, this can be:
  *                  regression_l2, regression_l1, huber, fair, poisson, quantile, mape, gamma or tweedie.
  *                  For classification applications, this can be: binary, multiclass, or multiclassova.
  * @param fobj      Customized objective function.
  *                  Should accept two parameters: preds, train_data, and return (grad, hess).
  */
case class ObjectiveParams(objective: String,
                           fobj: Option[FObjTrait]) extends ParamGroup {
  def appendParams(sb: ParamsStringBuilder): ParamsStringBuilder = {
      sb.appendParamValueIfNotThere("objective", Option(if (fobj.isEmpty) objective else "custom"))
  }
}

/** Defines parameters related to seed and determinism for lightgbm.
  *
  * @param seed                Main seed, used to generate other seeds.
  *
  * @param deterministic       Setting this to true should ensure stable results when using the
  *                            same data and the same parameters.
  * @param baggingSeed         Bagging seed.
  * @param featureFractionSeed Feature fraction seed.
  * @param extraSeed           Random seed for selecting threshold when extra_trees is true.
  * @param dropSeed            Random seed to choose dropping models. Only used in dart.
  * @param dataRandomSeed      Random seed for sampling data to construct histogram bins.
  * @param objectiveSeed       Random seed for objectives, if random process is needed.
  *                            Currently used only for rank_xendcg objective.
  * @param boostingType        Boosting type, used to determine if drop seed should be set.
  * @param objective           Objective, used to determine if objective seed should be set.
  */
case class SeedParams(seed: Option[Int],
                      deterministic: Option[Boolean],
                      baggingSeed: Option[Int],
                      featureFractionSeed: Option[Int],
                      extraSeed: Option[Int],
                      dropSeed: Option[Int],
                      dataRandomSeed: Option[Int],
                      objectiveSeed: Option[Int],
                      boostingType: String,
                      objective: String) extends ParamGroup {
  def appendParams(sb: ParamsStringBuilder): ParamsStringBuilder = {
    sb.appendParamValueIfNotThere("seed", seed)
      .appendParamValueIfNotThere("deterministic", deterministic)
      .appendParamValueIfNotThere("bagging_seed", baggingSeed)
      .appendParamValueIfNotThere("feature_fraction_seed", featureFractionSeed)
      .appendParamValueIfNotThere("extra_seed", extraSeed)
      .appendParamValueIfNotThere("data_random_seed", dataRandomSeed)
      .appendParamValueIfNotThere("drop_seed", if (boostingType == "dart") dropSeed else None)
      .appendParamValueIfNotThere("objective_seed", if (objective == "rank_xendcg") objectiveSeed else None)
  }
}

/** Defines parameters related to categorical features for lightgbm.
  *
  * @param minDataPerGroup    minimal number of data per categorical group.
  * @param maxCatThreshold    limit number of split points considered for categorical features
  * @param catl2              L2 regularization in categorical split.
  * @param catSmooth          this can reduce the effect of noises in categorical features,
  *                           especially for categories with few data.
  * @param maxCatToOneHot     when number of categories of one feature smaller than or equal to this,
  *                           one-vs-other split algorithm will be used.
  */
case class CategoricalParams(minDataPerGroup: Option[Int],
                             maxCatThreshold: Option[Int],
                             catl2: Option[Double],
                             catSmooth: Option[Double],
                             maxCatToOneHot: Option[Int]) extends ParamGroup {
  def appendParams(sb: ParamsStringBuilder): ParamsStringBuilder = {
    sb.appendParamValueIfNotThere("min_data_per_group", minDataPerGroup)
      .appendParamValueIfNotThere("max_cat_threshold", maxCatThreshold)
      .appendParamValueIfNotThere("cat_l2", catl2)
      .appendParamValueIfNotThere("cat_smooth", catSmooth)
      .appendParamValueIfNotThere("max_cat_to_onehot", maxCatToOneHot)
  }
}
