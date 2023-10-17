// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.params

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts.{HasInitScoreCol, HasValidationIndicatorCol, HasWeightCol}
import com.microsoft.azure.synapse.ml.lightgbm.booster.LightGBMBooster
import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMConstants, LightGBMDelegate}
import com.microsoft.azure.synapse.ml.param.ByteArrayParam
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.DefaultParamsWritable

/** Defines common LightGBM execution parameters.
  */
trait LightGBMExecutionParams extends Wrappable {
  val passThroughArgs = new Param[String](this, "passThroughArgs",
    "Direct string to pass through to LightGBM library (appended with other explicitly set params). " +
      "Will override any parameters given with explicit setters. Can include multiple parameters in one string. " +
      "e.g., force_row_wise=true")
  setDefault(passThroughArgs->"")
  def getPassThroughArgs: String = $(passThroughArgs)
  def setPassThroughArgs(value: String): this.type = set(passThroughArgs, value)

  val parallelism = new Param[String](this, "parallelism",
    "Tree learner parallelism, can be set to data_parallel or voting_parallel")
  setDefault(parallelism->"data_parallel")
  def getParallelism: String = $(parallelism)
  def setParallelism(value: String): this.type = set(parallelism, value)

  val topK = new IntParam(this, "topK",
    "The top_k value used in Voting parallel, " +
      "set this to larger value for more accurate result, but it will slow down the training speed. " +
      "It should be greater than 0")
  setDefault(topK -> LightGBMConstants.DefaultTopK)
  def getTopK: Int = $(topK)
  def setTopK(value: Int): this.type = set(topK, value)

  val defaultListenPort = new IntParam(this, "defaultListenPort",
    "The default listen port on executors, used for testing")
  setDefault(defaultListenPort -> LightGBMConstants.DefaultLocalListenPort)
  def getDefaultListenPort: Int = $(defaultListenPort)
  def setDefaultListenPort(value: Int): this.type = set(defaultListenPort, value)

  val driverListenPort = new IntParam(this, "driverListenPort",
    "The listen port on a driver. Default value is 0 (random)")
  setDefault(driverListenPort -> LightGBMConstants.DefaultDriverListenPort)
  def getDriverListenPort: Int = $(driverListenPort)
  def setDriverListenPort(value: Int): this.type = set(driverListenPort, value)

  val timeout = new DoubleParam(this, "timeout", "Timeout in seconds")
  setDefault(timeout -> 1200)
  def getTimeout: Double = $(timeout)
  def setTimeout(value: Double): this.type = set(timeout, value)

  val useBarrierExecutionMode = new BooleanParam(this, "useBarrierExecutionMode",
    "Barrier execution mode which uses a barrier stage, off by default.")
  setDefault(useBarrierExecutionMode -> false)
  def getUseBarrierExecutionMode: Boolean = $(useBarrierExecutionMode)
  def setUseBarrierExecutionMode(value: Boolean): this.type = set(useBarrierExecutionMode, value)

  val samplingMode = new Param[String](this, "samplingMode",
    "Data sampling for streaming mode. Sampled data is used to define bins. " +
      "'global': sample from all data, 'subset': sample from first N rows, or 'fixed': Take first N rows as sample." +
      "Values can be global, subset, or fixed. Default is subset.")
  setDefault(samplingMode -> LightGBMConstants.SubsetSamplingModeSubset)
  def getSamplingMode: String = $(samplingMode)
  def setSamplingMode(value: String): this.type = set(samplingMode, value)

  val samplingSubsetSize = new IntParam(this, "samplingSubsetSize",
    "Specify subset size N for the sampling mode 'subset'. 'binSampleCount' rows will be chosen from " +
      "the first N values of the dataset. Subset can be used when rows are expected to be random and data is huge.")
  setDefault(samplingSubsetSize -> 1000000)
  def getSamplingSubsetSize: Int = $(samplingSubsetSize)
  def setSamplingSubsetSize(value: Int): this.type = set(samplingSubsetSize, value)

  val referenceDataset: ByteArrayParam = new ByteArrayParam(
    this,
    "referenceDataset",
    "The reference Dataset that was used for the fit. If using samplingMode=custom, this must be set before fit()."
  )
  setDefault(referenceDataset -> Array.empty[Byte])
  def getReferenceDataset: Array[Byte] = $(referenceDataset)
  def setReferenceDataset(value: Array[Byte]): this.type = set(referenceDataset, value)

  @deprecated("Please use 'dataTransferMode'", since = "0.11.1")
  val executionMode = new Param[String](this, "executionMode",
    "Deprecated. Please use dataTransferMode.")
  @deprecated("Please use 'setDataTransferMode'", since = "0.11.1")
  def setExecutionMode(value: String): this.type = set(dataTransferMode, value)

  val dataTransferMode = new Param[String](this, "dataTransferMode",
    "Specify how SynapseML transfers data from Spark to LightGBM.  " +
      "Values can be streaming, bulk. Default is bulk, which is the legacy mode.")
  setDefault(dataTransferMode -> LightGBMConstants.StreamingDataTransferMode)
  def getDataTransferMode: String = $(dataTransferMode)
  def setDataTransferMode(value: String): this.type = set(dataTransferMode, value)

  val microBatchSize = new IntParam(this, "microBatchSize",
    "Specify how many elements are sent in a streaming micro-batch.")
  setDefault(microBatchSize -> 100)
  def getMicroBatchSize: Int = $(microBatchSize)
  def setMicroBatchSize(value: Int): this.type = set(microBatchSize, value)

  val useSingleDatasetMode = new BooleanParam(this, "useSingleDatasetMode",
    "Use single dataset execution mode to create a single native dataset per executor (singleton) " +
      "to reduce memory and communication overhead.")
  setDefault(useSingleDatasetMode -> true)
  def getUseSingleDatasetMode: Boolean = $(useSingleDatasetMode)
  def setUseSingleDatasetMode(value: Boolean): this.type = set(useSingleDatasetMode, value)

  val numBatches = new IntParam(this, "numBatches",
    "If greater than 0, splits data into separate batches during training")
  setDefault(numBatches -> 0)
  def getNumBatches: Int = $(numBatches)
  def setNumBatches(value: Int): this.type = set(numBatches, value)

  val repartitionByGroupingColumn = new BooleanParam(this, "repartitionByGroupingColumn",
    "Repartition training data according to grouping column, on by default.")
  setDefault(repartitionByGroupingColumn -> true)
  def getRepartitionByGroupingColumn: Boolean = $(repartitionByGroupingColumn)
  def setRepartitionByGroupingColumn(value: Boolean): this.type = set(repartitionByGroupingColumn, value)

  val numTasks = new IntParam(this, "numTasks",
    "Advanced parameter to specify the number of tasks.  " +
      "SynapseML tries to guess this based on cluster configuration, but this parameter can be used to override.")
  setDefault(numTasks -> 0)
  def getNumTasks: Int = $(numTasks)
  def setNumTasks(value: Int): this.type = set(numTasks, value)

  val chunkSize = new IntParam(this, "chunkSize",
    "Advanced parameter to specify the chunk size for copying Java data to native.  " +
      "If set too high, memory may be wasted, but if set too low, performance may be reduced during data copy." +
      "If dataset size is known beforehand, set to the number of rows in the dataset.")
  setDefault(chunkSize -> 10000)
  def getChunkSize: Int = $(chunkSize)
  def setChunkSize(value: Int): this.type = set(chunkSize, value)

  val matrixType = new Param[String](this, "matrixType",
    "Advanced parameter to specify whether the native lightgbm matrix constructed should be sparse or dense.  " +
      "Values can be auto, sparse or dense. Default value is auto, which samples first ten rows to determine type.")
  setDefault(matrixType -> "auto")
  def getMatrixType: String = $(matrixType)
  def setMatrixType(value: String): this.type = set(matrixType, value)

  val numThreads = new IntParam(this, "numThreads",
    "Number of threads per executor for LightGBM. For the best speed, set this to the number of real CPU cores.")
  setDefault(numThreads -> 0)
  def getNumThreads: Int = $(numThreads)
  def setNumThreads(value: Int): this.type = set(numThreads, value)

  val maxStreamingOMPThreads = new IntParam(this,
    "maxStreamingOMPThreads",
    "Maximum number of OpenMP threads used by a LightGBM thread. Used only for thread-safe buffer allocation." +
      " Use -1 to use OpenMP default, but in a Spark environment it's best to set a fixed value.")
  setDefault(maxStreamingOMPThreads -> 16)
  def getMaxStreamingOMPThreads: Int = $(maxStreamingOMPThreads)
  def setMaxStreamingOMPThreads(value: Int): this.type = set(maxStreamingOMPThreads, value)
}

/** Defines common parameters across all LightGBM learners related to dataset handling.
  */
trait LightGBMDatasetParams extends Wrappable {
  val isEnableSparse = new BooleanParam(this, "isEnableSparse", "Used to enable/disable sparse optimization")
  setDefault(isEnableSparse -> true)
  def getIsEnableSparse: Boolean = $(isEnableSparse)
  def setIsEnableSparse(value: Boolean): this.type = set(isEnableSparse, value)

  val useMissing = new BooleanParam(this,
    "useMissing",
    "Set this to false to disable the special handle of missing value")
  setDefault(useMissing -> true)
  def getUseMissing: Boolean = $(useMissing)
  def setUseMissing(value: Boolean): this.type = set(useMissing, value)

  val zeroAsMissing = new BooleanParam(this,
    "zeroAsMissing",
    "Set to true to treat all zero as missing values (including the unshown values in LibSVM / sparse matrices)." +
    " Set to false to use na for representing missing values")
  setDefault(zeroAsMissing -> false)
  def getZeroAsMissing: Boolean = $(zeroAsMissing)
  def setZeroAsMissing(value: Boolean): this.type = set(zeroAsMissing, value)
}

/** Defines common parameters across all LightGBM learners related to learning score evolution.
  */
trait LightGBMLearnerParams extends Wrappable {
  val earlyStoppingRound = new IntParam(this, "earlyStoppingRound", "Early stopping round")
  setDefault(earlyStoppingRound -> 0)
  def getEarlyStoppingRound: Int = $(earlyStoppingRound)
  def setEarlyStoppingRound(value: Int): this.type = set(earlyStoppingRound, value)

  val improvementTolerance = new DoubleParam(this, "improvementTolerance",
    "Tolerance to consider improvement in metric")
  setDefault(improvementTolerance -> 0.0)
  def getImprovementTolerance: Double = $(improvementTolerance)
  def setImprovementTolerance(value: Double): this.type = set(improvementTolerance, value)

  val monotoneConstraints = new IntArrayParam(this, "monotoneConstraints",
    "used for constraints of monotonic features. 1 means increasing, -1 means decreasing, 0 means non-constraint." +
    " Specify all features in order.")
  setDefault(monotoneConstraints -> Array.empty)
  def getMonotoneConstraints: Array[Int] = $(monotoneConstraints)
  def setMonotoneConstraints(value: Array[Int]): this.type = set(monotoneConstraints, value)

  val monotoneConstraintsMethod = new Param[String](this, "monotoneConstraintsMethod",
    "Monotone constraints method. basic, intermediate, or advanced.")
  setDefault(monotoneConstraintsMethod -> "basic")
  def getMonotoneConstraintsMethod: String = $(monotoneConstraintsMethod)
  def setMonotoneConstraintsMethod(value: String): this.type = set(monotoneConstraintsMethod, value)

  val monotonePenalty = new DoubleParam(this, "monotonePenalty",
    "A penalization parameter X forbids any monotone splits on the first X (rounded down) level(s) of the tree.")
  setDefault(monotonePenalty -> 0.0)
  def getMonotonePenalty: Double = $(monotonePenalty)
  def setMonotonePenalty(value: Double): this.type = set(monotonePenalty, value)

  val topRate = new DoubleParam(this, "topRate", "The retain ratio of large gradient data. Only used in goss.")
  setDefault(topRate -> 0.2)
  def getTopRate: Double = $(topRate)
  def setTopRate(value: Double): this.type = set(topRate, value)

  val otherRate = new DoubleParam(this, "otherRate","The retain ratio of small gradient data. Only used in goss.")
  setDefault(otherRate -> 0.1)
  def getOtherRate: Double = $(otherRate)
  def setOtherRate(value: Double): this.type = set(otherRate, value)
}

/** Defines common parameters across all LightGBM learners related to histogram bin construction.
  */
trait LightGBMBinParams extends Wrappable {
  val maxBin = new IntParam(this, "maxBin", "Max bin")
  setDefault(maxBin -> 255)
  def getMaxBin: Int = $(maxBin)
  def setMaxBin(value: Int): this.type = set(maxBin, value)

  val binSampleCount = new IntParam(this, "binSampleCount", "Number of samples considered at computing histogram bins")
  setDefault(binSampleCount -> 200000)
  def getBinSampleCount: Int = $(binSampleCount)
  def setBinSampleCount(value: Int): this.type = set(binSampleCount, value)
}

/** Defines parameters for dart mode across all LightGBM learners.
 */
trait LightGBMDartParams extends Wrappable {
  val dropRate = new DoubleParam(this, "dropRate",
    "Dropout rate: a fraction of previous trees to drop during the dropout")
  setDefault(dropRate -> 0.1)
  def getDropRate: Double = $(dropRate)
  def setDropRate(value: Double): this.type = set(dropRate, value)

  val maxDrop = new IntParam(this, "maxDrop",
    "Max number of dropped trees during one boosting iteration")
  setDefault(maxDrop -> 50)
  def getMaxDrop: Int = $(maxDrop)
  def setMaxDrop(value: Int): this.type = set(maxDrop, value)

  val skipDrop = new DoubleParam(this, "skipDrop",
    "Probability of skipping the dropout procedure during a boosting iteration")
  setDefault(skipDrop -> 0.5)
  def getSkipDrop: Double = $(skipDrop)
  def setSkipDrop(value: Double): this.type = set(skipDrop, value)

  val xGBoostDartMode = new BooleanParam(this, "xGBoostDartMode",
    "Set this to true to use xgboost dart mode")
  setDefault(xGBoostDartMode -> false)
  def getXGBoostDartMode: Boolean = $(xGBoostDartMode)
  def setXGBoostDartMode(value: Boolean): this.type = set(xGBoostDartMode, value)

  val uniformDrop = new BooleanParam(this, "uniformDrop",
    "Set this to true to use uniform drop in dart mode")
  setDefault(uniformDrop -> false)
  def getUniformDrop: Boolean = $(uniformDrop)
  def setUniformDrop(value: Boolean): this.type = set(uniformDrop, value)
}

/** Defines parameters for slots across all LightGBM learners.
 */
trait LightGBMSlotParams extends Wrappable {
  val slotNames = new StringArrayParam(this, "slotNames",
    "List of slot names in the features column")
  setDefault(slotNames -> Array.empty)
  def getSlotNames: Array[String] = $(slotNames)
  def setSlotNames(value: Array[String]): this.type = set(slotNames, value)

  val categoricalSlotIndexes = new IntArrayParam(this, "categoricalSlotIndexes",
    "List of categorical column indexes, the slot index in the features column")
  def getCategoricalSlotIndexes: Array[Int] = $(categoricalSlotIndexes)
  def setCategoricalSlotIndexes(value: Array[Int]): this.type = set(categoricalSlotIndexes, value)

  setDefault(categoricalSlotIndexes -> Array.empty)
  val categoricalSlotNames = new StringArrayParam(this, "categoricalSlotNames",
    "List of categorical column slot names, the slot name in the features column")
  setDefault(categoricalSlotNames -> Array.empty)
  def getCategoricalSlotNames: Array[String] = $(categoricalSlotNames)
  def setCategoricalSlotNames(value: Array[String]): this.type = set(categoricalSlotNames, value)

}

/** Defines parameters for fraction across all LightGBM learners.
  */
trait LightGBMFractionParams extends Wrappable {
  val baggingFraction = new DoubleParam(this, "baggingFraction", "Bagging fraction")
  setDefault(baggingFraction->1)
  def getBaggingFraction: Double = $(baggingFraction)
  def setBaggingFraction(value: Double): this.type = set(baggingFraction, value)

  val posBaggingFraction = new DoubleParam(this, "posBaggingFraction", "Positive Bagging fraction")
  setDefault(posBaggingFraction->1)
  def getPosBaggingFraction: Double = $(posBaggingFraction)
  def setPosBaggingFraction(value: Double): this.type = set(posBaggingFraction, value)

  val negBaggingFraction = new DoubleParam(this, "negBaggingFraction", "Negative Bagging fraction")
  setDefault(negBaggingFraction->1)
  def getNegBaggingFraction: Double = $(negBaggingFraction)
  def setNegBaggingFraction(value: Double): this.type = set(negBaggingFraction, value)

  val featureFraction = new DoubleParam(this, "featureFraction", "Feature fraction")
  setDefault(featureFraction->1)
  def getFeatureFraction: Double = $(featureFraction)
  def setFeatureFraction(value: Double): this.type = set(featureFraction, value)

  val featureFractionByNode = new DoubleParam(this, "featureFractionByNode", "Feature fraction by node")
  setDefault(featureFraction->1)
  def getFeatureFractionByNode: Double = $(featureFractionByNode)
  def setFeatureFractionByNode(value: Double): this.type = set(featureFractionByNode, value)
}

/** Defines common prediction parameters across LightGBM Ranker, Classifier and Regressor
  */
trait LightGBMPredictionParams extends Wrappable {
  val leafPredictionCol = new Param[String](this, "leafPredictionCol",
    "Predicted leaf indices's column name")
  setDefault(leafPredictionCol -> "")
  def getLeafPredictionCol: String = $(leafPredictionCol)
  def setLeafPredictionCol(value: String): this.type = set(leafPredictionCol, value)

  val featuresShapCol = new Param[String](this, "featuresShapCol",
    "Output SHAP vector column name after prediction containing the feature contribution values")
  setDefault(featuresShapCol -> "")
  def getFeaturesShapCol: String = $(featuresShapCol)
  def setFeaturesShapCol(value: String): this.type = set(featuresShapCol, value)

  val predictDisableShapeCheck = new BooleanParam(this, "predictDisableShapeCheck",
    "control whether or not LightGBM raises an error " +
      "when you try to predict on data with a different number of features than the training data")
  setDefault(predictDisableShapeCheck -> false)
  def getPredictDisableShapeCheck: Boolean = $(predictDisableShapeCheck)
  def setPredictDisableShapeCheck(value: Boolean): this.type = set(predictDisableShapeCheck, value)
}

/** Defines parameters for LightGBM models
  */
trait LightGBMModelParams extends Wrappable {
  val lightGBMBooster = new LightGBMBoosterParam(this, "lightGBMBooster",
    "The trained LightGBM booster")
  def getLightGBMBooster: LightGBMBooster = $(lightGBMBooster)
  def setLightGBMBooster(value: LightGBMBooster): this.type = set(lightGBMBooster, value)

  /**
    * Alias for same method
    * @return The LightGBM Booster.
    */
  def getModel: LightGBMBooster = this.getLightGBMBooster

  val startIteration = new IntParam(this, "startIteration",
    "Sets the start index of the iteration to predict. If <= 0, starts from the first iteration.")
  setDefault(startIteration -> LightGBMConstants.DefaultStartIteration)
  def getStartIteration: Int = $(startIteration)
  def setStartIteration(value: Int): this.type = set(startIteration, value)

  val numIterations = new IntParam(this, "numIterations",
    "Sets the total number of iterations used in the prediction." +
      "If <= 0, all iterations from ``start_iteration`` are used (no limits).")
  setDefault(numIterations -> LightGBMConstants.DefaultNumIterations)
  def getNumIterations: Int = $(numIterations)
  def setNumIterations(value: Int): this.type = set(numIterations, value)
}

/** Defines common objective parameters
  */
trait LightGBMObjectiveParams extends Wrappable {
  val objective = new Param[String](this, "objective",
    "The Objective. For regression applications, this can be: " +
      "regression_l2, regression_l1, huber, fair, poisson, quantile, mape, gamma or tweedie. " +
      "For classification applications, this can be: binary, multiclass, or multiclassova. ")
  setDefault(objective -> "regression")
  def getObjective: String = $(objective)
  def setObjective(value: String): this.type = set(objective, value)

  val fobj = new FObjParam(this, "fobj", "Customized objective function. " +
    "Should accept two parameters: preds, train_data, and return (grad, hess).")
  def getFObj: FObjTrait = $(fobj)
  def setFObj(value: FObjTrait): this.type = set(fobj, value)
}

/** Defines common parameters related to seed and determinism
  */
trait LightGBMSeedParams extends Wrappable {
  val seed = new IntParam(this, "seed", "Main seed, used to generate other seeds")
  def getSeed: Int = $(seed)
  def setSeed(value: Int): this.type = set(seed, value)

  val deterministic = new BooleanParam(this, "deterministic", "Used only with cpu " +
    "devide type. Setting this to true should ensure stable results when using the same data and the " +
    "same parameters.  Note: setting this to true may slow down training.  To avoid potential instability " +
    "due to numerical issues, please set force_col_wise=true or force_row_wise=true when setting " +
    "deterministic=true")
  setDefault(deterministic->false)
  def getDeterministic: Boolean = $(deterministic)
  def setDeterministic(value: Boolean): this.type = set(deterministic, value)

  val baggingSeed = new IntParam(this, "baggingSeed", "Bagging seed")
  setDefault(baggingSeed->3)
  def getBaggingSeed: Int = $(baggingSeed)
  def setBaggingSeed(value: Int): this.type = set(baggingSeed, value)

  val featureFractionSeed = new IntParam(this, "featureFractionSeed", "Feature fraction seed")
  setDefault(featureFractionSeed->2)
  def getFeatureFractionSeed: Int = $(featureFractionSeed)
  def setFeatureFractionSeed(value: Int): this.type = set(featureFractionSeed, value)

  val extraSeed = new IntParam(this, "extraSeed", "Random seed for selecting threshold " +
    "when extra_trees is true")
  setDefault(extraSeed->6)
  def getExtraSeed: Int = $(extraSeed)
  def setExtraSeed(value: Int): this.type = set(extraSeed, value)

  val dropSeed = new IntParam(this, "dropSeed", "Random seed to choose dropping models. " +
    "Only used in dart.")
  setDefault(dropSeed->4)
  def getDropSeed: Int = $(dropSeed)
  def setDropSeed(value: Int): this.type = set(dropSeed, value)

  val dataRandomSeed = new IntParam(this, "dataRandomSeed", "Random seed for sampling " +
    "data to construct histogram bins.")
  setDefault(dataRandomSeed->1)
  def getDataRandomSeed: Int = $(dataRandomSeed)
  def setDataRandomSeed(value: Int): this.type = set(dataRandomSeed, value)

  val objectiveSeed = new IntParam(this, "objectiveSeed", "Random seed for objectives, " +
    "if random process is needed.  Currently used only for rank_xendcg objective.")
  setDefault(objectiveSeed->5)
  def getObjectiveSeed: Int = $(objectiveSeed)
  def setObjectiveSeed(value: Int): this.type = set(objectiveSeed, value)
}

/** Defines common parameters across all LightGBM learners related to categorical variable treatment.
  */
trait LightGBMCategoricalParams extends Wrappable {
  val minDataPerGroup = new IntParam(this, "minDataPerGroup", "minimal number of data per categorical group")
  setDefault(minDataPerGroup -> 100)
  def getMinDataPerGroup: Int = $(minDataPerGroup)
  def setMinDataPerGroup(value: Int): this.type = set(minDataPerGroup, value)

  val maxCatThreshold = new IntParam(
    this,
    "maxCatThreshold",
    "limit number of split points considered for categorical features")
  setDefault(maxCatThreshold -> 32)
  def getMaxCatThreshold: Int = $(maxCatThreshold)
  def setMaxCatThreshold(value: Int): this.type = set(maxCatThreshold, value)

  val catl2 = new DoubleParam(this, "catl2", "L2 regularization in categorical split")
  setDefault(catl2 -> 10.0)
  def getCatl2: Double = $(catl2)
  def setCatl2(value: Double): this.type = set(catl2, value)

  val catSmooth = new DoubleParam(
    this,
    "catSmooth",
    "this can reduce the effect of noises in categorical features, especially for categories with few data")
  setDefault(catSmooth -> 10.0)
  def getCatSmooth: Double = $(catSmooth)
  def setCatSmooth(value: Double): this.type = set(catSmooth, value)

  val maxCatToOnehot = new IntParam(
    this,
    "maxCatToOnehot",
    "when number of categories of one feature smaller than or equal to this, one-vs-other split algorithm will be used")
  setDefault(maxCatToOnehot -> 4)
  def getMaxCatToOnehot: Int = $(maxCatToOnehot)
  def setMaxCatToOnehot(value: Int): this.type = set(maxCatToOnehot, value)
}

/** Defines common parameters across all LightGBM learners.
  */
trait LightGBMParams extends Wrappable
  with DefaultParamsWritable
  with HasWeightCol
  with HasValidationIndicatorCol
  with HasInitScoreCol
  with LightGBMExecutionParams
  with LightGBMSlotParams
  with LightGBMFractionParams
  with LightGBMBinParams
  with LightGBMLearnerParams
  with LightGBMDatasetParams
  with LightGBMDartParams
  with LightGBMPredictionParams
  with LightGBMObjectiveParams
  with LightGBMSeedParams
  with LightGBMCategoricalParams {

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

  val baggingFreq = new IntParam(this, "baggingFreq", "Bagging frequency")
  setDefault(baggingFreq->0)
  def getBaggingFreq: Int = $(baggingFreq)
  def setBaggingFreq(value: Int): this.type = set(baggingFreq, value)

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
  setDefault(verbosity -> -1)
  def getVerbosity: Int = $(verbosity)
  def setVerbosity(value: Int): this.type = set(verbosity, value)

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

  val minDataPerBin = new IntParam(this, "minDataPerBin",
    "Minimal number of data inside one bin")
  setDefault(minDataPerBin -> 3)
  def getMinDataPerBin: Int = $(minDataPerBin)
  def setMinDataPerBin(value: Int): this.type = set(minDataPerBin, value)

  val minDataInLeaf = new IntParam(this, "minDataInLeaf",
    "Minimal number of data in one leaf. Can be used to deal with over-fitting.")
  setDefault(minDataInLeaf -> 20)
  def getMinDataInLeaf: Int = $(minDataInLeaf)
  def setMinDataInLeaf(value: Int): this.type = set(minDataInLeaf, value)

  val maxNumClasses = new IntParam(this, "maxNumClasses",
    "Number of max classes to infer numClass in multi-class classification.")
  setDefault(maxNumClasses -> 100)
  def getMaxNumClasses: Int = $(maxNumClasses)
  def setMaxNumClasses(value: Int): this.type = set(maxNumClasses, value)

  var delegate: Option[LightGBMDelegate] = None
  def getDelegate: Option[LightGBMDelegate] = delegate
  def setDelegate(delegate: LightGBMDelegate): this.type = {
    this.delegate = Option(delegate)
    this
  }
}
