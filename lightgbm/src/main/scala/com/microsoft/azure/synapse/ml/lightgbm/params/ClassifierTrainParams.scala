// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.params

import com.microsoft.azure.synapse.ml.core.utils.ParamsStringBuilder
import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMConstants, LightGBMDelegate}

/** Defines the Booster parameters passed to the LightGBM classifier.
  */
case class ClassifierTrainParams(passThroughArgs: Option[String],
                                 isUnbalance: Boolean,
                                 numClass: Int,
                                 boostFromAverage: Boolean,
                                 isProvideTrainingMetric: Option[Boolean],
                                 delegate: Option[LightGBMDelegate],
                                 generalParams: GeneralParams,
                                 datasetParams: DatasetParams,
                                 dartModeParams: DartModeParams,
                                 executionParams: ExecutionParams,
                                 objectiveParams: ObjectiveParams,
                                 seedParams: SeedParams,
                                 categoricalParams: CategoricalParams) extends BaseTrainParams {
  override def appendSpecializedParams(sb: ParamsStringBuilder): ParamsStringBuilder =
  {
    val isBinary = objectiveParams.objective == LightGBMConstants.BinaryObjective
    sb.appendParamValueIfNotThere("num_class", if (!isBinary) Option(numClass) else None)
      .appendParamValueIfNotThere("is_unbalance", if (isBinary) Option(isUnbalance) else None)
      .appendParamValueIfNotThere("boost_from_average", Option(boostFromAverage))
  }
}
