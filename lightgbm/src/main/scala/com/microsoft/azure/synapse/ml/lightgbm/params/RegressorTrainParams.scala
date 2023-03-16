// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.params

import com.microsoft.azure.synapse.ml.core.utils.ParamsStringBuilder
import com.microsoft.azure.synapse.ml.lightgbm.LightGBMDelegate

/** Defines the Booster parameters passed to the LightGBM regressor.
  */
case class RegressorTrainParams(passThroughArgs: Option[String],
                                alpha: Double,
                                tweedieVariancePower: Double,
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
    sb.appendParamValueIfNotThere("alpha", Option(alpha))
      .appendParamValueIfNotThere("tweedie_variance_power", Option(tweedieVariancePower))
      .appendParamValueIfNotThere("boost_from_average", Option(boostFromAverage))
  }
}
