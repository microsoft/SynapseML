// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.params

import com.microsoft.azure.synapse.ml.core.utils.ParamsStringBuilder
import com.microsoft.azure.synapse.ml.lightgbm.LightGBMDelegate

/** Defines the Booster parameters passed to the LightGBM ranker.
  */
case class RankerTrainParams(passThroughArgs: Option[String],
                             maxPosition: Int,
                             labelGain: Array[Double],
                             evalAt: Array[Int],
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
    sb.appendParamListIfNotThere("label_gain", labelGain)
      .appendParamListIfNotThere("eval_at", evalAt)
      .appendParamValueIfNotThere("max_position", Option(maxPosition))
  }
}

