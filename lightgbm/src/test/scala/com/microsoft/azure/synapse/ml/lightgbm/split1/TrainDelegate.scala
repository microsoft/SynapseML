// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.lightgbm.params.{FObjTrait, BaseTrainParams}
import com.microsoft.azure.synapse.ml.lightgbm._
import org.slf4j.Logger

@SerialVersionUID(100L)
class TrainDelegate extends LightGBMDelegate {
  override def getLearningRate(batchIndex: Int,
                               partitionId: Int,
                               curIters: Int,
                               log: Logger,
                               trainParams: BaseTrainParams,
                               previousLearningRate: Double): Double = {
    if (curIters == 0) {
      previousLearningRate
    } else {
      previousLearningRate * 0.05
    }
  }
}
