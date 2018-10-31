// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.{Model, PipelineModel, Transformer}
import org.apache.spark.ml.param.ParamMap

/** Defines common inheritance and functions across auto trained models.
  */
abstract class AutoTrainedModel[TrainedModel <: Model[TrainedModel]](val model: PipelineModel)
  extends Model[TrainedModel] with ConstructorWritable[TrainedModel] {
  /** Retrieve the param map from the underlying model.
    * @return The param map from the underlying model.
    */
  def getParamMap: ParamMap = model.stages.last.extractParamMap()

  /** Retrieve the underlying model.
    * @return The underlying model.
    */
  def getModel: Transformer = model.stages.last
}
