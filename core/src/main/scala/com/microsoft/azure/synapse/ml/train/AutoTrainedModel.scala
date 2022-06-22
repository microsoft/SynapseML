// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.train

import com.microsoft.azure.synapse.ml.core.contracts.{HasFeaturesCol, HasLabelCol}
import com.microsoft.azure.synapse.ml.param.TransformerParam
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.{ComplexParamsWritable, Model, PipelineModel, Transformer}

/** Defines common inheritance and functions across auto trained models.
  */
abstract class AutoTrainedModel[TrainedModel <: Model[TrainedModel]]
  extends Model[TrainedModel] with ComplexParamsWritable with HasLabelCol with HasFeaturesCol{

  private def validate(t: Transformer): Boolean = {
    t match {
      case _: PipelineModel => true
      case _ => false
    }
  }

  val model = new TransformerParam(this, "model", "model produced by training", validate)

  def getModel: PipelineModel = $(model).asInstanceOf[PipelineModel]

  def setModel(v: PipelineModel): this.type = set(model, v)

  /** Retrieve the param map from the underlying model.
    *
    * @return The param map from the underlying model.
    */
  def getParamMap: ParamMap = getModel.stages.last.extractParamMap()

  /** Retrieve the underlying model.
    *
    * @return The underlying model.
    */
  def getLastStage: Transformer = getModel.stages.last
}
