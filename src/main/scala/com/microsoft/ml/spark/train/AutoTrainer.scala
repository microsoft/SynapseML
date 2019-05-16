// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.train

import com.microsoft.ml.spark.core.contracts.{HasFeaturesCol, HasLabelCol}
import org.apache.spark.ml.{ComplexParamsWritable, Estimator, Model}
import org.apache.spark.ml.param.{EstimatorParam, IntParam}

/** Defines common inheritance and parameters across trainers.
  */
trait AutoTrainer[TrainedModel <: Model[TrainedModel]] extends Estimator[TrainedModel]
  with HasLabelCol with ComplexParamsWritable with HasFeaturesCol {

  /** Doc for model to run.
    */
  def modelDoc: String

  /** Number of features to hash to
    * @group param
    */
  val numFeatures = new IntParam(this, "numFeatures", "Number of features to hash to")
  setDefault(numFeatures -> 0)

  /** @group getParam */
  def getNumFeatures: Int = $(numFeatures)
  /** @group setParam */
  def setNumFeatures(value: Int): this.type = set(numFeatures, value)

  /** Model to run.  See doc on derived classes.
    * @group param
    */
  val model = new EstimatorParam(this, "model", modelDoc)
  /** @group getParam */
  def getModel: Estimator[_ <: Model[_]] = $(model)
  /** @group setParam */
  def setModel(value: Estimator[_ <: Model[_]]): this.type = set(model, value)
}
