// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import org.apache.spark.ml.{Estimator, Model, PipelineStage}
import org.apache.spark.ml.util.Identifiable

/**
  * Param for Estimator.  Needed as spark has explicit params for many different types but not Estimator.
  */
class EstimatorParam(parent: String, name: String, doc: String, isValid: Estimator[_ <: Model[_]] => Boolean)
  extends Param[Estimator[_ <: Model[_]]](parent, name, doc, isValid) {

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  def this(parent: Identifiable, name: String, doc: String, isValid: Estimator[_ <: Model[_]] => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) =
    this(parent.uid, name, doc)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: Estimator[_ <: Model[_]]): ParamPair[Estimator[_ <: Model[_]]] =
    super.w(value)

  override def jsonEncode(value: Estimator[_ <: Model[_]]): String = {
    throw new NotImplementedError("The transform cannot be encoded.")
  }

  override def jsonDecode(json: String): Estimator[_ <: Model[_]] = {
    throw new NotImplementedError("The transform cannot be decoded.")
  }

}

/**
  * Param for PipelineStage.  Needed as spark has explicit params for many different types but not PipelineStage.
  */
class PipelineStageParam(parent: String, name: String, doc: String, isValid: PipelineStage => Boolean)
  extends Param[PipelineStage](parent, name, doc, isValid) {

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  def this(parent: Identifiable, name: String, doc: String, isValid: PipelineStage => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) =
    this(parent.uid, name, doc)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: PipelineStage): ParamPair[PipelineStage] =
    super.w(value)

  override def jsonEncode(value: PipelineStage): String = {
    throw new NotImplementedError("The transform cannot be encoded.")
  }

  override def jsonDecode(json: String): Estimator[_ <: Model[_]] = {
    throw new NotImplementedError("The transform cannot be decoded.")
  }

}
