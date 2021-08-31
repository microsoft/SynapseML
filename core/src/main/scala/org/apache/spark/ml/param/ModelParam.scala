// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import com.microsoft.ml.spark.core.serialize.ComplexParam
import org.apache.spark.ml.Model

/** Param for Transformer.  Needed as spark has explicit com.microsoft.ml.spark.core.serialize.params for many different
  * types but not Transformer.
  */
class ModelParam(parent: Params, name: String, doc: String, isValid: Model[_ <: Model[_]] => Boolean)
  extends ComplexParam[Model[_ <: Model[_]]](parent, name, doc, isValid)
    with PipelineStageWrappable[Model[_ <: Model[_]]]
    with WrappableParam[Model[_ <: Model[_]]] {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  override def dotnetType: String = "ScalaModel<M>"

  override def dotnetReturnType: String = "Model<object>"
}
