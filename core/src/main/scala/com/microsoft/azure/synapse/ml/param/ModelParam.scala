// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.serialize.ComplexParam
import org.apache.spark.ml.Model
import org.apache.spark.ml.param.Params

/** Param for Transformer.
  * Needed as spark has explicit com.microsoft.azure.synapse.ml.core.serialize.params for many different
  * types but not Transformer.
  */
class ModelParam(parent: Params, name: String, doc: String, isValid: Model[_ <: Model[_]] => Boolean)
  extends ComplexParam[Model[_ <: Model[_]]](parent, name, doc, isValid)
    with PipelineStageWrappable[Model[_ <: Model[_]]] {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, (_: Model[_ <: Model[_]]) => true)


}
