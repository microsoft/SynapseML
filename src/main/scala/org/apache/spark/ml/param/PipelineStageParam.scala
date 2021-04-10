// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import com.microsoft.ml.spark.core.serialize.ComplexParam
import org.apache.spark.ml.{Estimator, Model, PipelineStage}

/** Param for Transformer.  Needed as spark has explicit com.microsoft.ml.spark.core.serialize.params for many different
  * types but not Transformer.
  */
class PipelineStageParam(parent: Params, name: String, doc: String, isValid: PipelineStage => Boolean)
  extends ComplexParam[PipelineStage](parent, name, doc, isValid)
    with PipelineStageWrappable[PipelineStage] {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

}
