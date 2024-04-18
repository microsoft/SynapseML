// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.serialize.ComplexParam
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.param.Params

/** Param for Transformer.  Needed as spark has explicit params for many different
  * types but not Transformer.
  */
class PipelineStageParam(parent: Params, name: String, doc: String, isValid: PipelineStage => Boolean)
  extends ComplexParam[PipelineStage](parent, name, doc, isValid)
    with PipelineStageWrappable[PipelineStage] {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, (_: PipelineStage) => true)

  override def rLoadLine(modelNum: Int): String = {
    s"""
       |${name}Model <- ml_load(sc, path = file.path(test_data_dir, "model-$modelNum.model", "complexParams", "$name"))
       |${name}Model <- ml_stages(${name}Model)[[1]]
       """.stripMargin
  }

}
