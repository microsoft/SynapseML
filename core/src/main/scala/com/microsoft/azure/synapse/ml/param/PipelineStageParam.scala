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

  override private[ml] def dotnetType: String = "JavaPipelineStage"

  override private[ml] def dotnetGetter(capName: String): String =
    dotnetGetterHelper(dotnetReturnType, dotnetReturnType, capName)

  override def rLoadLine(modelNum: Int): String = {
    s"""
       |${name}Dir <- file.path(test_data_dir, "model-${modelNum}.model", "complexParams", "${name}")
       |${name}DF <- spark_dataframe(spark_read_parquet(sc, path = ${name}Dir))
       """.stripMargin
  }

}
