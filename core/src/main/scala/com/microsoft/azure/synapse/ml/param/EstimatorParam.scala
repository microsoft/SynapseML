// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.serialize.ComplexParam
import com.microsoft.azure.synapse.ml.core.utils.{ModelEquality, ParamEquality}
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.{Estimator, Model, Pipeline, PipelineStage}


trait PipelineStageWrappable[T <: PipelineStage]
  extends ParamEquality[T]
  with ExternalWrappableParam[T] {

  override def pyValue(v: T): String = {
    s"""${name}Model"""
  }

  override def pyLoadLine(modelNum: Int): String = {
    s"""
       |from pyspark.ml import Pipeline
       |${name}Model = Pipeline.load(join(test_data_dir, "model-$modelNum.model", "complexParams", "$name"))
       |${name}Model = ${name}Model.getStages()[0]
       |""".stripMargin
  }

  override def rValue(v: T): String = {
    s"""${name}Model"""
  }

  override def rLoadLine(modelNum: Int): String = {
    s"""
       |${name}Model <- ml_load(sc, path = file.path(test_data_dir, "model-$modelNum.model", "complexParams", "$name"))
       |${name}Model <- ml_stages(${name}Model)[[1]]
       """.stripMargin
  }

  override def assertEquality(v1: Any, v2: Any): Unit = {
    (v1, v2) match {
      case (e1: PipelineStage, e2: PipelineStage) =>
        ModelEquality.assertEqual(e1, e2)
      case _ =>
        throw new AssertionError("Values do not extend from PipelineStage type")
    }
  }

}

/** Param for Estimator.  Needed as spark has explicit params for many different
  * types but not Estimator.
  */
class EstimatorParam(parent: Params, name: String, doc: String, isValid: Estimator[_ <: Model[_]] => Boolean)
  extends ComplexParam[Estimator[_ <: Model[_]]](parent, name, doc, isValid)
    with PipelineStageWrappable[Estimator[_ <: Model[_]]] {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, (_: Estimator[_ <: Model[_]]) => true)

  def rValue(v: Estimator[_]): String = {
    s"""${name}Model"""
  }

  override def rLoadLine(modelNum: Int): String = {
    s"""
       |${name}Model <- ml_load(sc, path = file.path(test_data_dir, "model-$modelNum.model", "complexParams", "$name"))
       |${name}Model <- ml_stages(${name}Model)[[1]]
     """.stripMargin
  }

}
