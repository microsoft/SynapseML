// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import com.microsoft.azure.synapse.ml.core.serialize.ComplexParam
import com.microsoft.azure.synapse.ml.core.utils.{ModelEquality, ParamEquality}
import org.apache.spark.ml.{Estimator, Model, PipelineStage}
import org.apache.spark.ml.Pipeline

trait PipelineStageWrappable[T <: PipelineStage] extends ExternalPythonWrappableParam[T]
  with ParamEquality[T] with ExternalDotnetWrappableParam[T] {

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

  override def dotnetValue(v: T): String = {
    s"""${name}Model"""
  }

//  // TODO: add this back once we support all underlyingTypes & fix assemblies access for Pipeline.GetStages
//  override def dotnetLoadLine(modelNum: Int, testDataDir: String): String = {
//    val underlyingType = Pipeline.load(
//      testDataDir + s"\\model-$modelNum.model\\complexParams\\$name")
//      .getStages.head.getClass.getTypeName.split(".".toCharArray).last
//
//    s"""
//       |var ${name}Load = Pipeline.Load(
//       |    Path.Combine(TestDataDir, "model-$modelNum.model", "complexParams", "$name"));
//       |var ${name}Model = ($underlyingType)${name}Load.GetStages()[0];
//       |""".stripMargin
//  }

  override def dotnetLoadLine(modelNum: Int): String =
    throw new NotImplementedError("No translation found for complex parameter")

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
    this(parent, name, doc, ParamValidators.alwaysTrue)

  override def dotnetType: String = "ScalaEstimator<M>"

  override def dotnetReturnType: String = "Estimator<object>"

}
