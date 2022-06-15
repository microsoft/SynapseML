// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.serialize.ComplexParam
import com.microsoft.azure.synapse.ml.core.utils.{ModelEquality, ParamEquality}
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.{Estimator, Model, Pipeline, PipelineStage}

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

  override private[ml] def dotnetTestValue(v: T): String = {
    s"""${name}Model"""
  }

  override private[ml] def dotnetLoadLine(modelNum: Int): String =
    throw new NotImplementedError("Implement dotnetLoadLine(modelNum: Int, testDataDir: String) method instead")

  private[ml] def dotnetLoadLine(modelNum: Int, testDataDir: String): String = {
    val underlyingType = Pipeline.load(s"$testDataDir/model-$modelNum.model/complexParams/$name")
      .getStages.head.getClass.getTypeName.split(".".toCharArray).last

    s"""
       |var ${name}Loaded = Pipeline.Load(
       |    Path.Combine(TestDataDir, "model-$modelNum.model", "complexParams", "$name"));
       |var ${name}Model = ($underlyingType)${name}Loaded.GetStages()[0];
       |""".stripMargin
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

  override private[ml] def dotnetType: String = "JavaEstimator<M>"

  override private[ml] def dotnetReturnType: String = "IEstimator<object>"

  override private[ml] def dotnetSetter(dotnetClassName: String,
                                        capName: String,
                                        dotnetClassWrapperName: String): String = {
    s"""|public $dotnetClassName Set$capName<M>($dotnetType value) where M : JavaModel<M> =>
        |    $dotnetClassWrapperName(Reference.Invoke(\"set$capName\", (object)value));
        |""".stripMargin
  }

  override private[ml] def dotnetGetter(capName: String): String =
    dotnetGetterHelper(dotnetReturnType, "JavaPipelineStage", capName)

}
