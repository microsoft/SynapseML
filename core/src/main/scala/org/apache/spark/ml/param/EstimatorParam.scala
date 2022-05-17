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

  override def dotnetTestValue(v: T): String = {
    s"""${name}Model"""
  }

  override def dotnetLoadLine(modelNum: Int): String =
    throw new NotImplementedError("Implement dotnetLoadLine(modelNum: Int, testDataDir: String) method instead")

  def dotnetLoadLine(modelNum: Int, testDataDir: String): String = {
    val underlyingType = Pipeline.load(
      testDataDir + s"\\model-$modelNum.model\\complexParams\\$name")
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
    this(parent, name, doc, ParamValidators.alwaysTrue)

  override def dotnetType: String = "JavaEstimator<M>"

  override def dotnetReturnType: String = "IEstimator<object>"

  override def dotnetSetter(dotnetClassName: String, capName: String, dotnetClassWrapperName: String): String = {
    s"""|public $dotnetClassName Set$capName<M>($dotnetType value) where M : JavaModel<M> =>
        |    $dotnetClassWrapperName(Reference.Invoke(\"set$capName\", (object)value));
        |""".stripMargin
  }

  override def dotnetGetter(capName: String): String = {
    val parentClassType = "JavaPipelineStage"
    s"""|public $dotnetReturnType Get$capName()
        |{
        |    var jvmObject = (JvmObjectReference)Reference.Invoke(\"get$capName\");
        |    Dictionary<string, Type> classMapping = JvmObjectUtils.ConstructJavaClassMapping(
        |                typeof($parentClassType),
        |                "s_className");
        |    JvmObjectUtils.TryConstructInstanceFromJvmObject(
        |                jvmObject,
        |                classMapping,
        |                out $dotnetReturnType instance);
        |    return instance;
        |}
        |""".stripMargin
  }

}
