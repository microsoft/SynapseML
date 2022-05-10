// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import com.microsoft.azure.synapse.ml.core.serialize.ComplexParam
import org.apache.spark.ml.Model

/** Param for Transformer.
  * Needed as spark has explicit com.microsoft.azure.synapse.ml.core.serialize.params for many different
  * types but not Transformer.
  */
class ModelParam(parent: Params, name: String, doc: String, isValid: Model[_ <: Model[_]] => Boolean)
  extends ComplexParam[Model[_ <: Model[_]]](parent, name, doc, isValid)
    with PipelineStageWrappable[Model[_ <: Model[_]]] {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  override def dotnetType: String = "JavaModel<M>"

  override def dotnetReturnType: String = "IModel<object>"

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
