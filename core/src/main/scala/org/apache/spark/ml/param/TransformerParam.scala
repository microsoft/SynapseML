// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import com.microsoft.azure.synapse.ml.core.serialize.ComplexParam
import org.apache.spark.ml.Transformer

/** Param for Transformer.  Needed as spark has explicit params for many different
  * types but not Transformer.
  */
class TransformerParam(parent: Params, name: String, doc: String, isValid: Transformer => Boolean)
  extends ComplexParam[Transformer](parent, name, doc, isValid)
    with PipelineStageWrappable[Transformer] {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  override def dotnetType: String = "JavaTransformer"

  override def dotnetGetter(capName: String): String = {
    s"""|public $dotnetReturnType Get$capName()
        |{
        |    var jvmObject = (JvmObjectReference)Reference.Invoke(\"get$capName\");
        |    Dictionary<string, Type> classMapping = JvmObjectUtils.ConstructJavaClassMapping(
        |                typeof($dotnetReturnType),
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
