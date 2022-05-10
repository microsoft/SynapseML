// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import com.microsoft.azure.synapse.ml.core.serialize.ComplexParam
import org.apache.spark.ml.Estimator

import scala.collection.JavaConverters._

/** Param for Array of Models. */
class EstimatorArrayParam(parent: Params, name: String, doc: String, isValid: Array[Estimator[_]] => Boolean)
  extends ComplexParam[Array[Estimator[_]]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  /** Creates a param pair with the given value (for Java). */
  def w(value: java.util.List[Estimator[_]]): ParamPair[Array[Estimator[_]]] = w(value.asScala.toArray)

  override def dotnetType: String = "IEstimator<object>[]"

  override def dotnetSetter(dotnetClassName: String, capName: String, dotnetClassWrapperName: String): String = {
    s"""|public $dotnetClassName Set$capName($dotnetType value)
        |    => $dotnetClassWrapperName(Reference.Invoke(\"set$capName\", (object)value.ToJavaArrayList()));
        |""".stripMargin
  }

  override def dotnetGetter(capName: String): String = {
    val dType = "IEstimator<object>"
    val parentType = "JavaPipelineStage"
    s"""|public $dotnetReturnType Get$capName()
        |{
        |    var jvmObjects = (JvmObjectReference[])Reference.Invoke(\"get$capName\");
        |    var result = new $dType[jvmObjects.Length];
        |    Dictionary<string, Type> classMapping = JvmObjectUtils.ConstructJavaClassMapping(
        |                typeof($parentType),
        |                "s_className");
        |    for (int i=0; i < jvmObjects.Length; i++)
        |    {
        |        if (JvmObjectUtils.TryConstructInstanceFromJvmObject(
        |            jvmObjects[i],
        |            classMapping,
        |            out $dType instance))
        |        {
        |            result[i] = instance;
        |        }
        |    }
        |    return result;
        |}
        |""".stripMargin
  }

}
