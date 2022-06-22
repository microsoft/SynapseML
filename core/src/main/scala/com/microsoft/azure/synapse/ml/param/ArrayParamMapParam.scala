// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.serialize.ComplexParam
import com.microsoft.azure.synapse.ml.core.utils.{ModelEquality, ParamEquality}
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.Serializer
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.sql.SparkSession

import scala.reflect.runtime.universe.typeTag

/** Param for Array of ParamMaps.  Needed as spark has explicit params for many different
  * types but not Array of ParamMaps.
  */
class ArrayParamMapParam(parent: Params, name: String, doc: String, isValid: Array[ParamMap] => Boolean)
  extends ComplexParam[Array[ParamMap]](parent, name, doc, isValid) with ParamEquality[Array[ParamMap]]
    with ExternalDotnetWrappableParam[Array[ParamMap]]  {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, (_: Array[ParamMap]) => true)

  override def assertEquality(v1: Any, v2: Any): Unit = {
    (v1, v2) match {
      case (e1: Array[ParamMap], e2: Array[ParamMap]) =>
        for(i <- e1.indices) {
          ModelEquality.assertEqual(e1(i), e2(i))
        }
      case _ =>
        throw new AssertionError("Values do not extend from Evaluator type")
    }
  }

  override private[ml] def dotnetType: String = "ParamMap[]"

  override private[ml] def dotnetSetter(dotnetClassName: String,
                                        capName: String,
                                        dotnetClassWrapperName: String): String = {
    s"""|public $dotnetClassName Set$capName($dotnetType value)
        |    => $dotnetClassWrapperName(Reference.Invoke(\"set$capName\", (object)value.ToJavaArrayList()));
        |""".stripMargin
  }

  override private[ml] def dotnetGetter(capName: String): String = {
    s"""|public $dotnetReturnType Get$capName()
        |{
        |    var jvmObjects = (JvmObjectReference[])Reference.Invoke(\"get$capName\");
        |    var result = new ParamMap[jvmObjects.Length];
        |    for (int i=0; i < jvmObjects.Length; i++)
        |    {
        |        result[i] = new ParamMap(jvmObjects[i]);
        |    }
        |    return result;
        |}
        |""".stripMargin
  }

  override private[ml] def dotnetTestValue(v: Array[ParamMap]): String = {
    s"""${name}Param"""
  }

  override private[ml] def dotnetLoadLine(modelNum: Int): String = {
    s"""var ${name}ParamLoaded = (JvmObjectReference[])_jvm.CallStaticJavaMethod(
       |    "com.microsoft.azure.synapse.ml.param.ArrayParamMapParam",
       |    "loadForTest",
       |    _spark,
       |    Path.Combine(TestDataDir, "model-$modelNum.model", "complexParams", "$name"));
       |var ${name}Param = new ParamMap[${name}ParamLoaded.Length];
       |for (int i = 0; i < ${name}ParamLoaded.Length; i++)
       |{
       |    ${name}Param[i] = new ParamMap(${name}ParamLoaded[i]);
       |}""".stripMargin
  }

}

// For auto test generation usage only, in production we
// should use model's load function to load the whole model
object ArrayParamMapParam {
  def loadForTest(sparkSession: SparkSession, path: String): Array[ParamMap] = {
    Serializer.typeToSerializer[Array[ParamMap]](
      typeTag[Array[ParamMap]].tpe, sparkSession).read(new Path(path))
  }
}
