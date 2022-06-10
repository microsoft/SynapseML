// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import com.microsoft.azure.synapse.ml.core.serialize.ComplexParam
import com.microsoft.azure.synapse.ml.core.utils.{ModelEquality, ParamEquality}
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.Serializer
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.sql.SparkSession

import scala.reflect.runtime.universe.typeTag

/** Param for Evaluator.  Needed as spark has explicit params for many different
  * types but not Evaluator.
  */
class EvaluatorParam(parent: Params, name: String, doc: String, isValid: Evaluator => Boolean)
  extends ComplexParam[Evaluator](parent, name, doc, isValid)
    with ParamEquality[Evaluator] with ExternalDotnetWrappableParam[Evaluator] {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  override def assertEquality(v1: Any, v2: Any): Unit = {
    (v1, v2) match {
      case (e1: Evaluator, e2: Evaluator) =>
        ModelEquality.assertEqual(e1, e2)
      case _ =>
        throw new AssertionError("Values do not extend from Evaluator type")
    }
  }

  override def dotnetType: String = "JavaEvaluator"

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

  override def dotnetTestValue(v: Evaluator): String = {
    s"""${name}Param"""
  }

  override def dotnetLoadLine(modelNum: Int): String =
    throw new NotImplementedError("Implement dotnetLoadLine(modelNum: Int, testDataDir: String) method instead")

  def dotnetLoadLine(modelNum: Int, testDataDir: String): String = {
    val underlyingType = EvaluatorParam.loadForTest(
      SparkSession.builder().getOrCreate(),
       s"$testDataDir/model-$modelNum.model/complexParams/$name")
      .getClass.getTypeName.split(".".toCharArray).last

    s"""var ${name}ParamLoaded = (JvmObjectReference)_jvm.CallStaticJavaMethod(
       |    "org.apache.spark.ml.param.EvaluatorParam",
       |    "loadForTest",
       |    _spark,
       |    Path.Combine(TestDataDir, "model-$modelNum.model", "complexParams", "$name"));
       |var ${name}Param = new $underlyingType(${name}ParamLoaded);""".stripMargin
  }

}

// For auto test generation usage only, in production we
// should use model's load function to load the whole model
object EvaluatorParam {
  def loadForTest(sparkSession: SparkSession, path: String): Evaluator = {
    Serializer.typeToSerializer[Evaluator](
      typeTag[Evaluator].tpe, sparkSession).read(new Path(path))
  }
}
