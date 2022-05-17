// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import com.microsoft.azure.synapse.ml.core.serialize.ComplexParam
import com.microsoft.azure.synapse.ml.core.utils.ParamEquality
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.Serializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.scalactic.TripleEquals._

import scala.reflect.runtime.universe.typeTag

/** Param for UserDefinedFunction.  Needed as spark has explicit params for many different
  * types but not UserDefinedFunction.
  */
class UDFParam(parent: Params, name: String, doc: String, isValid: UserDefinedFunction => Boolean)
  extends ComplexParam[UserDefinedFunction](parent, name, doc, isValid)
    with ParamEquality[UserDefinedFunction] with ExternalDotnetWrappableParam[UserDefinedFunction] {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  override def assertEquality(v1: Any, v2: Any): Unit = {
    (v1, v2) match {
      case (udf1: UserDefinedFunction, udf2: UserDefinedFunction) =>
        val (f1, dt1) = UDFUtils.unpackUdf(udf1)
        val (f2, dt2) = UDFUtils.unpackUdf(udf2)
        assert(dt1 === dt2)
        assert(f1.toString.split("/".toCharArray).head === f2.toString.split("/".toCharArray).head)
      case _ =>
        throw new AssertionError("Values did not have UserDefinedFunction type")
    }
  }

  override def dotnetTestValue(v: UserDefinedFunction): String = {
    s"""${name}Param"""
  }

  override def dotnetLoadLine(modelNum: Int): String = {
    s"""var ${name}Param = _jvm.CallStaticJavaMethod(
       |    "org.apache.spark.ml.param.UDFParam",
       |    "loadForTest",
       |    _spark,
       |    Path.Combine(TestDataDir, "model-$modelNum.model", "complexParams", "$name"));""".stripMargin
  }

}

// For auto test generation usage only, in production we
// should use model's load function to load the whole model
object UDFParam {
  def loadForTest(sparkSession: SparkSession, path: String): UserDefinedFunction = {
    Serializer.typeToSerializer[UserDefinedFunction](
      typeTag[UserDefinedFunction].tpe, sparkSession).read(new Path(path))
  }
}
