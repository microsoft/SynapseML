// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.serialize.ComplexParam
import com.microsoft.azure.synapse.ml.core.utils.ParamEquality
import org.apache.hadoop.fs.Path
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.Serializer
import org.apache.spark.ml.param.Params
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.scalactic.TripleEquals._

import scala.reflect.runtime.universe.typeTag

/** Param for UserDefinedFunction.  Needed as spark has explicit params for many different
  * types but not UserDefinedFunction.
  */
class UDFParam(parent: Params, name: String, doc: String, isValid: UserDefinedFunction => Boolean)
  extends ComplexParam[UserDefinedFunction](parent, name, doc, isValid)
    with ParamEquality[UserDefinedFunction] {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, (_: UserDefinedFunction) => true)

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


}

// For auto test generation usage only, in production we
// should use model's load function to load the whole model
object UDFParam {
  def loadForTest(sparkSession: SparkSession, path: String): UserDefinedFunction = {
    Serializer.typeToSerializer[UserDefinedFunction](
      typeTag[UserDefinedFunction].tpe, sparkSession).read(new Path(path))
  }
}
