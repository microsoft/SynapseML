// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import com.microsoft.ml.spark.core.serialize.ComplexParam
import com.microsoft.ml.spark.core.utils.ParamEquality
import org.apache.spark.injections.UDFUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.scalactic.TripleEquals._

/** Param for UserDefinedFunction.  Needed as spark has explicit params for many different
  * types but not UserDefinedFunction.
  */
class UDFParam(parent: Params, name: String, doc: String, isValid: UserDefinedFunction => Boolean)
  extends ComplexParam[UserDefinedFunction](parent, name, doc, isValid)
    with ParamEquality[UserDefinedFunction] {

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
}
