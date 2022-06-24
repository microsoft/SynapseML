// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.injections

import org.apache.spark.sql.api.java.{UDF1, UDF2, UDF3}
import org.apache.spark.sql.expressions.{SparkUserDefinedFunction, UserDefinedFunction}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DataType

object UDFUtils {
  def unpackUdf(u: UserDefinedFunction): (AnyRef, DataType) = {
    u match {
      case SparkUserDefinedFunction(f, dataType, _, _, _, _, _) =>
        (f, dataType)
      case _ =>
        throw new IllegalArgumentException("Can only unpack scala UDFs")
    }
  }

  def oldUdf[RT, A1](f: Function1[A1, RT], dataType: DataType): UserDefinedFunction = {
    udf(new UDF1[A1, RT] {
      def call(a1: A1): RT = f(a1)
    }, dataType)
  }

  def oldUdf[RT, A1, A2](f: Function2[A1, A2, RT], dataType: DataType): UserDefinedFunction = {
    udf(new UDF2[A1, A2, RT] {
      def call(a1: A1, a2: A2): RT = f(a1, a2)
    }, dataType)
  }

  def oldUdf[RT, A1, A2, A3](f: Function3[A1, A2, A3, RT], dataType: DataType): UserDefinedFunction = {
    udf(new UDF3[A1, A2, A3, RT] {
      def call(a1: A1, a2: A2, a3: A3): RT = f(a1, a2, a3)
    }, dataType)
  }
}
