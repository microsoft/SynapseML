// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType

//scalastyle:off
object udfs {

  def get_value_udf(i:Int): UserDefinedFunction = {
    UDFUtils.oldUdf({
      vec: org.apache.spark.ml.linalg.Vector => vec(i)
    }, DoubleType)
  }

  def get_value_at(colName: String, i: Int): Column = {
    get_value_udf(i)(col(colName))
  }

  val to_vector: UserDefinedFunction = UDFUtils.oldUdf({
    arr: Seq[Double] => Vectors.dense(arr.toArray)
  }, VectorType)

  def to_vector(colName: String): Column = to_vector(col(colName))


}
