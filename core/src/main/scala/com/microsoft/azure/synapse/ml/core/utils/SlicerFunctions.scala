// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.utils

import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._

private[ml] object SlicerFunctions {
  private def slice[T](values: Int => T, indices: Seq[Int])(implicit num: Numeric[_]): Vector = {
    val n = num.asInstanceOf[Numeric[T]]
    Vectors.dense(indices.map(values.apply).map(n.toDouble).toArray)
  }

  private val DataTypeToNumericMap: Map[NumericType, Numeric[_]] = Map(
    FloatType -> implicitly[Numeric[Float]],
    DoubleType -> implicitly[Numeric[Double]],
    ByteType -> implicitly[Numeric[Byte]],
    ShortType -> implicitly[Numeric[Short]],
    IntegerType -> implicitly[Numeric[Int]],
    LongType -> implicitly[Numeric[Long]]
  )

  /**
    * A UDF that takes a vector, and a seq of indices. The function slices the given vector at given indices,
    * and returns the result in a Vector.
    */
  def vectorSlicer: UserDefinedFunction = {
    implicit val num: Numeric[_] = DataTypeToNumericMap(DoubleType)
    UDFUtils.oldUdf(
      (v: Vector, indices: Seq[Int]) => slice(v.apply, indices),
      VectorType
    )
  }

  /**
    * A UDF that takes an array of numeric types, and a seq of indices.
    * The function slices the given array at given indices, and returns the result in a Vector.
    */
  def arraySlicer(elementType: NumericType): UserDefinedFunction = {
    implicit val num: Numeric[_] = DataTypeToNumericMap(elementType)
    UDFUtils.oldUdf(
      (v: Seq[Any], indices: Seq[Int]) => slice(v.apply, indices),
      VectorType
    )
  }

  /**
    * A UDF that takes a map of integer keys and numeric values, and a seq of keys.
    * The function slices the given array at given keys, and returns the result in a Vector.
    */
  def mapSlicer(valueType: NumericType): UserDefinedFunction = {
    implicit val num: Numeric[_] = DataTypeToNumericMap(valueType)
    UDFUtils.oldUdf(
      (m: Map[Int, Any], indices: Seq[Int]) => slice(m.apply, indices),
      VectorType
    )
  }
}
