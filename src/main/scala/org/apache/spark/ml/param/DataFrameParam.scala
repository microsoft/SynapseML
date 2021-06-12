// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import com.microsoft.ml.spark.core.serialize.ComplexParam
import com.microsoft.ml.spark.core.utils.ParamEquality
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalactic.TripleEquals._
import org.scalactic.{Equality, TolerantNumerics}


trait DataFrameEquality extends Serializable {
  val epsilon = 1e-4
  @transient implicit lazy val doubleEq: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(epsilon)

  @transient implicit lazy val dvEq: Equality[DenseVector] = new Equality[DenseVector] {
    def areEqual(a: DenseVector, b: Any): Boolean = b match {
      case bArr: DenseVector =>
        a.values.zip(bArr.values).forall { case (x, y) => doubleEq.areEqual(x, y) }
    }
  }

  @transient implicit lazy val rowEq: Equality[Row] = new Equality[Row] {
    def areEqual(a: Row, bAny: Any): Boolean = bAny match {
      case b: Row =>
        if (a.length != b.length) {
          false
        } else {
          (0 until a.length).forall(j => {
            a(j) match {
              case lhs: DenseVector =>
                lhs === b(j)
              case lhs: Array[Byte] =>
                lhs === b(j)
              case lhs: Double if lhs.isNaN =>
                b(j).asInstanceOf[Double].isNaN
              case lhs: Double =>
                b(j).asInstanceOf[Double] === lhs
              case lhs =>
                lhs === b(j)
            }
          })
        }
    }
  }

  val sortInDataframeEquality = false

  @transient val baseDfEq: Equality[DataFrame] = new Equality[DataFrame] {
    def areEqual(a: DataFrame, bAny: Any): Boolean = bAny match {
      case ds: Dataset[_] =>
        val b = ds.toDF()
        if (a.columns !== b.columns) {
          return false
        }
        val (aList, bList) = if (sortInDataframeEquality) {
          (a.sort(a.columns.sorted.map(col): _*).collect(),
            b.sort(b.columns.sorted.map(col): _*).collect())
        } else {
          (a.collect(), b.collect())
        }

        if (aList.length != bList.length) {
          return false
        }
        aList.zip(bList).forall { case (rowA, rowB) =>
          rowA === rowB
        }
    }
  }

  @transient implicit lazy val dfEq: Equality[DataFrame] = baseDfEq

  def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    val result = eq.areEqual(df1, df2)
    if (!result) {
      println("df1:")
      df1.show(10)
      println("df2:")
      df2.show(10)
    }
    assert(result)
  }

}


/** Param for DataFrame.  Needed as spark has explicit com.microsoft.ml.spark.core.serialize.params for many different
  * types but not DataFrame.
  */
class DataFrameParam(parent: Params, name: String, doc: String, isValid: DataFrame => Boolean)
  extends ComplexParam[DataFrame](parent, name, doc, isValid)
    with ExternalPythonWrappableParam[DataFrame] with ParamEquality[DataFrame] with DataFrameEquality {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  override def pyValue(v: DataFrame): String = {
    s"""${name}DF"""
  }

  override def pyLoadLine(modelNum: Int): String = {
    s"""${name}DF = spark.read.parquet(join(test_data_dir, "model-${modelNum}.model", "complexParams", "${name}"))"""
  }

  override def assertEquality(v1: Any, v2: Any): Unit = {
    (v1, v2) match {
      case (df1: Dataset[_], df2: Dataset[_]) =>
        // assert(df1.toDF() === df2.toDF())
        assertDFEq(df1.toDF, df2.toDF)
      case _ =>
        throw new AssertionError("Values did not have DataFrame type")
    }
  }
}
