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

import scala.reflect.ClassTag


trait DataFrameEquality extends Serializable {
  val epsilon = 1e-4
  @transient implicit lazy val doubleEq: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(epsilon)

  @transient implicit lazy val dvEq: Equality[DenseVector] = (a: DenseVector, b: Any) => b match {
    case bArr: DenseVector =>
      a.values.zip(bArr.values).forall {
        case (x, y) => (x.isNaN && y.isNaN) || doubleEq.areEqual(x, y)
      }
  }

  @transient implicit lazy val seqEq: Equality[Seq[_]] = new Equality[Seq[_]] {
    override def areEqual(a: Seq[_], b: Any): Boolean = {
      b match {
        case bSeq: Seq[_] =>
          a.zip(bSeq).forall {
            case (lhs, rhs) =>
              lhs.getClass match {
                case c if c == classOf[DenseVector] =>
                  lhs.asInstanceOf[DenseVector] === rhs.asInstanceOf[DenseVector]
                case c if c == classOf[Double] =>
                  lhs.asInstanceOf[Double] === rhs.asInstanceOf[Double]
                case _ =>
                  lhs === rhs
              }
          }
      }
    }
  }


  @transient implicit lazy val rowEq: Equality[Row] = (a: Row, bAny: Any) => bAny match {
    case b: Row =>
      if (a.length != b.length) {
        false
      } else {
        (0 until a.length).forall(j => {
          a(j) match {
            case lhs: DenseVector =>
              lhs === b(j)
            case lhs: Seq[_] =>
              lhs === b(j).asInstanceOf[Seq[_]]
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
          false
        } else {
          aList.zip(bList).forall { case (rowA, rowB) =>
            rowA === rowB
          }
        }
    }
  }

  @transient implicit lazy val dfEq: Equality[DataFrame] = baseDfEq

  def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    val result = eq.areEqual(df1, df2)
    if (!result) {
      println("df1:")
      df1.show(100, false)
      println("df2:")
      df2.show(100, false)
    }
    assert(result)
  }

}


/** Param for DataFrame.  Needed as spark has explicit com.microsoft.ml.spark.core.serialize.params for many different
  * types but not DataFrame.
  */
class DataFrameParam(parent: Params, name: String, doc: String, isValid: DataFrame => Boolean)
  extends ComplexParam[DataFrame](parent, name, doc, isValid)
    with ExternalPythonWrappableParam[DataFrame] with ParamEquality[DataFrame] with DataFrameEquality
    with ExternalDotnetWrappableParam[DataFrame] {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  override def pyValue(v: DataFrame): String = {
    s"""${name}DF"""
  }

  override def pyLoadLine(modelNum: Int): String = {
    s"""${name}DF = spark.read.parquet(join(test_data_dir, "model-${modelNum}.model", "complexParams", "${name}"))"""
  }

  override def dotnetValue(v: DataFrame): String = {
    s"""${name}DF"""
  }

  override def dotnetLoadLine(modelNum: Int): String = {
    s"""var ${name}DF = spark.Read().Parquet(
       |    Path.Combine(test_data_dir, "model-$modelNum.model", "complexParams", "$name"));""".stripMargin
  }

  override def assertEquality(v1: Any, v2: Any): Unit = {
    (v1, v2) match {
      case (df1: Dataset[_], df2: Dataset[_]) =>
        assertDFEq(df1.toDF, df2.toDF)
      case _ =>
        throw new AssertionError("Values did not have DataFrame type")
    }
  }

  override val sortInDataframeEquality: Boolean = true
}
