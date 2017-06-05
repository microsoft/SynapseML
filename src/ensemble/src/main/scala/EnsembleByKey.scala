// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.SQLDataTypes._
import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

import scala.collection.mutable

object EnsembleByKey extends DefaultParamsReadable[EnsembleByKey]

class EnsembleByKey(val uid: String) extends Transformer with MMLParams {
  def this() = this(Identifiable.randomUID("EnsembleByKey"))

  val keys = new StringArrayParam(this, "keys", "keys to group by")

  def getKeys: Array[String] = $(keys)

  def setKeys(arr: Array[String]): this.type = set(keys, arr)

  def setKeys(arr: String*): this.type = set(keys, arr.toArray)

  def setKey(value: String): this.type = set(keys, Array(value))

  val cols = new StringArrayParam(this, "cols", "Cols to ensemble")

  def getCols: Array[String] = $(cols)

  def setCols(arr: Array[String]): this.type = set(cols, arr)

  def setCols(arr: String*): this.type = set(cols, arr.toArray)

  def setCol(value: String): this.type = set(cols, Array(value))

  val allowedStrategies = Set("mean")
  val strategy = StringParam(this, "strategy", "How to ensemble the scores ex:" +
    " mean", { x: String => allowedStrategies(x) })

  def getStrategy: String = $(strategy)

  def setStrategy(value: String): this.type = set(strategy, value)

  setDefault(strategy -> "mean")

  val collapseGroup = BooleanParam(this, "collapseGroup", "whether to collapse all items in group to one entry")

  def getCollapseGroup: Boolean = $(collapseGroup)

  def setCollapseGroup(value: Boolean): this.type = set(collapseGroup, value)

  setDefault(collapseGroup -> true)

  override def transform(dataset: Dataset[_]): DataFrame = {

    val strategyToFloatFunction = Map(
      "mean" -> { x: String => mean(x) }
    )

    val newCols = getCols.map { colname =>
      dataset.schema(colname).dataType match {
        case _: DoubleType =>
          strategyToFloatFunction(getStrategy)(colname)
        case _: FloatType =>
          strategyToFloatFunction(getStrategy)(colname)
        case v if v == VectorType && getStrategy == "mean" =>
          val dim = dataset.select(colname).take(1)(0).getAs[DenseVector](0).size
          new VectorAvg(dim)(dataset(colname)).asInstanceOf[Column].alias(s"avg($colname)")
        case t =>
          throw new IllegalArgumentException(s"Cannot operate on type:$t with strategy:$getStrategy")
      }
    }

    val aggregated = dataset.toDF()
      .groupBy(getKeys.head, getKeys.tail: _*)
      .agg(newCols.head, newCols.tail: _*)

    if (getCollapseGroup) {
      aggregated
    } else {
      dataset.toDF().join(aggregated, getKeys)
    }

  }

  def transformSchema(schema: StructType): StructType = {
    val keySet = getKeys.toSet
    val colSet = getCols.toSet

    val newFields = schema.fields.flatMap { f =>
      if (colSet(f.name)) {
        f.dataType match {
          case _: DoubleType => Some(StructField(s"avg(${f.name})", f.dataType))
          case _: FloatType => Some(StructField(s"avg(${f.name})", f.dataType))
          case fdt if fdt == VectorType =>
            Some(StructField(s"avg(${f.name})", f.dataType))
          case t => throw new IllegalArgumentException(s"Cannot operate on type:$t with strategy:$getStrategy")
        }
      } else {
        None
      }
    }

    val keyFields = schema.fields.filter(f => colSet(f.name))
    val fields = if (getCollapseGroup) {
      schema.fields.++(newFields)
    } else {
      keyFields.++(newFields)
    }

    new StructType(fields)
  }

  def copy(extra: ParamMap): this.type = defaultCopy(extra)

}

private class VectorAvg(n: Int) extends UserDefinedAggregateFunction {
  def inputSchema: StructType = new StructType().add("v", VectorType)

  def bufferSchema: StructType =
    new StructType().add("buff", ArrayType(DoubleType)).add("count", LongType)

  def dataType: DataType = VectorType

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, Array.fill(n)(0.0))
    buffer.update(1, 0L)
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      val buff = buffer.getAs[mutable.WrappedArray[Double]](0)
      val count = buffer.getLong(1)

      val v = input.getAs[Vector](0).toSparse
      for (i <- v.indices) {
        buff(i) += v(i)
      }
      buffer.update(0, buff)
      buffer.update(1, count + 1L)
    }
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val buff1 = buffer1.getAs[mutable.WrappedArray[Double]](0)
    val buff2 = buffer2.getAs[mutable.WrappedArray[Double]](0)
    val c1 = buffer1.getLong(1)
    val c2 = buffer2.getLong(1)

    for ((x, i) <- buff2.zipWithIndex) {
      buff1(i) += x
    }
    buffer1.update(0, buff1)
    buffer1.update(1, c1 + c2)
  }

  def evaluate(buffer: Row): Vector = {
    val c = buffer.getLong(1)
    Vectors.dense(buffer.getAs[Seq[Double]](0).map(_ / c).toArray)
  }
}
