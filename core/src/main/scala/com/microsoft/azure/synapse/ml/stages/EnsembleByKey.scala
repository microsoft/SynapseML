// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.StringIntMapParam
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.SQLDataTypes._
import org.apache.spark.ml.param._
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.JavaConverters._

object EnsembleByKey extends DefaultParamsReadable[EnsembleByKey]

class EnsembleByKey(val uid: String) extends Transformer
  with Wrappable with DefaultParamsWritable with SynapseMLLogging {
  logClass(FeatureNames.Core)

  def this() = this(Identifiable.randomUID("EnsembleByKey"))

  val keys = new StringArrayParam(this, "keys", "Keys to group by")

  def getKeys: Array[String] = $(keys)

  def setKeys(arr: Array[String]): this.type = set(keys, arr)

  def setKeys(arr: String*): this.type = set(keys, arr.toArray)

  def setKey(value: String): this.type = set(keys, Array(value))

  val cols = new StringArrayParam(this, "cols", "Cols to ensemble")

  def getCols: Array[String] = $(cols)

  def setCols(arr: Array[String]): this.type = set(cols, arr)

  def setCols(arr: String*): this.type = set(cols, arr.toArray)

  def setCol(value: String): this.type = set(cols, Array(value))

  val colNames = new StringArrayParam(this, "colNames", "Names of the result of each col")

  def getColNames: Array[String] = $(colNames)

  def setColNames(arr: Array[String]): this.type = set(colNames, arr)

  def setColNames(arr: String*): this.type = set(colNames, arr.toArray)

  def setColName(value: String): this.type = set(colNames, Array(value))

  val allowedStrategies: Set[String] = Set("mean")
  val strategy = new Param[String](this, "strategy", "How to ensemble the scores, ex: mean",
    { x: String => allowedStrategies(x) })

  def getStrategy: String = $(strategy)

  def setStrategy(value: String): this.type = set(strategy, value)

  setDefault(strategy -> "mean")

  val collapseGroup = new BooleanParam(
    this, "collapseGroup", "Whether to collapse all items in group to one entry")

  def getCollapseGroup: Boolean = $(collapseGroup)

  def setCollapseGroup(value: Boolean): this.type = set(collapseGroup, value)

  val vectorDims = new StringIntMapParam(this, "vectorDims",
    "the dimensions of any vector columns, used to avoid materialization")

  def getVectorDims: Map[String, Int] = get(vectorDims).getOrElse(Map())

  def setVectorDims(value: Map[String, Int]): this.type = set(vectorDims, value)

  def setVectorDims(value: java.util.HashMap[String, Int]): this.type = set(vectorDims, value.asScala.toMap)

  setDefault(collapseGroup -> true)

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({

      if (get(colNames).isEmpty) {
        setDefault(colNames -> getCols.map(name => s"$getStrategy($name)"))
      }

      transformSchema(dataset.schema)

      val strategyToFloatFunction = Map(
        "mean" -> { (x: String, y: String) => mean(x).alias(y) }
      )

      val strategyToVectorFunction = Map(
        "mean" -> { (x: String, y: String) =>
          Summarizer.mean(col(x)).alias(y)
        }
      )

      val newCols = getCols.zip(getColNames).map { case (inColName, outColName) =>
        dataset.schema(inColName).dataType match {
          case _: DoubleType =>
            strategyToFloatFunction(getStrategy)(inColName, outColName)
          case _: FloatType =>
            strategyToFloatFunction(getStrategy)(inColName, outColName)
          case v if v == VectorType =>
            strategyToVectorFunction(getStrategy)(inColName, outColName)
          case t =>
            throw new IllegalArgumentException(s"Cannot operate on type $t with strategy $getStrategy")
        }
      }

      val aggregated = dataset.toDF()
        .groupBy(getKeys.head, getKeys.tail: _*)
        .agg(newCols.head, newCols.tail: _*)

      if (getCollapseGroup) {
        aggregated
      } else {
        val needToDrop = getColNames.toSet & dataset.columns.toSet
        dataset.drop(needToDrop.toList: _*).toDF().join(aggregated, getKeys)
      }
    }, dataset.columns.length)

  }

  def transformSchema(schema: StructType): StructType = {
    val colSet = getCols.toSet
    val colToNewName = getCols.zip(getColNames).toMap

    val newFields = schema.fields.flatMap { f =>
      if (!colSet(f.name)) None
      else {
        val newField = StructField(colToNewName(f.name), f.dataType)
        f.dataType match {
          case _: DoubleType => Some(newField)
          case _: FloatType => Some(newField)
          case fdt if fdt == VectorType => Some(newField)
          case t => throw new IllegalArgumentException(s"Cannot operate on type $t with strategy $getStrategy")
        }
      }
    }

    val keyFields = schema.fields.filter(f => colSet(f.name))
    val fields =
      (if (getCollapseGroup) schema.fields else keyFields).++(newFields)

    new StructType(fields)
  }

  def copy(extra: ParamMap): this.type = defaultCopy(extra)
}
