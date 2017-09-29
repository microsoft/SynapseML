// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.functions.{max, col, lit}
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.reflect.runtime.universe.{TypeTag, typeTag}

class ClassBalancer(val uid: String) extends Estimator[ClassBalancerModel]
  with DefaultParamsWritable with HasInputCol with HasOutputCol {

  def this() = this(Identifiable.randomUID("ClassBalancer"))

  setDefault(outputCol->"weight")

  def fit(dataset: Dataset[_]): ClassBalancerModel = {
    val counts = dataset.toDF().select(getInputCol).groupBy(getInputCol).count()
    val maxVal = counts.agg(max("count")).collect().head.getLong(0)
    val weights = counts
      .withColumn(getOutputCol, lit(maxVal) / col("count"))
      .select(getInputCol, getOutputCol)
    new ClassBalancerModel(uid, getInputCol, getOutputCol, weights).setParent(this)
  }

  override def copy(extra: ParamMap): Estimator[ClassBalancerModel] = defaultCopy(extra)

  def transformSchema(schema: StructType): StructType = schema.add(getOutputCol, DoubleType)
}

object ClassBalancer extends DefaultParamsReadable[ClassBalancer]

class ClassBalancerModel(val uid: String, val inputCol: String,
                         val outputCol: String, val weights: DataFrame)
  extends Model[ClassBalancerModel] with ConstructorWritable[ClassBalancerModel] {

  override def copy(extra: ParamMap): ClassBalancerModel = defaultCopy(extra)

  val ttag: TypeTag[ClassBalancerModel] = typeTag[ClassBalancerModel]

  def objectsToSave: List[Any] = List(uid, inputCol, outputCol, weights)

  def transformSchema(schema: StructType): StructType = schema.add(outputCol, DoubleType)

  def transform(dataset: Dataset[_]): DataFrame = {
    dataset.toDF().join(weights, inputCol)
  }
}

object ClassBalancerModel extends ConstructorReadable[ClassBalancerModel]
