// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.nn

import breeze.linalg.{DenseVector => BDV}
import com.microsoft.ml.spark.core.contracts.HasLabelCol
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.param.{ConditionalBallTreeParam, Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Estimator, Model}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.injections.OptimizedCKNNFitting
import org.apache.spark.sql.{DataFrame, Dataset, Row}

trait ConditionalKNNParams extends KNNParams with HasLabelCol {
  val conditionerCol = new Param[String](this, "conditionerCol",
    "column holding identifiers for features that will be returned when queried")

  def getConditionerCol: String = $(conditionerCol)

  def setConditionerCol(v: String): this.type = set(conditionerCol, v)
}

object ConditionalKNN extends DefaultParamsReadable[ConditionalKNN]

class ConditionalKNN(override val uid: String) extends Estimator[ConditionalKNNModel]
  with ConditionalKNNParams with DefaultParamsWritable with OptimizedCKNNFitting {

  def this() = this(Identifiable.randomUID("ConditionalKNN"))

  setDefault(featuresCol, "features")
  setDefault(valuesCol, "values")
  setDefault(outputCol, uid + "_output")
  setDefault(k, 5)
  setDefault(leafSize, 50)
  setDefault(labelCol, "labels")
  setDefault(conditionerCol, "conditioner")

  override def fit(dataset: Dataset[_]): ConditionalKNNModel = {
    fitOptimized(dataset)
  }

  override def copy(extra: ParamMap): Estimator[ConditionalKNNModel] =
    defaultCopy(extra)

  def transformSchema(schema: StructType): StructType = {
    schema.add(getOutputCol, ArrayType(new StructType()
      .add("value", schema(getValuesCol).dataType)
      .add("distance", DoubleType)
      .add("label", schema(getLabelCol).dataType)
    ))
  }

}

private[ml] object KNNFuncHolder {
  def queryFunc[L, V](bbt: Broadcast[ConditionalBallTree[L, V]], k: Int)
                     (dv: DenseVector, conditioner: Seq[L]): Seq[Row] = {
    bbt.value.findMaximumInnerProducts(new BDV(dv.values), conditioner.toSet, k)
      .map(bm => Row(bbt.value.values(bm.index), bm.distance, bbt.value.labels(bm.index)))
  }

}

class ConditionalKNNModel(val uid: String) extends Model[ConditionalKNNModel]
  with ComplexParamsWritable with ConditionalKNNParams {
  def this() = this(Identifiable.randomUID("ConditionalKNNModel"))

  private var broadcastedModelOption: Option[Broadcast[ConditionalBallTree[_, _]]] = None

  val ballTree = new ConditionalBallTreeParam(this, "ballTree",
    "the ballTree model used for perfoming queries", { _ => true })

  def getBallTree: ConditionalBallTree[_, _] = $(ballTree)

  def setBallTree(v: ConditionalBallTree[_, _]): this.type = {
    broadcastedModelOption.foreach(_.unpersist())
    broadcastedModelOption = None
    set(ballTree, v)
  }

  override def copy(extra: ParamMap): ConditionalKNNModel = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    if (broadcastedModelOption.isEmpty) {
      broadcastedModelOption = Some(dataset.sparkSession.sparkContext.broadcast(getBallTree))
    }
    val getNeighborUDF = udf(KNNFuncHolder.queryFunc[Any, Any](
      broadcastedModelOption.get.asInstanceOf[Broadcast[ConditionalBallTree[Any, Any]]], getK) _,
      ArrayType(new StructType()
        .add("value", dataset.schema(getValuesCol).dataType)
        .add("distance", DoubleType)
        .add("label", dataset.schema(getLabelCol).dataType)
      ))

    dataset.toDF().withColumn(getOutputCol, getNeighborUDF(col(getFeaturesCol), col(getConditionerCol)))
  }

  override def transformSchema(schema: StructType): StructType = {
    schema.add(getOutputCol, ArrayType(new StructType()
      .add("value", schema(getValuesCol).dataType)
      .add("distance", DoubleType)
      .add("label", schema(getLabelCol).dataType)
    ))
  }

}

object ConditionalKNNModel extends ComplexParamsReadable[ConditionalKNNModel]
