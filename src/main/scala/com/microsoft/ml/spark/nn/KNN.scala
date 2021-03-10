// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.nn

import breeze.linalg.{DenseVector => BDV}
import com.microsoft.ml.spark.core.contracts.{HasFeaturesCol, HasOutputCol}
import com.microsoft.ml.spark.codegen.Wrappable
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml._
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.injections.OptimizedKNNFitting
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object KNN extends DefaultParamsReadable[KNN]

trait KNNParams extends HasFeaturesCol with Wrappable with HasOutputCol {

  val valuesCol = new Param[String](this, "valuesCol",
    "column holding values for each feature (key) that will be returned when queried")

  def getValuesCol: String = $(valuesCol)

  def setValuesCol(v: String): this.type = set(valuesCol, v)

  val leafSize = new IntParam(this, "leafSize",
    "max size of the leaves of the tree")

  def getLeafSize: Int = $(leafSize)

  def setLeafSize(v: Int): this.type = set(leafSize, v)

  val k = new IntParam(this, "k",
    "number of matches to return")

  def getK: Int = $(k)

  def setK(v: Int): this.type = set(k, v)

}

class KNN(override val uid: String) extends Estimator[KNNModel] with KNNParams
  with DefaultParamsWritable with OptimizedKNNFitting {

  def this() = this(Identifiable.randomUID("KNN"))

  setDefault(featuresCol, "features")
  setDefault(valuesCol, "values")
  setDefault(outputCol, uid + "_output")
  setDefault(k, 5)
  setDefault(leafSize, 50)

  override def fit(dataset: Dataset[_]): KNNModel = {
    fitOptimized(dataset)
  }

  override def copy(extra: ParamMap): Estimator[KNNModel] =
    defaultCopy(extra)

  def transformSchema(schema: StructType): StructType = {
    schema.add(getOutputCol, ArrayType(new StructType()
      .add("value", schema(getValuesCol).dataType)
      .add("distance", DoubleType)
    ))
  }

}

class KNNModel(val uid: String) extends Model[KNNModel] with ComplexParamsWritable with KNNParams {
  def this() = this(Identifiable.randomUID("KNNModel"))

  private var broadcastedModelOption: Option[Broadcast[BallTree[_]]] = None

  val ballTree = new BallTreeParam(this, "ballTree",
    "the ballTree model used for perfoming queries", { _ => true })

  def getBallTree: BallTree[_] = $(ballTree)

  def setBallTree(v: BallTree[_]): this.type = {
    broadcastedModelOption.foreach(_.unpersist())
    broadcastedModelOption = None
    set(ballTree, v)
  }

  override def copy(extra: ParamMap): KNNModel = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    if (broadcastedModelOption.isEmpty) {
      broadcastedModelOption = Some(dataset.sparkSession.sparkContext.broadcast(getBallTree))
    }
    val getNeighborUDF = UDFUtils.oldUdf({ dv: DenseVector =>
      val bt = broadcastedModelOption.get.value
      bt.findMaximumInnerProducts(new BDV(dv.values), getK)
        .map(bm => Row(bt.values(bm.index), bm.distance))
    }, ArrayType(new StructType()
      .add("value", dataset.schema(getValuesCol).dataType)
      .add("distance", DoubleType)
    ))

    dataset.toDF().withColumn(getOutputCol, getNeighborUDF(col(getFeaturesCol)))
  }

  override def transformSchema(schema: StructType): StructType = {
    schema.add(getOutputCol, ArrayType(new StructType()
      .add("value", schema(getValuesCol).dataType)
      .add("distance", DoubleType)
    ))
  }

}

object KNNModel extends ComplexParamsReadable[KNNModel]
