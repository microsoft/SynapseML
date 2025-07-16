// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.sql.types.injections

import breeze.linalg.{DenseVector => BDV}
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import com.microsoft.azure.synapse.ml.nn._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.types._

trait OptimizedCKNNFitting extends ConditionalKNNParams with SynapseMLLogging {

  private def fitGeneric[V, L](dataset: Dataset[_]): ConditionalKNNModel = {

    val kvlTriples = dataset.toDF().select(getFeaturesCol, getValuesCol, getLabelCol).collect()
      .map { row =>
        val bdv = new BDV(row.getAs[Vector](getFeaturesCol).toDense.values)
        val value = row.getAs[V](getValuesCol)
        val label = row.getAs[L](getLabelCol)
        (bdv, value, label)
      }
    val ballTree = ConditionalBallTree(
      kvlTriples.map(_._1), kvlTriples.map(_._2), kvlTriples.map(_._3), getLeafSize)
    new ConditionalKNNModel()
      .setFeaturesCol(getFeaturesCol)
      .setValuesCol(getValuesCol)
      .setBallTree(ballTree)
      .setOutputCol(getOutputCol)
      .setLabelCol(getLabelCol)
      .setConditionerCol(getConditionerCol)
      .setK(getK)
  }

  protected def fitOptimized(dataset: Dataset[_]): ConditionalKNNModel = {

    val vt = PhysicalDataType.apply(dataset.schema(getValuesCol).dataType)
    val lt = PhysicalDataType.apply(dataset.schema(getLabelCol).dataType)
    (vt, lt) match {
      case (avt: PhysicalDataType, alt: PhysicalDataType) => fitGeneric[avt.InternalType, alt.InternalType](dataset)
      case (avt: PhysicalDataType, _) => fitGeneric[avt.InternalType, Any](dataset)
      case (_, alt: PhysicalDataType) => fitGeneric[Any, alt.InternalType](dataset)
      case _ => fitGeneric[Any, Any](dataset)
    }
  }

}

trait OptimizedKNNFitting extends KNNParams with SynapseMLLogging {

  private def fitGeneric[V](dataset: Dataset[_]): KNNModel = {

    val kvlTuples = dataset.toDF().select(getFeaturesCol, getValuesCol).collect()
      .map { row =>
        val bdv = new BDV(row.getAs[Vector](getFeaturesCol).toDense.values)
        val value = row.getAs[V](getValuesCol)
        (bdv, value)
      }
    val ballTree = BallTree(
      kvlTuples.map(_._1), kvlTuples.map(_._2), getLeafSize)
    new KNNModel()
      .setFeaturesCol(getFeaturesCol)
      .setValuesCol(getValuesCol)
      .setBallTree(ballTree)
      .setOutputCol(getOutputCol)
      .setK(getK)
  }

  protected def fitOptimized(dataset: Dataset[_]): KNNModel = {

    PhysicalDataType.apply(dataset.schema(getValuesCol).dataType) match {
      case avt: PhysicalDataType => fitGeneric[avt.InternalType](dataset)
      case _ => fitGeneric[Any](dataset)
    }
  }

}
