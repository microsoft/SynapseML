// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.sql.types.injections

import com.microsoft.ml.spark.nn._
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.Dataset
import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._

trait OptimizedCKNNFitting extends ConditionalKNNParams with Logging{

  private def fitGeneric[V, L](dataset: Dataset[_]): ConditionalKNNModel = {
    logInfo("Calling function fitGeneric --- telemetry record")
    val kvlTriples = dataset.toDF().select(getFeaturesCol, getValuesCol, getLabelCol).collect()
      .map { row =>
        val bdv = new BDV(row.getAs[DenseVector](getFeaturesCol).values)
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
    logInfo("Calling function fitOptimized --- telemetry record")
    val vt = dataset.schema(getValuesCol).dataType
    val lt = dataset.schema(getLabelCol).dataType
    (vt, lt) match {
      case (avt: AtomicType, alt: AtomicType) => fitGeneric[avt.InternalType, alt.InternalType](dataset)
      case (avt: AtomicType, _) => fitGeneric[avt.InternalType, Any](dataset)
      case (_, alt: AtomicType) => fitGeneric[Any, alt.InternalType](dataset)
      case _ => fitGeneric[Any, Any](dataset)
    }
  }

}

trait OptimizedKNNFitting extends KNNParams with Logging{

  private def fitGeneric[V](dataset: Dataset[_]): KNNModel = {
    logInfo("Calling function fitGeneric --- telemetry record")
    val kvlTuples = dataset.toDF().select(getFeaturesCol, getValuesCol).collect()
      .map { row =>
        val bdv = new BDV(row.getAs[DenseVector](getFeaturesCol).values)
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
    logInfo("Calling function fitOptimized --- telemetry record")
    dataset.schema(getValuesCol).dataType match {
      case avt: AtomicType => fitGeneric[avt.InternalType](dataset)
      case _ => fitGeneric[Any](dataset)
    }
  }

}

