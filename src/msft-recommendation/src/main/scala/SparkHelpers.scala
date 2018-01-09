// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.recommendation

import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.BLAS
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.param.shared.HasPredictionCol
import org.apache.spark.ml.util.DefaultParamsReader
import org.apache.spark.ml.util.DefaultParamsReader.Metadata
import org.apache.spark.sql.Dataset
import com.github.fommil.netlib.{BLAS => NetlibBLAS}
import org.apache.spark.util.BoundedPriorityQueue

trait MsftRecommendationModelParams extends Params with ALSModelParams with HasPredictionCol

trait MsftRecommendationParams extends MsftRecommendationModelParams with ALSParams

trait MsftHasPredictionCol extends Params with HasPredictionCol

object MsftRecHelper {

  private def blockify(
                        factors: Dataset[(Int, Array[Float])],
                        blockSize: Int = 4096): Dataset[Seq[(Int, Array[Float])]] = {
    import factors.sparkSession.implicits._
    factors.mapPartitions(_.grouped(blockSize))
  }

  def loadMetadata(path: String, sc: SparkContext, className: String = ""): Metadata =
    DefaultParamsReader.loadMetadata(path, sc, className)

  def getAndSetParams(model: Params, metadata: Metadata): Unit =
    DefaultParamsReader.getAndSetParams(model, metadata)

  val f2jBLAS: NetlibBLAS = BLAS.f2jBLAS

  def getTopByKeyAggregator(num: Int, ord: Ordering[(Int, Float)]): TopByKeyAggregator[Int, Int, Float] =
    new TopByKeyAggregator[Int, Int, Float](num, ord)

  def getBoundedPriorityQueue(maxSize: Int)(implicit ord: Ordering[(Int, Float)]): BoundedPriorityQueue[(Int, Float)] =
    new BoundedPriorityQueue[(Int, Float)](maxSize)(Ordering.by(_._2))
}
