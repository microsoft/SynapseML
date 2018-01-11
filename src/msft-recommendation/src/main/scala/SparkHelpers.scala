// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.recommendation

import com.github.fommil.netlib.{BLAS => NetlibBLAS}
import com.microsoft.ml.spark.Wrappable
import org.apache.spark.SparkContext
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.linalg.BLAS
import org.apache.spark.ml.param.{DoubleParam, IntParam, ParamValidators, Params}
import org.apache.spark.ml.param.shared.{HasLabelCol, HasPredictionCol}
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.ml.tuning.ValidatorParams
import org.apache.spark.ml.util.DefaultParamsReader.Metadata
import org.apache.spark.ml.util.{DefaultParamsReader, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql._
import org.apache.spark.util.BoundedPriorityQueue

trait MsftRecommendationModelParams extends Params with ALSModelParams with HasPredictionCol

trait MsftRecommendationParams extends Wrappable with MsftRecommendationModelParams with ALSParams

trait MsftHasPredictionCol extends Params with HasPredictionCol

trait TVSplitRecommendationParams extends Wrappable with ValidatorParams {
  /**
    * Param for ratio between train and validation data. Must be between 0 and 1.
    * Default: 0.75
    *
    * @group param
    */
  val minRatingsU: IntParam = new IntParam(this, "minRatingsU",
    "min ratings for users > 0", ParamValidators.inRange(1, Integer.MAX_VALUE))

  val minRatingsI: IntParam = new IntParam(this, "minRatingsI",
    "min ratings for items > 0", ParamValidators.inRange(1, Integer.MAX_VALUE))

  val trainRatio: DoubleParam = new DoubleParam(this, "trainRatio",
    "ratio between training set and validation set (>= 0 && <= 1)", ParamValidators.inRange(0, 1))

  /** @group getParam */
  def getTrainRatio: Double = $(trainRatio)

  /** @group getParam */
  def getMinRatingsU: Int = $(minRatingsU)

  /** @group getParam */
  def getMinRatingsI: Int = $(minRatingsI)

  setDefault(trainRatio -> 0.75)
  setDefault(minRatingsU -> 1)
  setDefault(minRatingsI -> 1)
}

trait MsftRecEvaluatorParams extends Evaluator
  with HasPredictionCol with HasLabelCol with DefaultParamsWritable

object MsftRecHelper {
  def popRow(r: Row): Any = r.getDouble(1)

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

  def getRow(row: Row): Rating[Int] = Rating.apply(row.getInt(0), row.getInt(1), row.getFloat(2))

  def recommendForAllUsers(
                            alsModel: ALSModel,
                            num: Int): DataFrame =
    alsModel.recommendForAllUsers(num)

  def recommendForAllItems(
                            alsModel: ALSModel,
                            num: Int): DataFrame =
    alsModel.recommendForAllItems(num)

  def transform(
                 rank: Int,
                 userFactors: DataFrame,
                 itemFactors: DataFrame,
                 dataset: Dataset[_]): DataFrame =
    new ALSModel(Identifiable.randomUID("als"), rank, userFactors, itemFactors).transform(dataset)

}
