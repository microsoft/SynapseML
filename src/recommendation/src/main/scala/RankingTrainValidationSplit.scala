// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.tuning

import com.microsoft.ml.spark.{RankingAdapter, RankingEvaluator}
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.{EstimatorParam, Param}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.{Dataset, RankingDataset}

class RankingTrainValidationSplit extends TrainValidationSplit
  with ComplexParamsWritable
  with RankingTuningTrait {
  override def fit(dataset: Dataset[_]): RankingTrainValidationSplitModel = {
    val estimator = getEstimator
    val rankingEstimator = new RankingAdapter()
      .setRecommender(estimator.asInstanceOf[Estimator[_ <: Model[_]]])
      .setK(getEvaluator.asInstanceOf[RankingEvaluator].getK)

    setEstimator(rankingEstimator)
    val rankingDF = RankingDataset.toRankingDataSet[Any](dataset)
      .setUserCol(getUserCol)
      .setItemCol(getItemCol)
      .setRatingCol(getRatingCol)

    val trainedModel = super.fit(rankingDF)

    val subModels: Option[Array[Model[_]]] = if (super.getCollectSubModels) {
      Some(trainedModel.subModels)
    } else None

    val model = new RankingTrainValidationSplitModel("rtvs", trainedModel.bestModel, trainedModel.validationMetrics)
    model.setSubModels(subModels)

    setEstimator(estimator)
    model
  }

  /** @group setParam */
  override def setEvaluator(value: Evaluator): this.type = set(evaluator, value)

//  override val estimator = new EstimatorParam(this, "estimator", "estimator for selection")

  /** @group setParam */
  def setEstimator(value: Estimator[_ <: Model[_]]): this.type = set(estimator, value)

    /** @group getParam */
  override def getEstimator: Estimator[_] = $(estimator)

}

object RankingTrainValidationSplit extends ComplexParamsReadable[RankingTrainValidationSplit]

class RankingTrainValidationSplitModel(
  override val uid: String,
  override val bestModel: Model[_],
  override val validationMetrics: Array[Double])
  extends TrainValidationSplitModel(uid, bestModel, validationMetrics)
    with MLWritable with RankingTuningModelTrait {
  override def write: TrainValidationSplitModel.TrainValidationSplitModelWriter = {
    new TrainValidationSplitModel.TrainValidationSplitModelWriter(this)
  }
}

object RankingTrainValidationSplitModel extends ComplexParamsReadable[RankingTrainValidationSplitModel]
