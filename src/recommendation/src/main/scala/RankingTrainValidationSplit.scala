// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.tuning

import com.microsoft.ml.spark.{RankingAdapter, RankingEvaluator}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.{Dataset, RankingDataset}

class RankingTrainValidationSplit extends TrainValidationSplit
  with ComplexParamsWritable
  with RankingTuningTrait {
  override def fit(dataset: Dataset[_]): RankingTrainValidationSplitModel = {
    getEstimator.asInstanceOf[RankingAdapter]
      .setK(getEvaluator.asInstanceOf[RankingEvaluator].getK)

    val rankingDF = RankingDataset.toRankingDataSet[Any](dataset)
      .setUserCol(getUserCol)
      .setItemCol(getItemCol)
      .setRatingCol(getRatingCol)

    val model = super.fit(rankingDF)
    new RankingTrainValidationSplitModel("rtvs", model.bestModel, model.validationMetrics)
  }

  /** @group setParam */
  override def setEstimator(value: Estimator[_]): this.type = {
    val ranking = new RankingAdapter()
      .setRecommender(value.asInstanceOf[Estimator[_ <: Model[_]]])
    set(estimator, ranking)
  }
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
