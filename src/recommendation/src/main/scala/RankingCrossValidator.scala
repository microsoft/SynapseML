// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.tuning

import com.microsoft.ml.spark.{RankingAdapter, RankingAdapterModel, RankingEvaluator}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.recommendation.{ALS, HasRecommenderCols}
import org.apache.spark.sql.{DataFrame, Dataset, RankingDataset}

class RankingCrossValidator extends CrossValidator with RankingTuningTrait {
  override def fit(dataset: Dataset[_]): RankingCrossValidatorModel = {
    getEstimator.asInstanceOf[RankingAdapter]
      .setK(getEvaluator.asInstanceOf[RankingEvaluator].getK)

    val rankingDF = RankingDataset.toRankingDataSet[Any](dataset)
      .setUserCol(getUserCol)
      .setItemCol(getItemCol)
      .setRatingCol(getRatingCol)

    val model = super.fit(rankingDF)
    new RankingCrossValidatorModel("rtvs", model.bestModel, model.avgMetrics)
  }

  /** @group setParam */
  override def setEstimator(value: Estimator[_]): this.type = {
    val ranking = new RankingAdapter()
      .setRecommender(value.asInstanceOf[Estimator[_ <: Model[_]]])
    set(estimator, ranking)
  }

}

class RankingCrossValidatorModel(
  override val uid: String,
  override val bestModel: Model[_],
  override val avgMetrics: Array[Double])
  extends CrossValidatorModel(uid, bestModel, avgMetrics)
  with RankingTuningModelTrait
