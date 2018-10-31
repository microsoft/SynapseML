// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.tuning

import com.microsoft.ml.spark.{RankingAdapter, RankingAdapterModel, RankingEvaluator}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.recommendation.{ALS, HasRecommenderCols}
import org.apache.spark.sql.{DataFrame, Dataset, RankingDataset}

class RankingCrossValidator extends CrossValidator with HasRecommenderCols {
  override def fit(dataset: Dataset[_]): RankingCrossValidatorModel = {
    val rankingDF = RankingDataset.toRankingDataSet[Any](dataset)
      .setUserCol(getUserCol)
      .setItemCol(getItemCol)
      .setRatingCol(getRatingCol)

    val model = super.fit(rankingDF)
    new RankingCrossValidatorModel("rtvs", model.bestModel, model.avgMetrics)
  }

  /** @group getParam */
  override def getItemCol: String = getEstimator.asInstanceOf[RankingAdapter].getItemCol

  /** @group getParam */
  override def getUserCol: String = getEstimator.asInstanceOf[RankingAdapter].getUserCol

  /** @group getParam */
  override def getRatingCol: String = getEstimator.asInstanceOf[RankingAdapter].getRatingCol

  /** @group setParam */
  override def setEstimator(value: Estimator[_]): this.type = {
    val ranking = new RankingAdapter()
      .setRecommender(value.asInstanceOf[Estimator[_ <: Model[_]]])
      .setK(getEvaluator.asInstanceOf[RankingEvaluator].getK)
    set(estimator, ranking)
  }

}

class RankingCrossValidatorModel(
  override val uid: String,
  override val bestModel: Model[_],
  override val avgMetrics: Array[Double]) extends CrossValidatorModel(uid, bestModel, avgMetrics) {
  def recommendForAllUsers(k: Int): DataFrame =
    bestModel
      .asInstanceOf[RankingAdapterModel]
      .recommendForAllUsers(k)

  def recommendForAllItems(k: Int): DataFrame =
    bestModel
      .asInstanceOf[RankingAdapterModel]
      .recommendForAllItems(k)
}
