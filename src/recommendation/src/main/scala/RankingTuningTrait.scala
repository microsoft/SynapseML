// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.tuning

import com.microsoft.ml.spark.{RankingAdapter, RankingAdapterModel}
import org.apache.spark.ml.Model
import org.apache.spark.ml.recommendation.HasRecommenderCols
import org.apache.spark.sql.DataFrame

trait RankingTuningTrait extends HasRecommenderCols with ValidatorParams{

  /** @group getParam */
  override def getItemCol: String = getEstimator.asInstanceOf[RankingAdapter].getItemCol

  /** @group getParam */
  override def getUserCol: String = getEstimator.asInstanceOf[RankingAdapter].getUserCol

  /** @group getParam */
  override def getRatingCol: String = getEstimator.asInstanceOf[RankingAdapter].getRatingCol

}

trait RankingTuningModelTrait{
  val bestModel: Model[_]

  def recommendForAllUsers(k: Int): DataFrame =
    bestModel
      .asInstanceOf[RankingAdapterModel]
      .recommendForAllUsers(k)

  def recommendForAllItems(k: Int): DataFrame =
    bestModel
      .asInstanceOf[RankingAdapterModel]
      .recommendForAllItems(k)
}
