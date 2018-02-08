// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.Model
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.recommendation.{ALSModel, MsftRecommendationModelParams}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
  * Model fitted by ALS.
  *
  * @param rank        rank of the matrix factorization model
  * @param userFactors a DataFrame that stores user factors in two columns: `id` and `features`
  * @param itemFactors a DataFrame that stores item factors in two columns: `id` and `features`
  * @param alsModel    a trained ALS model
  */
class MsftRecommendationModel(
                               override val uid: String,
                               val rank: Int,
                               val userFactors: DataFrame,
                               val itemFactors: DataFrame,
                               val alsModel: ALSModel)
  extends Model[MsftRecommendationModel] with MsftRecommendationModelParams
    with ConstructorWritable[MsftRecommendationModel] {

  def this() = this(Identifiable.randomUID("SARModel"), 1, null, null, null)

  /**
    * Returns top `numItems` items recommended for each user, for all users.
    *
    * @param numItems max number of recommendations for each user
    * @return a DataFrame of (userCol: Int, recommendations), where recommendations are
    *         stored as an array of (itemCol: Int, rating: Float) Rows.
    */
  override def recommendForAllUsers(numItems: Int): DataFrame = {
    recommendForAllUsers(alsModel, numItems)
  }

  /**
    * Returns top `numUsers` users recommended for each item, for all items.
    *
    * @param numUsers max number of recommendations for each item
    * @return a DataFrame of (itemCol: Int, recommendations), where recommendations are
    *         stored as an array of (userCol: Int, rating: Float) Rows.
    */
  override def recommendForAllItems(numUsers: Int): DataFrame = {
    recommendForAllItems(alsModel, numUsers)
  }

  override def copy(extra: ParamMap): MsftRecommendationModel = {
    val copied = new MsftRecommendationModel(uid, rank, userFactors, itemFactors, alsModel)
    copyValues(copied, extra).setParent(parent)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    alsModel.transform(dataset)
  }

  override def transformSchema(schema: StructType): StructType =
    alsModel.transformSchema(schema)

  override val ttag: TypeTag[MsftRecommendationModel] = typeTag[MsftRecommendationModel]

  override def objectsToSave: List[AnyRef] = List(uid, Int.box(rank), userFactors, itemFactors, alsModel)
}

object MsftRecommendationModel extends ConstructorReadable[MsftRecommendationModel] {
  private val NaN = "nan"
  private val Drop = "drop"
}
