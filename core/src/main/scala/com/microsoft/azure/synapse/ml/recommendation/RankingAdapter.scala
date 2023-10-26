// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.recommendation

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts.HasLabelCol
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.{EstimatorParam, TransformerParam}
import org.apache.spark.ml._
import org.apache.spark.ml.param._
import org.apache.spark.ml.recommendation._
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{collect_list, rank => r, _}
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, StructType}

trait RankingParams extends HasRecommenderCols with HasLabelCol with hasK {
  val minRatingsPerUser: IntParam =
    new IntParam(this, "minRatingsPerUser", "min ratings for users > 0", ParamValidators.inRange(0, Integer.MAX_VALUE))

  /** @group setParam */
  def setMinRatingsPerUser(value: Int): this.type = set(minRatingsPerUser, value)

  /** @group getParam */
  def getMinRatingsPerUser: Int = $(minRatingsPerUser)

  val minRatingsPerItem: IntParam =
    new IntParam(this, "minRatingsPerItem", "min ratings for items > 0", ParamValidators.inRange(0, Integer.MAX_VALUE))

  /** @group setParam */
  def setMinRatingsPerItem(value: Int): this.type = set(minRatingsPerItem, value)

  /** @group getParam */
  def getMinRatingsPerItem: Int = $(minRatingsPerItem)

  val recommender = new EstimatorParam(this, "recommender", "estimator for selection", { _: Estimator[_] => true })

  /** @group getParam */
  def getRecommender: Estimator[_ <: Model[_]] = $(recommender)

  /** @group setParam */
  def setRecommender(value: Estimator[_ <: Model[_]]): this.type = set(recommender, value)

  setDefault(minRatingsPerUser -> 1, minRatingsPerItem -> 1, k -> 10, labelCol -> "label")
}

trait Mode extends HasRecommenderCols {

  val mode: Param[String] = new Param(this, "mode", "recommendation mode")

  /** @group getParam */
  def getMode: String = $(mode)

  /** @group setParam */
  def setMode(value: String): this.type = set(mode, value)

  setDefault(mode -> "allUsers")

  def transformSchema(schema: StructType): StructType = {
    new StructType()
      .add(getUserCol, IntegerType)
      .add("recommendations", ArrayType(
        new StructType().add(getItemCol, IntegerType).add("rating", FloatType))
      )
  }
}

class RankingAdapter(override val uid: String)
  extends Estimator[RankingAdapterModel] with ComplexParamsWritable
    with RankingParams with Mode with Wrappable with SynapseMLLogging {
  logClass(FeatureNames.Recommendation)

  def this() = this(Identifiable.randomUID("RecommenderAdapter"))

  override def setRecommender(value: Estimator[_ <: Model[_]]): this.type = {
    val v = value.asInstanceOf[Estimator[_ <: Model[_]] with RecommendationParams]
    v.get(v.userCol).map(setUserCol)
    v.get(v.ratingCol).map(setRatingCol)
    v.get(v.itemCol).map(setItemCol)
    super.setRecommender(v)
  }

  def fit(dataset: Dataset[_]): RankingAdapterModel = {
    transformSchema(dataset.schema)
    logFit({
      new RankingAdapterModel()
        .setRecommenderModel(getRecommender.fit(dataset))
        .setMode(getMode)
        .setK(getK)
        .setUserCol(getUserCol)
        .setItemCol(getItemCol)
        .setRatingCol(getRatingCol)
        .setLabelCol(getLabelCol)
    }, dataset.columns.length)
  }

  override def transformSchema(schema: StructType): StructType = {
    // Trigger the updating of parameters
    // in case python parameterizes the call
    get(recommender).foreach(setRecommender)
    super.transformSchema(schema)
  }

  override def copy(extra: ParamMap): RankingAdapter = {
    defaultCopy(extra)
  }

}

object RankingAdapter extends ComplexParamsReadable[RankingAdapter]

/**
  * Model from train validation split.
  *
  * @param uid Id.
  */
class RankingAdapterModel private[ml](val uid: String)
  extends Model[RankingAdapterModel] with ComplexParamsWritable
    with Wrappable with RankingParams with Mode with SynapseMLLogging {
  logClass(FeatureNames.Recommendation)

  def this() = this(Identifiable.randomUID("RankingAdapterModel"))

  val recommenderModel = new TransformerParam(this, "recommenderModel", "recommenderModel", { _: Transformer => true })

  def setRecommenderModel(m: Model[_]): this.type = set(recommenderModel, m)

  def getRecommenderModel: Model[_] = $(recommenderModel).asInstanceOf[Model[_]]

  def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      transformSchema(dataset.schema)

      val windowSpec = Window.partitionBy(getUserCol).orderBy(col(getRatingCol).desc, col(getItemCol))

      val perUserActualItemsDF = dataset
        .withColumn("rank", r().over(windowSpec).alias("rank"))
        .where(col("rank") <= getK)
        .groupBy(getUserCol)
        .agg(col(getUserCol), collect_list(col(getItemCol)).as(getLabelCol))
        .select(getUserCol, getLabelCol)

      val recs = getMode match {
        case "allUsers" =>
          this.getRecommenderModel match {
            case als: ALSModel => als.recommendForAllUsers(getK)
            case sar: SARModel => sar.recommendForAllUsers(getK)
          }
        case "normal" => SparkHelpers.flatten(getRecommenderModel.transform(dataset), getK, getItemCol, getUserCol)
      }

      recs
        .select(col(getUserCol), col("recommendations." + getItemCol).as("prediction"))
        .join(perUserActualItemsDF, getUserCol)
        .drop(getUserCol)
    }, dataset.columns.length)
  }

  override def copy(extra: ParamMap): RankingAdapterModel = {
    defaultCopy(extra)
  }

  def recommendForAllUsers(k: Int): DataFrame = getRecommenderModel.asInstanceOf[ALSModel].recommendForAllUsers(k)

}

object RankingAdapterModel extends ComplexParamsReadable[RankingAdapterModel]
