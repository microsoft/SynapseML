// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.param._
import org.apache.spark.ml.recommendation._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{collect_list, rank => r}
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, StructType}

import scala.collection.mutable
import scala.util.Random

trait RankingParams extends kTrait {
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

  val recommender = new EstimatorParam(this, "recommender", "estimator for selection", { x: Estimator[_] => true })

  /** @group getParam */
  def getRecommender: Estimator[_ <: Model[_]] = $(recommender)

  /** @group setParam */
  def setRecommender(value: Estimator[_ <: Model[_]]): this.type = set(recommender, value)
}

trait RankingFunctions extends RankingParams with HasRecommenderCols with HasLabelCol {

  lazy val userStructType: StructType = new StructType()
    .add(getUserCol, IntegerType)
    .add("recommendations", ArrayType(
      new StructType().add(getItemCol, IntegerType).add("rating", FloatType))
    )

  lazy val itemStructType: StructType = new StructType()
    .add(getItemCol, IntegerType)
    .add("recommendations", ArrayType(
      new StructType().add(getUserCol, IntegerType).add("rating", FloatType))
    )

  setDefault(labelCol -> "label")

  def prepareTestData(validationDataset: DataFrame, recs: DataFrame, k: Int): Dataset[_] = {

    val rankingCol = if (validationDataset.columns.contains(getRatingCol)) getRatingCol
    else getItemCol

    val windowSpec = Window.partitionBy(getUserCol).orderBy(col(rankingCol).desc)

    val perUserActualItemsDF = validationDataset
      .withColumn("rank", r().over(windowSpec).alias("rank"))
      .where(col("rank") <= k)
      .groupBy(getUserCol)
      .agg(col(getUserCol), collect_list(col(getItemCol)).as(getLabelCol))
      .select(getUserCol, getLabelCol)

    recs
      .select(getUserCol, "recommendations." + getItemCol)
      .withColumnRenamed(getItemCol, "prediction")
      .join(perUserActualItemsDF, getUserCol)
      .drop(getUserCol)
  }
}

class RankingAdapter(override val uid: String)
  extends Estimator[RankingAdapterModel] with ComplexParamsWritable with RankingFunctions {

  def this() = this(Identifiable.randomUID("RecommenderAdapter"))

  /** @group getParam */
  override def getUserCol: String = getRecommender.asInstanceOf[Estimator[_] with PublicALSParams].getUserCol

  /** @group getParam */
  override def getItemCol: String = getRecommender.asInstanceOf[Estimator[_] with PublicALSParams].getItemCol

  /** @group getParam */
  override def getRatingCol: String = getRecommender.asInstanceOf[Estimator[_] with PublicALSParams].getRatingCol

  val mode: Param[String] = new Param(this, "mode", "recommendation mode")

  /** @group getParam */
  def getMode: String = $(mode)

  /** @group setParam */
  def setMode(value: String): this.type = set(mode, value)

  setDefault(mode -> "allUsers", k -> 10)

  def transformSchema(schema: StructType): StructType = {
    getMode match {
      case "allUsers" => userStructType
      case "allItems" => itemStructType
      case "normal"   => userStructType
    }
  }

  def fit(dataset: Dataset[_]): RankingAdapterModel = {
    new RankingAdapterModel()
      .setRecommenderModel(getRecommender.fit(dataset))
      .setMode(getMode)
      .setNItems(getK)
      .setUserCol(getUserCol)
      .setItemCol(getItemCol)
      .setRatingCol(getRatingCol)
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
  extends Model[RankingAdapterModel] with ComplexParamsWritable with Wrappable with RankingFunctions {

  def recommendForAllUsers(i: Int): DataFrame = getRecommenderModel.asInstanceOf[ALSModel].recommendForAllUsers(3)

  def recommendForAllItems(i: Int): DataFrame = getRecommenderModel.asInstanceOf[ALSModel].recommendForAllItems(3)

  def this() = this(Identifiable.randomUID("RankingAdapterModel"))

  val recommenderModel = new TransformerParam(this, "recommenderModel", "recommenderModel", { x: Transformer => true })

  def setRecommenderModel(m: Model[_]): this.type = set(recommenderModel, m)

  def getRecommenderModel: Model[_] = $(recommenderModel).asInstanceOf[Model[_]]

  val mode: Param[String] = new Param(this, "mode", "recommendation mode")

  /** @group getParam */
  def getMode: String = $(mode)

  /** @group setParam */
  def setMode(value: String): this.type = set(mode, value)

  val nItems: IntParam = new IntParam(this, "nItems", "recommendation mode")

  /** @group getParam */
  def getNItems: Int = $(nItems)

  /** @group setParam */
  def setNItems(value: Int): this.type = set(nItems, value)

  val nUsers: IntParam = new IntParam(this, "nUsers", "recommendation mode")

  /** @group getParam */
  def getNUsers: Int = $(nUsers)

  /** @group setParam */
  def setNUsers(value: Int): this.type = set(nUsers, value)

  def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema)
    val model = getRecommenderModel
    getMode match {
      case "allUsers" => {
        val recs = this.recommendForAllUsers(getNItems)
        prepareTestData(dataset.toDF(), recs, getK).toDF()
      }
      case "allItems" => {
        val recs = this.recommendForAllItems(getNUsers)
        prepareTestData(dataset.toDF(), recs, getK).toDF()
      }
      case "normal"   => {
        val recs = SparkHelper.flatten(model.transform(dataset), getK, getItemCol, getUserCol)
        prepareTestData(dataset.toDF(), recs, getK).toDF()
      }
    }
  }

  def transformSchema(schema: StructType): StructType = {
    getMode match {
      case "allUsers" => userStructType
      case "allItems" => itemStructType
      case "normal"   => userStructType
    }
  }

  override def copy(extra: ParamMap): RankingAdapterModel = {
    defaultCopy(extra)
  }

}

object RankingAdapterModel extends ComplexParamsReadable[RankingAdapterModel]
