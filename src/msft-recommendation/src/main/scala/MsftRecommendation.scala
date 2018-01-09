// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.{util => ju}

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.ml.recommendation.{MsftRecHelper, MsftRecommendationModelParams, _}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/** Featurize text.
  *
  * @param uid The id of the module
  */
class MsftRecommendation(override val uid: String) extends Estimator[MsftRecommendationModel]
  with MsftRecommendationParams with MsftHasPredictionCol with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("msftRecommendation"))

  /** @group setParam */
  def setRank(value: Int): this.type = set(rank, value)

  /** @group setParam */
  def setNumUserBlocks(value: Int): this.type = set(numUserBlocks, value)

  /** @group setParam */
  def setNumItemBlocks(value: Int): this.type = set(numItemBlocks, value)

  /** @group setParam */
  def setImplicitPrefs(value: Boolean): this.type = set(implicitPrefs, value)

  /** @group setParam */
  def setAlpha(value: Double): this.type = set(alpha, value)

  /** @group setParam */
  def setUserCol(value: String): this.type = set(userCol, value)

  /** @group setParam */
  def setItemCol(value: String): this.type = set(itemCol, value)

  /** @group setParam */
  def setRatingCol(value: String): this.type = set(ratingCol, value)

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  def setRegParam(value: Double): this.type = set(regParam, value)

  /** @group setParam */
  def setNonnegative(value: Boolean): this.type = set(nonnegative, value)

  /** @group setParam */
  def setCheckpointInterval(value: Int): this.type = set(checkpointInterval, value)

  /** @group setParam */
  def setSeed(value: Long): this.type = set(seed, value)

  /** @group expertSetParam */
  def setIntermediateStorageLevel(value: String): this.type = set(intermediateStorageLevel, value)

  /** @group expertSetParam */
  def setFinalStorageLevel(value: String): this.type = set(finalStorageLevel, value)

  /** @group expertSetParam */
  def setColdStartStrategy(value: String): this.type = set(coldStartStrategy, value)

  /**
    * Sets both numUserBlocks and numItemBlocks to the specific value.
    *
    * @group setParam
    */
  def setNumBlocks(value: Int): this.type = {
    setNumUserBlocks(value)
    setNumItemBlocks(value)
    this
  }

  override def fit(dataset: Dataset[_]): MsftRecommendationModel = {
    val als = new ALS()
      .setNonnegative(getNonnegative)
      .setColdStartStrategy(getColdStartStrategy)
      .setRank(getRank)
      .setMaxIter(getMaxIter)
      .setRegParam(getRegParam)
      .setImplicitPrefs(getImplicitPrefs)
      .setNumUserBlocks(getNumUserBlocks)
      .setNumItemBlocks(getNumItemBlocks)
      .setUserCol(getUserCol)
      .setItemCol(getItemCol)
      .setRatingCol(getRatingCol)
      .setSeed(getSeed)
    val model2: ALSModel = als.fit(dataset)

    val model =
      new MsftRecommendationModel(uid, $(rank), model2.userFactors, model2.itemFactors, model2).setParent(this)
    copyValues(model)
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): MsftRecommendation = defaultCopy(extra)
}

object MsftRecommendation extends DefaultParamsReadable[MsftRecommendation]

class MsftRecommendationModel(
                               override val uid: String,
                               val rank: Int,
                               val userFactors: DataFrame,
                               val itemFactors: DataFrame,
                               val alsModel: ALSModel)
  extends Model[MsftRecommendationModel] with MsftRecommendationModelParams
    with ConstructorWritable[MsftRecommendationModel] {

  def recommendForAllUsers(numItems: Int): DataFrame = {
    MsftRecHelper.recommendForAll(rank, userFactors, itemFactors, $(userCol), $(itemCol), numItems)
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
