// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.{util => ju}

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.recommendation.{MsftRecHelper, MsftRecommendationModelParams, _}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.reflect.runtime.universe.{TypeTag, typeTag}
import scala.util.Random

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

object MsftRecommendation extends DefaultParamsReadable[MsftRecommendation] {
  /**
    * Returns top `numItems` items recommended for each user, for all users.
    *
    * @param dfRaw       input DataFrame
    * @param minRatingsU input DataFrame
    * @param minRatingsI input DataFrame
    * @param RATIO       input DataFrame
    * @return a DataFrame of (userCol: Int, recommendations), where recommendations are
    *         stored as an array of (itemCol: Int, rating: Float) Rows.
    */
  def split(dfRaw: DataFrame,
            minRatingsU: Int = 1,
            minRatingsI: Int = 1,
            RATIO: Double = 0.75): DataFrame = {
    val ratingsTemp = dfRaw.dropDuplicates()

    val ratingsIndexed1 = new StringIndexer()
      .setInputCol("customerID")
      .setOutputCol("customerIDindex")
      .fit(ratingsTemp).transform(ratingsTemp)

    val ratings = new StringIndexer()
      .setInputCol("itemID")
      .setOutputCol("itemIDindex")
      .fit(ratingsIndexed1).transform(ratingsIndexed1)
      .drop("customerID").withColumnRenamed("customerIDindex", "customerID")
      .drop("itemID").withColumnRenamed("itemIDindex", "itemID")

    ratings.cache()

    import dfRaw.sqlContext.implicits._
    import org.apache.spark.sql.functions._

    val tmpDF = ratings
      .groupBy("customerID")
      .agg('customerID, count('itemID))
      .withColumnRenamed("count(itemID)", "nitems")
      .where(col("nitems") >= minRatingsU)

    val inputDF = ratings.groupBy("itemID")
      .agg('itemID, count('customerID))
      .withColumnRenamed("count(customerID)", "ncustomers")
      .where(col("ncustomers") >= minRatingsI)
      .join(ratings, "itemID")
      .drop("ncustomers")
      .join(tmpDF, "customerID")
      .drop("nitems")

    inputDF.cache()

    val nusers_by_item = inputDF.groupBy("itemID")
      .agg('itemID, count("customerID"))
      .withColumnRenamed("count(customerID)", "nusers")
      .rdd

    val perm_indices = nusers_by_item.map(r => (r(0), Random.shuffle(List(r(1))), List(r(1))))
    perm_indices.cache()

    val tr_idx = perm_indices.map(r => (r._1, r._2.slice(0, math.round(r._3.size.toDouble * RATIO).toInt)))

    val train = inputDF.rdd
      .groupBy(r => r(1))
      .join(tr_idx)
      .flatMap(r => r._2._1.slice(0, r._2._2.size))
      .map(r => (r.getDouble(0), r.getDouble(1), r.getInt(2)))
      .toDF("customerID", "itemID", "rating")

    val testIndex = perm_indices.map(r => (r._1, r._2.drop(math.round(r._3.size.toDouble * RATIO).toInt)))

    val test = inputDF.rdd
      .groupBy(r => r(1))
      .join(testIndex)
      .flatMap(r => r._2._1.drop(r._2._2.size))
      .map(r => (r.getDouble(0).toInt, r.getDouble(1).toInt, r.getInt(2))).toDF("customerID", "itemID", "rating")

    train.withColumn("train", typedLit(1))
      .union(test.withColumn("train", typedLit(0)))
  }
}

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

  /**
    * Returns top `numItems` items recommended for each user, for all users.
    *
    * @param numItems max number of recommendations for each user
    * @return a DataFrame of (userCol: Int, recommendations), where recommendations are
    *         stored as an array of (itemCol: Int, rating: Float) Rows.
    */
  def recommendForAllUsers(numItems: Int): DataFrame = {
    MsftRecHelper.recommendForAllUsers(alsModel, numItems)
  }

  /**
    * Returns top `numUsers` users recommended for each item, for all items.
    *
    * @param numUsers max number of recommendations for each item
    * @return a DataFrame of (itemCol: Int, recommendations), where recommendations are
    *         stored as an array of (userCol: Int, rating: Float) Rows.
    */
  def recommendForAllItems(numUsers: Int): DataFrame = {
    MsftRecHelper.recommendForAllItems(alsModel, numUsers)
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
