// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.{util => ju}

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.param._
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.ml.recommendation._
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.storage.StorageLevel

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
    transformSchema(dataset.schema)
    import dataset.sparkSession.implicits._

    val r = if ($(ratingCol) != "") col($(ratingCol)).cast(FloatType) else lit(1.0f)
    val ratings = dataset
      .select(checkedCast(col($(userCol))), checkedCast(col($(itemCol))), r)
      .rdd
      .map { row =>
        Rating(row.getInt(0), row.getInt(1), row.getFloat(2))
      }

    val (userFactors, itemFactors) = ALS.train(ratings, rank = $(rank),
      numUserBlocks = $(numUserBlocks), numItemBlocks = $(numItemBlocks),
      maxIter = $(maxIter), regParam = $(regParam), implicitPrefs = $(implicitPrefs),
      alpha = $(alpha), nonnegative = $(nonnegative),
      intermediateRDDStorageLevel = StorageLevel.fromString($(intermediateStorageLevel)),
      finalRDDStorageLevel = StorageLevel.fromString($(finalStorageLevel)),
      checkpointInterval = $(checkpointInterval), seed = $(seed))
    val userDF = userFactors.toDF("id", "features")
    val itemDF = itemFactors.toDF("id", "features")
    val model = new MsftRecommendationModel(uid, $(rank), userDF, itemDF).setParent(this)
    copyValues(model)
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): MsftRecommendation = defaultCopy(extra)
}

object MsftRecommendation extends DefaultParamsReadable[MsftRecommendation]

class MsftRecommendationHelper() {
  def split(dfRaw: DataFrame): (DataFrame, DataFrame) = {
    val ratingsTemp = dfRaw.dropDuplicates()

    val customerIndexer = new StringIndexer()
      .setInputCol("customerID")
      .setOutputCol("customerIDindex")

    val ratingsIndexed1 = customerIndexer.fit(ratingsTemp).transform(ratingsTemp)

    val itemIndexer = new StringIndexer()
      .setInputCol("itemID")
      .setOutputCol("itemIDindex")

    val ratings = itemIndexer.fit(ratingsIndexed1).transform(ratingsIndexed1)
      .drop("customerID").withColumnRenamed("customerIDindex", "customerID")
      .drop("itemID").withColumnRenamed("itemIDindex", "itemID")

    ratings.cache()

    val minRatingsU = 1
    val minRatingsI = 1
    val RATIO = 0.75

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
      .map(r => (r.getDouble(0).toInt, r.getDouble(1).toInt, r.getInt(2)))
      .toDF("customerID", "itemID", "rating")

    train.cache()

    val testIndex = perm_indices.map(r => (r._1, r._2.drop(math.round(r._3.size.toDouble * RATIO).toInt)))

    val test = inputDF.rdd
      .groupBy(r => r(1))
      .join(testIndex)
      .flatMap(r => r._2._1.drop(r._2._2.size))
      .map(r => (r.getDouble(0).toInt, r.getDouble(1).toInt, r.getInt(2))).toDF("customerID", "itemID", "rating")

    test.cache()

    (train, test)
  }

}

