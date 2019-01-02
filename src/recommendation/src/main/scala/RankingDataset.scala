// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.sql

import org.apache.spark.ml.param._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.functions.{count => sqlCount, explode => sqlExplode, col => sqlCol, _}

import scala.collection.mutable
import scala.util.Random

private[spark] class RankingDataset[T](dataset: Dataset[_],
  override val sparkSession: SparkSession,
  override val queryExecution: QueryExecution,
  encoder: Encoder[T]) extends Dataset[T](sparkSession, queryExecution, encoder) with Params {

  override def copy(extra: ParamMap): RankingDataset[T] = {
    defaultCopy(extra)
  }

  override def randomSplit(weights: Array[Double], seed: Long): Array[Dataset[T]] = {
    val encoder: Encoder[T] = this.exprEnc.asInstanceOf[Encoder[T]]

    def filterByItemCount(dataset: RankingDataset[T]): DataFrame = dataset
      .dropDuplicates()
      .groupBy(getUserCol)
      .agg(sqlCol(getUserCol), sqlCount(sqlCol(getItemCol)).alias("nitems"))
      .where(sqlCol("nitems") >= getMinRatingsPerUser)
      .drop("nitems")
      .cache()

    def filterByUserRatingCount(dataset: Dataset[_]): DataFrame = dataset
      .dropDuplicates()
      .groupBy(getItemCol)
      .agg(sqlCol(getItemCol), sqlCount(sqlCol(getUserCol)).alias("nusers"))
      .where(sqlCol("nusers") >= getMinRatingsPerItem)
      .join(dataset, getItemCol)
      .drop("nusers")
      .cache()

    val trainRatio = weights(0)
    val dataframe = this
    val (trainingDataset, validationDataset) = {

      val filteredByItemCount = filterByItemCount(dataframe)
      val filteredByUserRatingCount = filterByUserRatingCount(dataframe)
      val dataset = filteredByUserRatingCount.join(filteredByItemCount, getUserCol)

      if (dataset.columns.contains(getRatingCol)) {
        val wrapColumn = udf((itemId: Double, rating: Double) => Array(itemId, rating))

        val sliceudf = udf(
          (r: mutable.WrappedArray[Array[Double]]) => r.slice(0, math.round(r.length * trainRatio)
            .toInt))
        val shuffle = udf((r: mutable.WrappedArray[Array[Double]]) => Random.shuffle(r.toSeq))
        val dropudf = udf((r: mutable.WrappedArray[Array[Double]]) => r.drop(math.round(r.length * trainRatio).toInt))

        val testds = dataset
          .withColumn("itemIDRating", wrapColumn(sqlCol(getItemCol), sqlCol(getRatingCol)))
          .groupBy(sqlCol(getUserCol))
          .agg(collect_list(sqlCol("itemIDRating")))
          .withColumn("shuffle", shuffle(sqlCol("collect_list(itemIDRating)")))
          .withColumn("train", sliceudf(sqlCol("shuffle")))
          .withColumn("test", dropudf(sqlCol("shuffle")))
          .drop(sqlCol("collect_list(itemIDRating)")).drop(sqlCol("shuffle"))
          .cache()

        val popLeft = udf((r: mutable.WrappedArray[Double]) => r(0))
        val popRight = udf((r: mutable.WrappedArray[Double]) => r(1))

        val train = testds
          .select(getUserCol, "train")
          .withColumn("itemIdRating", sqlExplode(sqlCol("train")))
          .drop("train")
          .withColumn(getItemCol, popLeft(sqlCol("itemIdRating")))
          .withColumn(getRatingCol, popRight(sqlCol("itemIdRating")))
          .drop("itemIdRating")

        val test = testds
          .select(getUserCol, "test")
          .withColumn("itemIdRating", sqlExplode(sqlCol("test")))
          .drop("test")
          .withColumn(getItemCol, popLeft(sqlCol("itemIdRating")))
          .withColumn(getRatingCol, popRight(sqlCol("itemIdRating")))
          .drop("itemIdRating")

        (train, test)
      } else {
        val shuffle = udf((r: mutable.WrappedArray[Double]) => Random.shuffle(r.toSeq))
        val sliceudf = udf((r: mutable.WrappedArray[Double]) => r.slice(0, math.round(r.length * trainRatio).toInt))
        val dropudf = udf((r: mutable.WrappedArray[Double]) => r.drop(math.round(r.length * trainRatio).toInt))

        val testds = dataset
          .groupBy(sqlCol(getUserCol))
          .agg(collect_list(col(getItemCol)))
          .withColumn("shuffle", shuffle(sqlCol("collect_list(" + getItemCol + ")")))
          .withColumn("train", sliceudf(sqlCol("shuffle")))
          .withColumn("test", dropudf(sqlCol("shuffle")))
          .drop(sqlCol("collect_list(" + getItemCol + ")")).drop(sqlCol("shuffle"))
          .cache()

        val train = testds
          .select(getUserCol, "train")
          .withColumn(getItemCol, sqlExplode(sqlCol("train")))
          .drop("train")

        val test = testds
          .select(getUserCol, "test")
          .withColumn(getItemCol, sqlExplode(sqlCol("test")))
          .drop("test")

        (train, test)
      }
    }

    List(
      new Dataset[T](trainingDataset.sparkSession, trainingDataset.queryExecution, encoder),
      new Dataset[T](validationDataset.sparkSession, validationDataset.queryExecution, encoder)
    ).toArray
  }

  override val uid: String = "RankingDataSet"

  val userCol = new Param[String](this, "userCol", "Column of users")

  /** @group setParam */
  def setUserCol(value: String): this.type = set(userCol, value)

  def getUserCol: String = $(userCol)

  val itemCol = new Param[String](this, "itemCol", "Column of items")

  /** @group setParam */
  def setItemCol(value: String): this.type = set(itemCol, value)

  def getItemCol: String = $(itemCol)

  val ratingCol = new Param[String](this, "ratingCol", "Column of ratings")

  /** @group setParam */
  def setRatingCol(value: String): this.type = set(ratingCol, value)

  def getRatingCol: String = $(ratingCol)

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

  setDefault(minRatingsPerItem -> 1, minRatingsPerUser -> 1)
}

private[spark] object RankingDataset {
  def toRankingDataSet[T](dataset: Dataset[_]): RankingDataset[T] = {
    val encoder: Encoder[T] = dataset.exprEnc.asInstanceOf[Encoder[T]]
    new RankingDataset[T](
      dataset,
      dataset.sparkSession,
      dataset.queryExecution,
      encoder)
  }
}
