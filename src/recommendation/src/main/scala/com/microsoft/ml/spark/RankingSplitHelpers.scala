// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml._
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param._
import org.apache.spark.ml.recommendation._
import org.apache.spark.ml.util._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

trait RecommendationSplitParams extends Params {
  val minRatingsPerUser: IntParam = new IntParam(this, "minRatingsPerUser",
    "min ratings for users > 0", ParamValidators.inRange(0, Integer.MAX_VALUE))

  val minRatingsPerItem: IntParam = new IntParam(this, "minRatingsPerItem",
    "min ratings for items > 0", ParamValidators.inRange(0, Integer.MAX_VALUE))

  /** @group setParam */
  def setMinRatingsPerUser(value: Int): this.type = set(minRatingsPerUser, value)

  /** @group setParam */
  def setMinRatingsPerItem(value: Int): this.type = set(minRatingsPerItem, value)

  /** @group getParam */
  def getMinRatingsPerUser: Int = $(minRatingsPerUser)

  /** @group getParam */
  def getMinRatingsPerItem: Int = $(minRatingsPerItem)

}

trait RecommendationSplitFunctions extends RecommendationSplitParams with HasRecommenderCols {

  private def filterByItemCount(dataset: Dataset[_]): DataFrame =
    dataset
      .groupBy($(userCol))
      .agg(col($(userCol)), count(col($(itemCol))).alias("nitems"))
      .where(col("nitems") >= $(minRatingsPerUser))
      .drop("nitems")
      .cache()

  private def filterByUserRatingCount(dataset: Dataset[_]): DataFrame =
    dataset
      .groupBy($(itemCol))
      .agg(col($(itemCol)), count(col($(userCol))).alias("nusers"))
      .where(col("nusers") >= $(minRatingsPerItem))
      .join(dataset, $(itemCol))
      .drop("nusers")
      .cache()

  def filterRatings(dataset: Dataset[_]): DataFrame = filterByUserRatingCount(dataset)
    .join(filterByItemCount(dataset), $(userCol))

  def prepareTestData(userColumn: String, itemColumn: String,
                      validationDataset: DataFrame, recs: DataFrame,
                      k: Int): Dataset[_] = {
    import org.apache.spark.sql.functions.{collect_list, rank => r}

    val perUserRecommendedItemsDF: DataFrame = recs
      .select(userColumn, "recommendations." + itemColumn)
      .withColumnRenamed(itemColumn, "prediction")

    val perUserActualItemsDF = if (validationDataset.columns.contains($(ratingCol))) {
      val windowSpec = Window.partitionBy(userColumn).orderBy(col($(ratingCol)).desc)

      validationDataset
        .select(userColumn, itemColumn, $(ratingCol))
        .withColumn("rank", r().over(windowSpec).alias("rank"))
        .where(col("rank") <= k)
        .groupBy(userColumn)
        .agg(col(userColumn), collect_list(col(itemColumn)))
        .withColumnRenamed("collect_list(" + itemColumn + ")", "label")
        .select(userColumn, "label")
    } else {
      val windowSpec = Window.partitionBy(userColumn).orderBy(col($(itemCol)).desc)

      validationDataset
        .select(userColumn, itemColumn)
        .withColumn("rank", r().over(windowSpec).alias("rank"))
        .where(col("rank") <= k)
        .groupBy(userColumn)
        .agg(col(userColumn), collect_list(col(itemColumn)))
        .withColumnRenamed("collect_list(" + itemColumn + ")", "label")
        .select(userColumn, "label")
    }
    val joined_rec_actual = perUserRecommendedItemsDF
      .join(perUserActualItemsDF, userColumn)
      .drop(userColumn)

    joined_rec_actual
  }

  def splitDF(dataset: DataFrame, trainRatio: Double): (DataFrame, DataFrame) = {
    val shuffleFlag = true
    val shuffleBC = dataset.sparkSession.sparkContext.broadcast(shuffleFlag)

    if (dataset.columns.contains($(ratingCol))) {
      val wrapColumn = udf((itemId: Double, rating: Double) => Array(itemId, rating))

      val sliceudf = udf(
        (r: mutable.WrappedArray[Array[Double]]) => r.slice(0, math.round(r.length * trainRatio).toInt))

      val shuffle = udf((r: mutable.WrappedArray[Array[Double]]) =>
        if (shuffleBC.value) Random.shuffle(r.toSeq)
        else r
      )
      val dropudf = udf((r: mutable.WrappedArray[Array[Double]]) => r.drop(math.round(r.length * trainRatio).toInt))

      val testds = dataset
        .withColumn("itemIDRating", wrapColumn(col($(itemCol)), col($(ratingCol))))
        .groupBy(col($(userCol)))
        .agg(collect_list(col("itemIDRating")))
        .withColumn("shuffle", shuffle(col("collect_list(itemIDRating)")))
        .withColumn("train", sliceudf(col("shuffle")))
        .withColumn("test", dropudf(col("shuffle")))
        .drop(col("collect_list(itemIDRating)")).drop(col("shuffle"))
        .cache()

      val popLeft = udf((r: mutable.WrappedArray[Double]) => r(0))
      val popRight = udf((r: mutable.WrappedArray[Double]) => r(1))

      val train = testds
        .select($(userCol), "train")
        .withColumn("itemIdRating", explode(col("train")))
        .drop("train")
        .withColumn($(itemCol), popLeft(col("itemIdRating")))
        .withColumn($(ratingCol), popRight(col("itemIdRating")))
        .drop("itemIdRating")

      val test = testds
        .select($(userCol), "test")
        .withColumn("itemIdRating", explode(col("test")))
        .drop("test")
        .withColumn($(itemCol), popLeft(col("itemIdRating")))
        .withColumn($(ratingCol), popRight(col("itemIdRating")))
        .drop("itemIdRating")

     (train, test)
    } else {
      val shuffle = udf((r: mutable.WrappedArray[Double]) =>
        if (shuffleBC.value) Random.shuffle(r.toSeq)
        else r
      )
      val sliceudf = udf(
        (r: mutable.WrappedArray[Double]) => r.slice(0, math.round(r.length * trainRatio).toInt))
      val dropudf = udf((r: mutable.WrappedArray[Double]) => r.drop(math.round(r.length * trainRatio).toInt))

      val testds = dataset
        .groupBy(col($(userCol)))
        .agg(collect_list(col($(itemCol))))
        .withColumn("shuffle", shuffle(col("collect_list(" + $(itemCol) + ")")))
        .withColumn("train", sliceudf(col("shuffle")))
        .withColumn("test", dropudf(col("shuffle")))
        .drop(col("collect_list(" + $(itemCol) + ")")).drop(col("shuffle"))
        .cache()

      val train = testds
        .select($(userCol), "train")
        .withColumn($(itemCol), explode(col("train")))
        .drop("train")

      val test = testds
        .select($(userCol), "test")
        .withColumn($(itemCol), explode(col("test")))
        .drop("test")

      (train, test)
    }
  }

  def prepareTestData(validationDataset: DataFrame, recs: DataFrame,
                      k: Int): Dataset[_] = {
    import org.apache.spark.sql.functions.{collect_list, rank => r}

    val perUserRecommendedItemsDF: DataFrame = recs
      .select(getUserCol, "recommendations." + getItemCol)
      .withColumnRenamed(getItemCol, "prediction")

    val perUserActualItemsDF = if (validationDataset.columns.contains(getRatingCol)) {
      val windowSpec = Window.partitionBy(getUserCol).orderBy(col(getRatingCol).desc)

      validationDataset
        .select(getUserCol, getItemCol, getRatingCol)
        .withColumn("rank", r().over(windowSpec).alias("rank"))
        .where(col("rank") <= k)
        .groupBy(getUserCol)
        .agg(col(getUserCol), collect_list(col(getItemCol)))
        .withColumnRenamed("collect_list(" + getItemCol + ")", "label")
        .select(getUserCol, "label")
    } else {
      val windowSpec = Window.partitionBy(getUserCol).orderBy(col(getItemCol).desc)

      validationDataset
        .select(getUserCol, getItemCol)
        .withColumn("rank", r().over(windowSpec).alias("rank"))
        .where(col("rank") <= k)
        .groupBy(getUserCol)
        .agg(col(getUserCol), collect_list(col(getItemCol)))
        .withColumnRenamed("collect_list(" + getItemCol + ")", "label")
        .select(getUserCol, "label")
    }
    val joined_rec_actual = perUserRecommendedItemsDF
      .join(perUserActualItemsDF, getUserCol)
      .drop(getUserCol)

    joined_rec_actual
  }

}

