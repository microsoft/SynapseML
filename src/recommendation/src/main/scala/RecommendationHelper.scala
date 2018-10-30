// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.recommendation

import com.microsoft.ml.spark.Wrappable
import org.apache.spark.ml.param.{IntParam, Param, ParamValidators, Params}
import org.apache.spark.ml.param.shared.{HasLabelCol, HasPredictionCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

trait RecEvaluatorParams extends Wrappable
  with HasPredictionCol with HasLabelCol with kTrait with ComplexParamsWritable

object SparkHelper {
  def flatten(ratings: Dataset[_], num: Int, dstOutputColumn: String, srcOutputColumn: String): DataFrame = {
    import ratings.sparkSession.implicits._

    val topKAggregator = new TopByKeyAggregator[Int, Int, Float](num, Ordering.by(_._2))
    val recs = ratings.as[(Int, Int, Float)].groupByKey(_._1).agg(topKAggregator.toColumn)
      .toDF("id", "recommendations")

    val arrayType = ArrayType(
      new StructType()
        .add(dstOutputColumn, IntegerType)
        .add("rating", FloatType)
    )
    recs.select(col("id").as(srcOutputColumn), col("recommendations").cast(arrayType))
  }
}

trait PublicALSParams extends ALSParams

trait HasRecommenderCols extends Params {
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

}

trait kTrait extends Params{
  val k: IntParam = new IntParam(this, "k", "number of items", ParamValidators.inRange(1, Integer.MAX_VALUE))

  /** @group getParam */
  def getK: Int = $(k)

  /** @group setParam */
  def setK(value: Int): this.type = set(k, value)
}
