// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.recommendation

import com.microsoft.ml.spark.Wrappable
import org.apache.spark
import org.apache.spark.ml.param.shared.{HasLabelCol, HasPredictionCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, StructType}

trait RecommendationParams extends Wrappable with ALSParams

trait RecEvaluatorParams extends Wrappable
  with HasPredictionCol with HasLabelCol with ComplexParamsWritable

object helper {
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
