// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.recommendation

import com.microsoft.ml.spark.Wrappable
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasLabelCol, HasPredictionCol, HasSeed}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.util.ThreadUtils

trait RecEvaluatorParams extends Wrappable
  with HasPredictionCol with HasLabelCol with hasK with ComplexParamsWritable

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

trait RecommendationParams extends ALSParams

trait BaseRecommendationModel extends Params with ALSModelParams with HasPredictionCol {

  private val id = Constants.idCol

  def getALSModel(uid: String,
    rank: Int,
    userFactors: DataFrame,
    itemFactors: DataFrame): ALSModel = {
    new ALSModel(uid, rank, userFactors, itemFactors)
  }

  def recommendForAllUsers(k: Int): DataFrame

  def transform(rank: Int, userDataFrame: DataFrame, itemDataFrame: DataFrame, dataset: Dataset[_]): DataFrame = {
    getALSModel(uid, rank,
      userDataFrame.withColumnRenamed(getUserCol, id).withColumnRenamed("flatList", "features"),
      itemDataFrame.withColumnRenamed(getItemCol, id).withColumnRenamed(Constants.itemAffinities, "features"))
      .setUserCol(getUserCol)
      .setItemCol(getItemCol)
      .setColdStartStrategy("drop")
      .transform(dataset)
  }
}

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

trait hasK extends Params {
  val k: IntParam = new IntParam(this, "k", "number of items", ParamValidators.inRange(1, Integer.MAX_VALUE))

  /** @group getParam */
  def getK: Int = $(k)

  /** @group setParam */
  def setK(value: Int): this.type = set(k, value)

  setDefault(k -> 10)
}

trait RankingTrainValidationSplitParams extends Wrappable with HasSeed {

  /**
    * Param for ratio between train and validation data. Must be between 0 and 1.
    * Default: 0.75
    *
    * @group param
    */
  val trainRatio: DoubleParam = new DoubleParam(this, "trainRatio",
    "ratio between training set and validation set (>= 0 && <= 1)", ParamValidators.inRange(0, 1))

  /** @group getParam */
  def getTrainRatio: Double = $(trainRatio)

  val minRatingsU: IntParam = new IntParam(this, "minRatingsU",
    "min ratings for users > 0", ParamValidators.inRange(0, Integer.MAX_VALUE))

  /** @group getParam */
  def getMinRatingsU: Int = $(minRatingsU)

  val minRatingsI: IntParam = new IntParam(this, "minRatingsI",
    "min ratings for items > 0", ParamValidators.inRange(0, Integer.MAX_VALUE))

  /** @group getParam */
  def getMinRatingsI: Int = $(minRatingsI)

  val estimatorParamMaps: ArrayParamMapParam =
    new ArrayParamMapParam(this, "estimatorParamMaps", "param maps for the estimator")

  /** @group getParam */
  def getEstimatorParamMaps: Array[ParamMap] = $(estimatorParamMaps)

  val evaluator: EvaluatorParam = new EvaluatorParam(this, "evaluator",
    "evaluator used to select hyper-parameters that maximize the validated metric")

  /** @group getParam */
  def getEvaluator: Evaluator = $(evaluator)

  val estimator = new EstimatorParam(this, "estimator", "estimator for selection")

  /** @group getParam */
  def getEstimator: Estimator[_ <: Model[_]] = $(estimator)

  setDefault(trainRatio -> 0.75, minRatingsU -> 1, minRatingsI -> 1)

  protected def transformSchemaImpl(schema: StructType): StructType = {
    require($(estimatorParamMaps).nonEmpty, s"Validator requires non-empty estimatorParamMaps")
    val firstEstimatorParamMap = $(estimatorParamMaps).head
    val est = $(estimator)
    for (paramMap <- $(estimatorParamMaps).tail) {
      est.copy(paramMap).transformSchema(schema)
    }
    est.copy(firstEstimatorParamMap).transformSchema(schema)
  }

  /**
    * Instrumentation logging for tuning params including the inner estimator and evaluator info.
    */
  protected def logTuningParams(instrumentation: Instrumentation): Unit = {
    instrumentation.logNamedValue("estimator", $(estimator).getClass.getCanonicalName)
    instrumentation.logNamedValue("evaluator", $(evaluator).getClass.getCanonicalName)
    instrumentation.logNamedValue("estimatorParamMapsLength", Int.int2long($(estimatorParamMaps).length))
  }
}

object SparkHelpers {
  def getThreadUtils(): ThreadUtils.type = {
    ThreadUtils
  }
}

object Constants {
  val idCol = "id"
  val userCol = "user"
  val itemCol = "item"
  val ratingCol = "rating"
  val recommendations = "recommendations"
  val featuresCol = "features"
  val tagId = "tagId"
  val relevance = "relevance"
  val affinityCol = "affinity"
  val prediction = "prediction"
  val itemAffinities = "itemAffinities"
  val label = "label"
}
