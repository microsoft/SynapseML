// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.recommendation

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.param.{ArrayParamMapParam, EstimatorParam, EvaluatorParam}
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasLabelCol, HasPredictionCol, HasSeed}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{ComplexParamsWritable, Estimator, Model}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.util.ThreadUtils

trait RecEvaluatorParams extends Wrappable
  with HasPredictionCol with HasLabelCol with hasK with ComplexParamsWritable

trait RecommendationParams extends ALSParams {
  /** @group setParam */
  def setNumUserBlocks(value: Int): this.type = set(numUserBlocks, value)

  /** @group setParam */
  def setNumItemBlocks(value: Int): this.type = set(numItemBlocks, value)

  /** @group setParam */
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  def setBlockSize(value: Int): this.type = set(blockSize, value)

  /** @group setParam */
  def setRegParam(value: Double): this.type = set(regParam, value)

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  def setRank(value: Int): this.type = set(rank, value)

  /** @group setParam */
  def setAlpha(value: Double): this.type = set(alpha, value)

  /** @group setParam */
  def setColdStartStrategy(value: String): this.type = set(coldStartStrategy, value)

  /** @group setParam */
  def setNonnegative(value: Boolean): this.type = set(nonnegative, value)

  /** @group setParam */
  def setIntermediateStorageLevel(value: String): this.type = set(intermediateStorageLevel, value)

  /** @group setParam */
  def setImplicitPrefs(value: Boolean): this.type = set(implicitPrefs, value)

  /** @group setParam */
  def setFinalStorageLevel(value: String): this.type = set(finalStorageLevel, value)

  /** @group setParam */
  def setCheckpointInterval(value: Int): this.type = set(checkpointInterval, value)

  /** @group setParam */
  def setSeed(value: Long): this.type = set(seed, value)
}

trait BaseRecommendationModel extends Params with ALSModelParams with HasPredictionCol {

  private val id = Constants.IdCol

  def getALSModel(uid: String,
    rank: Int,
    userFactors: DataFrame,
    itemFactors: DataFrame): ALSModel = {
    new ALSModel(uid, rank, userFactors, itemFactors)
  }

  def recommendForAllUsers(k: Int): DataFrame

  def recommendForAllItems(k: Int): DataFrame

  def transform(rank: Int, userDataFrame: DataFrame, itemDataFrame: DataFrame, dataset: Dataset[_]): DataFrame = {
    getALSModel(uid, rank,
      userDataFrame.withColumnRenamed(getUserCol, id).withColumnRenamed("flatList", "features"),
      itemDataFrame.withColumnRenamed(getItemCol, id).withColumnRenamed(Constants.ItemAffinities, "features"))
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
    "ratio between training set and validation set (>= 0 and <= 1)", ParamValidators.inRange(0, 1))

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
    require(getEstimatorParamMaps.nonEmpty, s"Validator requires non-empty estimatorParamMaps")
    val firstEstimatorParamMap = getEstimatorParamMaps.head
    val est = getEstimator
    for (paramMap <- getEstimatorParamMaps.tail) {
      est.copy(paramMap).transformSchema(schema)
    }
    est.copy(firstEstimatorParamMap).transformSchema(schema)
  }

  /**
    * Instrumentation logging for tuning params including the inner estimator and evaluator info.
    */
  protected def logTuningParams(instrumentation: Instrumentation): Unit = {
    instrumentation.logNamedValue("estimator", getEstimator.getClass.getCanonicalName)
    instrumentation.logNamedValue("evaluator", getEvaluator.getClass.getCanonicalName)
    instrumentation.logNamedValue("estimatorParamMapsLength", Int.int2long(getEstimatorParamMaps.length))
  }
}

object SparkHelpers {
  def getThreadUtils: ThreadUtils.type = {
    ThreadUtils
  }

  def flatten(ratings: Dataset[_], num: Int, dstOutputColumn: String, srcOutputColumn: String): DataFrame = {
    import ratings.sparkSession.implicits._
    import org.apache.spark.sql.functions.{collect_top_k, struct}

    val arrayType = ArrayType(
      new StructType()
        .add(dstOutputColumn, IntegerType)
        .add(Constants.RatingCol, FloatType)
    )

    ratings.toDF(srcOutputColumn, dstOutputColumn, Constants.RatingCol).groupBy(srcOutputColumn)
     .agg(collect_top_k(struct(Constants.RatingCol, dstOutputColumn), num, false))
     .as[(Int, Seq[(Float, Int)])]
     .map(t => (t._1, t._2.map(p => (p._2, p._1))))
     .toDF(srcOutputColumn, Constants.Recommendations)
     .withColumn(Constants.Recommendations, col(Constants.Recommendations).cast(arrayType))
  }
}

object Constants {
  val IdCol = "id"
  val UserCol = "user"
  val ItemCol = "item"
  val RatingCol = "rating"
  val Recommendations = "recommendations"
  val FeaturesCol = "features"
  val TagId = "tagId"
  val Relevance = "relevance"
  val AffinityCol = "affinity"
  val Prediction = "prediction"
  val ItemAffinities = "itemAffinities"
  val Label = "label"
}
