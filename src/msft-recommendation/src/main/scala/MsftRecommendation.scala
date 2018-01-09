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
    transformSchema(dataset.schema)
    import dataset.sparkSession.implicits._

    val r = if ($(ratingCol) != "") col($(ratingCol)).cast(FloatType) else lit(1.0f)

    def getRow(row: Row): Rating[Int] = Rating(row.getInt(0), row.getInt(1), row.getFloat(2))

    val ratings = dataset
      .select(checkedCast(col($(userCol))), checkedCast(col($(itemCol))), r)
      .rdd
      .map(getRow(_))

    val (userFactors, itemFactors): (RDD[(Int, Array[Float])], RDD[(Int, Array[Float])]) =
      ALS.train(ratings, rank = $(rank),
        numUserBlocks = $(numUserBlocks), numItemBlocks = $(numItemBlocks),
        maxIter = $(maxIter), regParam = $(regParam), implicitPrefs = $(implicitPrefs),
        alpha = $(alpha), nonnegative = $(nonnegative),
        intermediateRDDStorageLevel = StorageLevel.fromString($(intermediateStorageLevel)),
        finalRDDStorageLevel = StorageLevel.fromString($(finalStorageLevel)),
        checkpointInterval = $(checkpointInterval), seed = $(seed))

    val userDF: DataFrame = userFactors.toDF("id", "features")
    val itemDF: DataFrame = itemFactors.toDF("id", "features")
    val model = new MsftRecommendationModel(uid, $(rank), userDF, itemDF).setParent(this)
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
                               val itemFactors: DataFrame)
  extends Model[MsftRecommendationModel] with MsftRecommendationModelParams
    with ConstructorWritable[MsftRecommendationModel] {

  def recommendForAllUsers(numItems: Int): DataFrame = {
    recommendForAll(userFactors, itemFactors, $(userCol), $(itemCol), numItems)
  }

  def recommendForAll(
                       srcFactors: DataFrame,
                       dstFactors: DataFrame,
                       srcOutputColumn: String,
                       dstOutputColumn: String,
                       num: Int): DataFrame = {
    import srcFactors.sparkSession.implicits._

    val srcFactorsBlocked = blockify(srcFactors.as[(Int, Array[Float])])
    val dstFactorsBlocked = blockify(dstFactors.as[(Int, Array[Float])])
    val ratings = srcFactorsBlocked.crossJoin(dstFactorsBlocked)
      .as[(Seq[(Int, Array[Float])], Seq[(Int, Array[Float])])]
      .flatMap { case (srcIter, dstIter) =>
        val m = srcIter.size
        val n = math.min(dstIter.size, num)
        val output = new Array[(Int, Int, Float)](m * n)
        var i = 0
        val pq = MsftRecHelper.getBoundedPriorityQueue(num)(Ordering.by(_._2))
        srcIter.foreach { case (srcId, srcFactor) =>
          dstIter.foreach { case (dstId, dstFactor) =>
            // We use F2jBLAS which is faster than a call to native BLAS for vector dot product
            val score = MsftRecHelper.f2jBLAS.sdot(rank, srcFactor, 1, dstFactor, 1)
            pq += dstId -> score
          }
          pq.foreach { case (dstId, score) =>
            output(i) = (srcId, dstId, score)
            i += 1
          }
          pq.clear()
        }
        output.toSeq
      }
    // We'll force the IDs to be Int. Unfortunately this converts IDs to Int in the output.
    val topKAggregator = MsftRecHelper.getTopByKeyAggregator(num, Ordering.by(_._2))
    val recs = ratings.as[(Int, Int, Float)].groupByKey(_._1).agg(topKAggregator.toColumn)
      .toDF("id", "recommendations")

    val arrayType = ArrayType(
      new StructType()
        .add(dstOutputColumn, IntegerType)
        .add("rating", FloatType)
    )
    recs.select($"id".as(srcOutputColumn), $"recommendations".cast(arrayType))
  }

  private def blockify(
                        factors: Dataset[(Int, Array[Float])],
                        blockSize: Int = 4096): Dataset[Seq[(Int, Array[Float])]] = {
    import factors.sparkSession.implicits._
    factors.mapPartitions(_.grouped(blockSize))
  }

  override def copy(extra: ParamMap): MsftRecommendationModel = {
    val copied = new MsftRecommendationModel(uid, rank, userFactors, itemFactors)
    copyValues(copied, extra).setParent(parent)
  }

  private val predict = udf { (featuresA: Seq[Float], featuresB: Seq[Float]) =>
    if (featuresA != null && featuresB != null) {
      // TODO(SPARK-19759): try dot-producting on Seqs or another non-converted type for
      // potential optimization.
      blas.sdot(rank, featuresA.toArray, 1, featuresB.toArray, 1)
    } else {
      Float.NaN
    }
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema)
    // create a new column named map(predictionCol) by running the predict UDF.
    val predictions = dataset
      .join(userFactors,
        checkedCast(dataset($(userCol))) === userFactors("id"), "left")
      .join(itemFactors,
        checkedCast(dataset($(itemCol))) === itemFactors("id"), "left")
      .select(dataset("*"),
        predict(userFactors("features"), itemFactors("features")).as($(predictionCol)))
    getColdStartStrategy match {
      case MsftRecommendationModel.Drop =>
        predictions.na.drop("all", Seq($(predictionCol)))
      case MsftRecommendationModel.NaN =>
        predictions
    }
  }

  override def transformSchema(schema: StructType): StructType =
    new ALS().transformSchema(schema)

  override val ttag: TypeTag[MsftRecommendationModel] = typeTag[MsftRecommendationModel]

  override def objectsToSave: List[AnyRef] = List(uid, rank.toString, userFactors, itemFactors)
}

object MsftRecommendationModel extends ConstructorReadable[MsftRecommendationModel] {
  private val NaN = "nan"
  private val Drop = "drop"
}
