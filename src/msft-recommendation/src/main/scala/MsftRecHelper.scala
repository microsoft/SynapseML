// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.recommendation

import java.{util => ju}

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import com.microsoft.ml.spark.{ConstructorReadable, ConstructorWritable}
import org.apache.spark.SparkContext
import org.apache.spark.ml.Model
import org.apache.spark.ml.linalg.BLAS
import org.apache.spark.ml.param.shared.HasPredictionCol
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.DefaultParamsReader.Metadata
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.util.BoundedPriorityQueue

import scala.reflect.runtime.universe.{TypeTag, typeTag}

trait MsftRecommendationModelParams extends Params with ALSModelParams with HasPredictionCol

trait MsftRecommendationParams extends MsftRecommendationModelParams with ALSParams

trait MsftHasPredictionCol extends Params with HasPredictionCol

class MsftRecHelper {

  private def blockify(
                        factors: Dataset[(Int, Array[Float])],
                        blockSize: Int = 4096): Dataset[Seq[(Int, Array[Float])]] = {
    import factors.sparkSession.implicits._
    factors.mapPartitions(_.grouped(blockSize))
  }

  def loadMetadata(path: String, sc: SparkContext, className: String = ""): Metadata =
    DefaultParamsReader.loadMetadata(path, sc, className)

  def getAndSetParams(model: Params, metadata: Metadata): Unit =
    DefaultParamsReader.getAndSetParams(model, metadata)

}

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
        val pq = new BoundedPriorityQueue[(Int, Float)](num)(Ordering.by(_._2))
        srcIter.foreach { case (srcId, srcFactor) =>
          dstIter.foreach { case (dstId, dstFactor) =>
            // We use F2jBLAS which is faster than a call to native BLAS for vector dot product
            val score = BLAS.f2jBLAS.sdot(rank, srcFactor, 1, dstFactor, 1)
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
    val topKAggregator = new TopByKeyAggregator[Int, Int, Float](num, Ordering.by(_._2))
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
  override def objectsToSave: List[AnyRef] = List(uid, rank.toString,  userFactors, itemFactors)
}

object MsftRecommendationModel extends ConstructorReadable[MsftRecommendationModel] {
  private val NaN = "nan"
  private val Drop = "drop"
}


