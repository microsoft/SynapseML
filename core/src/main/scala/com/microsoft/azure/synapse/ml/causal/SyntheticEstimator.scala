// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row}
import org.apache.spark.sql.types._
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import com.microsoft.azure.synapse.ml.causal.linalg._
import com.microsoft.azure.synapse.ml.causal.opt.ConstrainedLeastSquare
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging

trait SyntheticEstimator extends SynapseMLLogging {
  this: BaseDiffInDiffEstimator with SyntheticEstimatorParams =>

  import SyntheticEstimator._

  implicit val vectorOps: DVectorOps.type = DVectorOps
  implicit val matrixOps: DMatrixOps.type = DMatrixOps
  implicit val matrixEntryEncoder: Encoder[MatrixEntry] = Encoders.product[MatrixEntry]
  implicit val vectorEntryEncoder: Encoder[VectorEntry] = Encoders.product[VectorEntry]

  private[causal] lazy val postTreatment = col(getPostTreatmentCol)
  private[causal] lazy val treatment = col(getTreatmentCol)
  private[causal] lazy val outcome = col(getOutcomeCol)
  private[causal] val findWeightsCol = DatasetExtensions.findUnusedColumnName("weights") _

  private def solveCLS(A: DMatrix, b: DVector, lambda: Double, fitIntercept: Boolean, size: (Long, Long))
    : (DVector, Double, Double, Seq[Double]) = {
    if (size._1 * size._2 <= getLocalSolverThreshold) {
      // If matrix size is less than LocalSolverThreshold (defaults to 1M),
      // collect the data on the driver node and solve it locally, where matrix-vector
      // multiplication is done with breeze. It's much faster than solving with Spark at scale.
      implicit val bzMatrixOps: MatrixOps[BDM[Double], BDV[Double]] = BzMatrixOps
      implicit val bzVectorOps: VectorOps[BDV[Double]] = BzVectorOps
      implicit val cacheOps: CacheOps[BDV[Double]] = BDVCacheOps

      val bzA = convertToBDM(A.collect(), size)
      val bzb = convertToBDV(b.collect(), size._1)
      val solver = new ConstrainedLeastSquare[BDM[Double], BDV[Double]](
        step = this.getStepSize, maxIter = this.getMaxIter,
        numIterNoChange = get(numIterNoChange), tol = this.getTol
      )

      val (x, intercept, rmse, lossHistory) = solver.solve(bzA, bzb, lambda, fitIntercept)
      val xdf = A.sparkSession.createDataset[VectorEntry](x.mapPairs((i, v) => VectorEntry(i, v)).toArray.toSeq)
      (xdf, intercept, rmse, lossHistory)
    } else {
      implicit val cacheOps: CacheOps[DVector] = DVectorCacheOps
      val solver = new ConstrainedLeastSquare[DMatrix, DVector](
        step = this.getStepSize, maxIter = this.getMaxIter,
        numIterNoChange = get(numIterNoChange), tol = this.getTol
      )

      solver.solve(A, b, lambda, fitIntercept)
    }
  }

  private[causal] def fitTimeWeights(indexedControlDf: DataFrame, size: (Long, Long))
                                    (unitIdxCol: String, timeIdxCol: String)
  : (DVector, Double, Double, Seq[Double]) =
    logVerb("fitTimeWeights", {
      val indexedPreControl = indexedControlDf.filter(not(postTreatment)).cache

      val outcomePre = indexedPreControl
        .toDMatrix(unitIdxCol, timeIdxCol, getOutcomeCol)

      val outcomePostMean = indexedControlDf.filter(postTreatment)
        .groupBy(col(unitIdxCol).as("i"))
        .agg(avg(col(getOutcomeCol)).as("value"))
        .as[VectorEntry]

      solveCLS(outcomePre, outcomePostMean, lambda = 0d, fitIntercept = true, size)
    })

  private[causal] def calculateRegularization(data: DataFrame): Double = logVerb("calculateRegularization", {
    val diffCol = DatasetExtensions.findUnusedColumnName("diff", data)
    val Row(firstDiffStd: Double) = data
      .filter(not(treatment) and not(postTreatment))
      .select(
        (outcome -
          lag(outcome, 1).over(
            Window.partitionBy(col(getUnitCol)).orderBy(col(getTimeCol))
          )).as(diffCol)
      )
      .agg(stddev_samp(col(diffCol)))
      .head

    val nTreatedPost = data.filter(treatment and postTreatment).count
    val zeta = math.pow(nTreatedPost, 0.25) * firstDiffStd
    zeta
  })

  private[causal] def fitUnitWeights(indexedPreDf: DataFrame,
                                     zeta: Double,
                                     fitIntercept: Boolean,
                                     size: (Long, Long))
                                    (unitIdxCol: String, timeIdxCol: String): (DVector, Double, Double, Seq[Double]) =
    logVerb("fitUnitWeights", {
      val outcomePreControl = indexedPreDf.filter(not(treatment))
        .toDMatrix(timeIdxCol, unitIdxCol, getOutcomeCol)

      val outcomePreTreatMean = indexedPreDf.filter(treatment)
        .groupBy(col(timeIdxCol).as("i"))
        .agg(avg(outcome).as("value"))
        .as[VectorEntry]

      val lambda = if (zeta == 0) 0d else {
        val t_pre = matrixOps.size(outcomePreControl)._1 // # of time periods pre-treatment
        zeta * zeta * t_pre
      }

      solveCLS(outcomePreControl, outcomePreTreatMean, lambda, fitIntercept, size)
    })

  private[causal] def handleMissingOutcomes(indexed: DataFrame, maxTimeLength: Int)
                                           (unitIdxCol: String, timeIdxCol: String): DataFrame = {
    // "skip", "zero", "impute"
    getHandleMissingOutcome match {
      case "skip" =>
        val timeCountCol = DatasetExtensions.findUnusedColumnName("time_count", indexed)
        indexed.withColumn(timeCountCol, count(col(timeIdxCol)).over(Window.partitionBy(col(unitIdxCol))))
          // Only skip units from the control_pre group where there is missing data.
          .filter(col(timeCountCol) === lit(maxTimeLength) or treatment or postTreatment)
          .drop(timeCountCol)
      case "zero" =>
        indexed
      case "impute" =>
        // Only impute the control_pre group.
        val controlPre = indexed.filter(not(treatment) and not(postTreatment))

        val imputed = imputeTimeSeries(controlPre, maxTimeLength, getOutcomeCol, unitIdxCol, timeIdxCol)
          .withColumn(getTreatmentCol, lit(false))
          .withColumn(getPostTreatmentCol, lit(false))

        indexed.as("l").join(
          imputed.as("r"),
          col(s"l.$unitIdxCol") === col(s"r.$unitIdxCol") and col(s"l.$timeIdxCol") === col(s"r.$timeIdxCol"),
          "full_outer"
        ).select(
          coalesce(col(s"l.$unitIdxCol"), col(s"r.$unitIdxCol")).as(unitIdxCol),
          coalesce(col(s"l.$timeIdxCol"), col(s"r.$timeIdxCol")).as(timeIdxCol),
          coalesce(col(s"l.$getOutcomeCol"), col(s"r.$getOutcomeCol")).as(getOutcomeCol),
          coalesce(col(s"l.$getTreatmentCol"), col(s"r.$getTreatmentCol")).as(getTreatmentCol),
          coalesce(col(s"l.$getPostTreatmentCol"), col(s"r.$getPostTreatmentCol")).as(getPostTreatmentCol)
        )
    }
  }
}

object SyntheticEstimator {
  private[causal] def imputeTimeSeries(df: DataFrame, maxTimeLength: Int,
                                       outcomeCol: String, unitIdxCol: String, timeIdxCol: String): DataFrame = {
    val impute: UserDefinedFunction = udf(imputeMissingValues(maxTimeLength) _)

    df
      // zip time and outcomes
      .select(col(unitIdxCol), struct(col(timeIdxCol), col(outcomeCol)).as(outcomeCol))
      .groupBy(unitIdxCol)
      // construct a map of time -> outcome per unit
      .agg(map_from_entries(collect_set(col(outcomeCol))).as(outcomeCol))
      // impute and explode back
      .select(col(unitIdxCol), explode(impute(col(outcomeCol))).as("exploded"))
      .select(
        col(unitIdxCol),
        col("exploded._1").as(timeIdxCol),
        col("exploded._2").as(outcomeCol)
      )
  }

  private[causal] def imputeMissingValues(maxLength: Int)(values: Map[Int, Double]): Seq[(Int, Double)] = {
    val range = 0 until maxLength

    // Find the nearest neighbors using collectFirst
    def findNeighbor(direction: Int, curr: Int): Option[Double] = {
      val searchRange = if (direction == -1) range.reverse else range
      searchRange.collectFirst {
        case j if j * direction > curr * direction && values.contains(j) => values(j)
      }
    }

    range.map {
      i =>
        if (values.contains(i))
          (i, values(i))
        else {
          (findNeighbor(-1, i), findNeighbor(1, i)) match {
            case (Some(left), Some(right)) => (i, (left + right) / 2.0)
            case (Some(left), None) => (i, left)
            case (None, Some(right)) => (i, right)
            case (None, None) => (i, 0.0) // Should never happen
          }
        }
    }
  }

  private[causal] def convertToBDM(mat: Array[MatrixEntry], size: (Long, Long)): BDM[Double] = {

    val denseMatrix = BDM.zeros[Double](size._1.toInt, size._2.toInt)
    mat.foreach(entry => {
      denseMatrix(entry.i.toInt, entry.j.toInt) = entry.value
    })

    denseMatrix
  }

  private[causal] def convertToBDV(vec: Array[VectorEntry], size: Long): BDV[Double] = {
    val denseVector = BDV.zeros[Double](size.toInt)

    vec.foreach(entry => {
      denseVector(entry.i.toInt) = entry.value
    })

    denseVector
  }

  private[causal] def assignRowIndex(df: DataFrame, colName: String): DataFrame = {
    df.sparkSession.createDataFrame(
      df.rdd.zipWithIndex.map(element =>
        Row.fromSeq(Seq(element._2) ++ element._1.toSeq)
      ),
      StructType(
        Array(StructField(colName, LongType, nullable = false)) ++ df.schema.fields
      )
    )
  }

  private[causal] def createIndex(data: DataFrame, inputCol: String, indexCol: String): DataFrame = {
    // The orderBy operation is needed here when creating index for Time. When fitting unit weights,
    // we need to compute the regularization term zeta, which depends on the first order difference of
    // the outcome, ordered by time. If we don't order the data by time when creating the time index,
    // the regularization term will be incorrect.
    assignRowIndex(data.select(col(inputCol)).distinct.orderBy(col(inputCol)), indexCol)
  }
}
