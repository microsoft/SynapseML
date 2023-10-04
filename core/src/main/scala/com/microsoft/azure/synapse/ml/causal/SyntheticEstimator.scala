package com.microsoft.azure.synapse.ml.causal

import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row}
import org.apache.spark.sql.types._
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import com.microsoft.azure.synapse.ml.causal.linalg._
import com.microsoft.azure.synapse.ml.causal.opt.ConstrainedLeastSquare
import scala.util.Random

trait SyntheticEstimator {
  this: DiffInDiffEstimatorParams with SyntheticEstimatorParams =>

  import SyntheticEstimator._

  implicit val vectorOps: DVectorOps.type = DVectorOps
  implicit val matrixOps: DMatrixOps.type = DMatrixOps
  implicit val matrixEntryEncoder: Encoder[MatrixEntry] = Encoders.product[MatrixEntry]
  implicit val vectorEntryEncoder: Encoder[VectorEntry] = Encoders.product[VectorEntry]

  private[causal] lazy val postTreatment = col(getPostTreatmentCol)
  private[causal] lazy val treatment = col(getTreatmentCol)
  private[causal] lazy val outcome = col(getOutcomeCol)
  private[causal] val interactionCol = "interaction"
  private[causal] val weightsCol = "weights"
  private[causal] val epsilon = 1E-10

  private def solveCLS(A: DMatrix, b: DVector, lambda: Double, fitIntercept: Boolean, seed: Long): (DVector, Double) = {
    val size = matrixOps.size(A)
    if (size._1 * size._2 <= 1000000) {
      // If matrix size is less than 1M, collect the data on the driver node and solve it locally, where matrix-vector
      // multiplication is done with breeze. It's much faster than solving with Spark at scale.
      implicit val bzMatrixOps: MatrixOps[BDM[Double], BDV[Double]] = BzMatrixOps
      implicit val bzVectorOps: VectorOps[BDV[Double]] = BzVectorOps
      implicit val cacheOps: CacheOps[BDV[Double]] = BDVCacheOps

      val bzA = convertToBDM(A.collect())
      val bzb = convertToBDV(b.collect())
      val solver = new ConstrainedLeastSquare[BDM[Double], BDV[Double]](
        step = this.getStepSize, maxIter = this.getMaxIter,
        numIterNoChange = getNumIterNoChange, tol = this.getTol
      )

      val (x, intercept) = solver.solve(bzA, bzb, lambda, fitIntercept, seed)
      val xdf = A.sparkSession.createDataset[VectorEntry](x.mapPairs((i, v) => VectorEntry(i, v)).toArray.toSeq)
      (xdf, intercept)
    } else {
      implicit val cacheOps: CacheOps[DVector] = DVectorCacheOps
      val solver = new ConstrainedLeastSquare[DMatrix, DVector](
        step = this.getStepSize, maxIter = this.getMaxIter,
        numIterNoChange = getNumIterNoChange, tol = this.getTol
      )

      solver.solve(A, b, lambda, fitIntercept, seed)
    }
  }

  private[causal] def fitTimeWeights(indexedControlDf: DataFrame, seed: Long = Random.nextLong): (DVector, Double) = {
    val indexedPreControl = indexedControlDf.filter(not(postTreatment)).cache

    val outcomePre = indexedPreControl
      .select(
        col(UnitIdxCol).as("i"),
        col(TimeIdxCol).as("j"),
        col(getOutcomeCol).as("value")
      )
      .as[MatrixEntry]

    val outcomePostMean = indexedControlDf.filter(postTreatment)
      .groupBy(col(UnitIdxCol).as("i"))
      .agg(avg(col(getOutcomeCol)).as("value"))
      .as[VectorEntry]

    solveCLS(outcomePre, outcomePostMean, lambda = 0d, fitIntercept = true, seed)
  }

  private[causal] def calculateRegularization(data: DataFrame): Double = {
    val Row(firstDiffStd: Double) = data
      .filter(not(treatment) and not(postTreatment))
      .select(
        (outcome -
          lag(outcome, 1).over(
            Window.partitionBy(col(getUnitCol)).orderBy(col(getTimeCol))
          )).as("diff")
      )
      .agg(stddev_samp(col("diff")))
      .head

    val nTreatedPost = data.filter(treatment and postTreatment).count
    val zeta = math.pow(nTreatedPost, 0.25) * firstDiffStd
    zeta
  }

  private[causal] def fitUnitWeights(indexedPreDf: DataFrame,
                                   zeta: Double,
                                   fitIntercept: Boolean,
                                   seed: Long = Random.nextLong): (DVector, Double) = {


    val outcomePreControl = indexedPreDf.filter(not(treatment))
      .select(col(TimeIdxCol).as("i"), col(UnitIdxCol).as("j"), outcome.as("value"))
      .as[MatrixEntry]

    val outcomePreTreatMean = indexedPreDf.filter(treatment)
      .groupBy(col(TimeIdxCol).as("i"))
      .agg(avg(outcome).as("value"))
      .as[VectorEntry]

    val lambda = if (zeta == 0) 0d else {
      val t_pre = matrixOps.size(outcomePreControl)._1 // # of time periods pre-treatment
      zeta * zeta * t_pre
    }

    val (weights, intercept) = solveCLS(outcomePreControl, outcomePreTreatMean, lambda, fitIntercept, seed)
    (weights, intercept)
  }

  private[causal] def handleMissingOutcomes(indexed: DataFrame, maxTimeLength: Int): DataFrame = {
    // "skip", "zero", "impute"
    getHandleMissingOutcome match {
      case "skip" =>
        indexed.withColumn("time_count", count(col(TimeIdxCol)).over(Window.partitionBy(col(UnitIdxCol))))
          // Only skip units from the control_pre group where there is missing data.
          .filter(col("time_count") === lit(maxTimeLength) or treatment or postTreatment)
          .drop("time_count")
      case "zero" =>
        indexed
      case "impute" =>
        // Only impute the control_pre group.
        val controlPre = indexed.filter(not(treatment) and not(postTreatment))

        val imputed = imputeTimeSeries(controlPre, maxTimeLength)
          .withColumn(getTreatmentCol, lit(false))
          .withColumn(getPostTreatmentCol, lit(false))

        indexed.as("l").join(
          imputed.as("r"),
          col(s"l.$UnitIdxCol") === col(s"r.$UnitIdxCol") and col(s"l.$TimeIdxCol") === col(s"r.$TimeIdxCol"),
          "full_outer"
        ).select(
          coalesce(col(s"l.$UnitIdxCol"), col(s"r.$UnitIdxCol")).as(UnitIdxCol),
          coalesce(col(s"l.$TimeIdxCol"), col(s"r.$TimeIdxCol")).as(TimeIdxCol),
          coalesce(col(s"l.$getOutcomeCol"), col(s"r.$getOutcomeCol")).as(getOutcomeCol),
          coalesce(col(s"l.$getTreatmentCol"), col(s"r.$getTreatmentCol")).as(getTreatmentCol),
          coalesce(col(s"l.$getPostTreatmentCol"), col(s"r.$getPostTreatmentCol")).as(getPostTreatmentCol)
        )
    }
  }

  private def imputeTimeSeries(df: DataFrame, maxTimeLength: Int): DataFrame = {
    val impute: UserDefinedFunction = udf((timeIndex: Seq[Int], values: Seq[Double]) => {
      val valueMap = timeIndex.zip(values).toMap
      val range = 0 until maxTimeLength
      range.map { i => valueMap.getOrElse(i, imputeMissingValue(i, valueMap, range)) }
    })

    df.groupBy(UnitIdxCol).agg(
        collect_list(TimeIdxCol).as(TimeIdxCol), collect_list(outcome).as(getOutcomeCol)
      )
      .select(col(UnitIdxCol), explode(impute(col(TimeIdxCol), outcome)).as("exploded"))
      .select(
        col(UnitIdxCol),
        col("exploded._1").as(TimeIdxCol),
        col("exploded._2").as(getOutcomeCol)
      )
  }
}

object SyntheticEstimator {
  val UnitIdxCol = "Unit_idx"
  val TimeIdxCol = "Time_idx"

  private def imputeMissingValue(i: Int, valueMap: Map[Int, Double], range: Range): (Int, Double) = {
    // Find the nearest neighbors using collectFirst
    def findNeighbor(direction: Int): Option[Double] = {
      val searchRange = if (direction == -1) range.reverse else range
      searchRange.collectFirst {
        case j if j * direction > i * direction && valueMap.contains(j) => valueMap(j)
      }
    }

    (findNeighbor(-1), findNeighbor(1)) match {
      case (Some(left), Some(right)) => (i, (left + right) / 2.0)
      case (Some(left), None) => (i, left)
      case (None, Some(right)) => (i, right)
      case (None, None) => (i, 0.0) // Should never happen
    }
  }

  private def convertToBDM(mat: Array[MatrixEntry]): BDM[Double] = {
    val numRows = mat.map(_.i).max.toInt + 1
    val numCols = mat.map(_.j).max.toInt + 1
    val denseMatrix = BDM.zeros[Double](numRows, numCols)
    mat.foreach(entry => {
      denseMatrix(entry.i.toInt, entry.j.toInt) = entry.value
    })

    denseMatrix
  }

  private def convertToBDV(vec: Array[VectorEntry]): BDV[Double] = {
    val length = vec.map(_.i).max.toInt + 1
    val denseVector = BDV.zeros[Double](length)

    vec.foreach(entry => {
      denseVector(entry.i.toInt) = entry.value
    })

    denseVector
  }

  private def assignRowIndex(df: DataFrame, colName: String): DataFrame = {
    df.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map(element =>
        Row.fromSeq(Seq(element._2) ++ element._1.toSeq)
      ),
      StructType(
        Array(StructField(colName, LongType, nullable = false)) ++ df.schema.fields
      )
    )
  }

  private[causal] def createIndex(data: DataFrame, inputCol: String, indexCol: String): DataFrame = {
    assignRowIndex(data.select(col(inputCol)).distinct.orderBy(col(inputCol)), indexCol)
  }
}