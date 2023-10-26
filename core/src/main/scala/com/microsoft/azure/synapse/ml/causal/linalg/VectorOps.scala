// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal.linalg

import breeze.linalg.{norm, DenseVector => BDV, max => bmax, sum => bsum}
import breeze.numerics.{abs => babs, exp => bexp}
import breeze.stats.{mean => bmean}
import org.apache.spark.sql.functions.{
  coalesce, col, lit, abs => sabs, exp => sexp, max => smax, mean => smean, sum => ssum
}
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}

case class VectorEntry(i: Long, value: Double)

trait VectorOps[T] {
  /**
    * sqrt( x'*x )
    */
  def nrm2(vector: T): Double

  /**
    * alpha * x + y
    */
  def axpy(x: T, yOpt: Option[T], alpha: Double = 1d): T

  def maxAbs(vec: T): Double

  def exp(vec: T): T

  def sum(vec: T): Double

  def elementwiseProduct(vec1: T, vec2: T): T

  def center(vec: T): T

  def mean(vec: T): Double

  def dot(vec1: T, vec2: T): Double

  def make(size: Long, value: => Double): T
}

object DVectorOps extends VectorOps[DVector] {

  private implicit val VectorEntryEncoder: Encoder[VectorEntry] = Encoders.product[VectorEntry]

  /**
    * sqrt( x'*x )
    */
  def nrm2(vector: DVector): Double = {
    val Row(sumOfSquares: Double) = vector
      .agg(ssum(col("value") * col("value")))
      .head
    math.sqrt(sumOfSquares)
  }

  /**
    * alpha * x + y
    */
  def axpy(x: DVector, yOpt: Option[DVector], alpha: Double = 1d): DVector = {
    val ax = x.toDVector(col("i"), lit(alpha) * col("value"))

    yOpt.map {
        y =>
          ax.as("l")
            .join(y.as("r"), col("l.i") === col("r.i"), "outer")
            .toDVector(
              coalesce(col("l.i"), col("r.i")),
              coalesce(col("l.value"), lit(0d))
                + coalesce(col("r.value"), lit(0d))
            )
      }
      .getOrElse(ax)
  }

  def maxAbs(vec: DVector): Double = {
    val Row(value: Double) = vec
      .agg(smax(sabs(col("value"))))
      .head
    value
  }

  def sum(vec: DVector): Double = {
    val Row(value: Double) = vec
      .agg(ssum(col("value")))
      .head
    value
  }

  def elementwiseProduct(vec1: DVector, vec2: DVector): DVector = {
    vec1.as("l").join(vec2.as("r"), col("l.i") === col("r.i"), "inner")
      .toDVector(col("l.i"), col("l.value") * col("r.value"))
  }

  def make(size: Long, value: => Double): DVector = {
    val spark = SparkSession.active
    import spark.implicits._
    val data = 0L until size
    spark
      .sparkContext
      .parallelize(data)
      .toDF("i")
      .withColumn("value", lit(value))
      .as[VectorEntry]
  }

  def exp(vec: DVector): DVector = {
    vec.toDVector(
      col("i"),
      sexp(col("value"))
    )
  }

  def center(vec: DVector): DVector = {
    val m = mean(vec)
    vec.toDVector(
      col("i"),
      col("value") - lit(m)
    )
  }

  def mean(vec: DVector): Double = {
    val Row(mean: Double) = vec.agg(smean(col("value"))).head
    mean
  }

  def dot(vec1: DVector, vec2: DVector): Double = {
    val Row(dot: Double) = vec1.as("l")
      .join(vec2.as("r"), col("l.i") === col("r.i"), "inner")
      .agg(ssum(col("l.value") * col("r.value")).as("value"))
      .head
    dot
  }
}

object BzVectorOps extends VectorOps[BDV[Double]] {
  /**
    * sqrt( x'*x )
    */
  def nrm2(vector: BDV[Double]): Double = norm(vector, 2)

  /**
    * alpha * x + y
    */
  def axpy(x: BDV[Double], yOpt: Option[BDV[Double]], alpha: Double = 1d): BDV[Double] = {
    val ax = x * alpha
    yOpt.map(_ + ax).getOrElse(ax)
  }

  def maxAbs(vec: BDV[Double]): Double = {
    bmax(babs(vec))
  }

  def sum(vec: BDV[Double]): Double = {
    bsum(vec)
  }

  def elementwiseProduct(vec1: BDV[Double], vec2: BDV[Double]): BDV[Double] = {
    vec1 *:* vec2
  }

  def make(size: Long, value: => Double): BDV[Double] = {
    BDV.fill(size.toInt)(value)
  }

  def exp(vec: BDV[Double]): BDV[Double] = {
    bexp(vec)
  }

  def center(vec: BDV[Double]): BDV[Double] = {
    vec - bmean(vec)
  }

  def mean(vec: BDV[Double]): Double = {
    bmean(vec)
  }

  def dot(vec1: BDV[Double], vec2: BDV[Double]): Double = {
    vec1 dot vec2
  }
}
