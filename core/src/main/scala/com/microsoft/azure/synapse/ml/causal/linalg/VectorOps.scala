package com.microsoft.azure.synapse.ml.causal.linalg

import breeze.linalg.{norm, DenseVector => BDV, max => bmax, sum => bsum}
import breeze.numerics.{abs => babs, exp => bexp}
import breeze.stats.distributions.{Rand, RandBasis}
import breeze.stats.{mean => bmean}
import org.apache.spark.mllib.random.RandomRDDs.uniformRDD
import org.apache.spark.sql.functions.{coalesce, col, lit}
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

  def uniformRandom(size: Long, seed: Long = util.Random.nextLong): T
}

object DVectorOps extends VectorOps[DVector] {

  private implicit val VectorEntryEncoder: Encoder[VectorEntry] = Encoders.product[VectorEntry]

  /**
    * sqrt( x'*x )
    */
  def nrm2(vector: DVector): Double = {
    val Row(sumOfSquares: Double) = vector
      .agg(org.apache.spark.sql.functions.sum(col("value") * col("value")))
      .head
    math.sqrt(sumOfSquares)
  }

  /**
    * alpha * x + y
    */
  def axpy(x: DVector, yOpt: Option[DVector], alpha: Double = 1d): DVector = {
    val ax = x.select(col("i"), (lit(alpha) * col("value")).as("value"))

    yOpt.map {
        y =>
          ax.as("l")
            .join(y.as("r"), col("l.i") === col("r.i"), "outer")
            .select(
              coalesce(col("l.i"), col("r.i")).as("i"),
              (coalesce(col("l.value"), lit(0d))
                + coalesce(col("r.value"), lit(0d))).as("value")
            )
      }
      .getOrElse(ax)
      .as[VectorEntry]
  }

  def maxAbs(vec: DVector): Double = {
    val Row(value: Double) = vec
      .agg(org.apache.spark.sql.functions.max(org.apache.spark.sql.functions.abs(col("value"))))
      .head
    value
  }

  def sum(vec: DVector): Double = {
    val Row(value: Double) = vec
      .agg(org.apache.spark.sql.functions.sum(col("value")))
      .head
    value
  }

  def elementwiseProduct(vec1: DVector, vec2: DVector): DVector = {
    vec1.as("l").join(vec2.as("r"), col("l.i") === col("r.i"), "inner")
      .select(
        col("l.i").as("i"),
        (col("l.value") * col("r.value")).alias("value")
      ).as[VectorEntry]
  }

  def uniformRandom(size: Long, seed: Long = util.Random.nextLong): DVector = {
    val spark = SparkSession.active
    import spark.implicits._
    val rdd = uniformRDD(spark.sparkContext, size, seed = seed).zipWithIndex.map(t => t.swap)
    rdd.toDF("i", "value").as[VectorEntry]
  }

  override def exp(vec: DVector): DVector = {
    vec.select(
      col("i"),
      org.apache.spark.sql.functions.exp(col("value")).as("value")
    ).as[VectorEntry]
  }

  override def center(vec: DVector): DVector = {
    val m = mean(vec)
    vec.select(
      col("i"),
      (col("value") - lit(m)).as("value")
    ).as[VectorEntry]
  }

  override def mean(vec: DVector): Double = {
    val Row(mean: Double) = vec.agg(org.apache.spark.sql.functions.mean(col("value"))).head
    mean
  }

  override def dot(vec1: DVector, vec2: DVector): Double = {
    val Row(dot: Double) = vec1.as("l")
      .join(vec2.as("r"), col("l.i") === col("r.i"), "inner")
      .agg(org.apache.spark.sql.functions.sum(col("l.value") * col("r.value")).as("value"))
      .head
    dot
  }
}

object BzVectorOps extends VectorOps[BDV[Double]] {
  /**
    * sqrt( x'*x )
    */
  override def nrm2(vector: BDV[Double]): Double = norm(vector, 2)

  /**
    * alpha * x + y
    */
  override def axpy(x: BDV[Double], yOpt: Option[BDV[Double]], alpha: Double = 1d): BDV[Double] = {
    val ax = x * alpha
    yOpt.map(_ + ax).getOrElse(ax)
  }

  override def maxAbs(vec: BDV[Double]): Double = {
    bmax(babs(vec))
  }

  override def sum(vec: BDV[Double]): Double = {
    bsum(vec)
  }

  override def elementwiseProduct(vec1: BDV[Double], vec2: BDV[Double]): BDV[Double] = {
    vec1 *:* vec2
  }

  override def uniformRandom(size: Long, seed: Long = util.Random.nextLong): BDV[Double] = {
    val r = RandBasis.withSeed(seed.toInt).uniform
    BDV.rand(size.toInt, r)
  }

  override def exp(vec: BDV[Double]): BDV[Double] = {
    bexp(vec)
  }

  override def center(vec: BDV[Double]): BDV[Double] = {
    vec - bmean(vec)
  }

  override def mean(vec: BDV[Double]): Double = {
    bmean(vec)
  }

  override def dot(vec1: BDV[Double], vec2: BDV[Double]): Double = {
    vec1 dot vec2
  }
}
