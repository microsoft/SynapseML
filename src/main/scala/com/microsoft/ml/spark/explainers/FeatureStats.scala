package com.microsoft.ml.spark.explainers

import breeze.numerics.abs
import breeze.stats.distributions.{RandBasis, Uniform}

private[explainers] trait FeatureStats[T] {
  def sample(instance: T)(implicit randBasis: RandBasis): T

  def getDistance(instance: T, sample: T): Double
}

private[explainers] final case class ContinuousFeatureStats(stddev: Double)
  extends FeatureStats[Double] with ContinuousFeatureSampler

private[explainers] trait ContinuousFeatureSampler extends FeatureStats[Double] {
  self: ContinuousFeatureStats =>

  override def sample(instance: Double)(implicit randBasis: RandBasis): Double = {
    randBasis.gaussian(instance, self.stddev).sample
  }

  override def getDistance(instance: Double, sample: Double): Double = {
    if (self.stddev == 0d) {
      0d
    } else {
      // Normalize by stddev
      abs(sample - instance) / self.stddev
    }
  }
}

private[explainers] final case class DiscreteFeatureStats(freq: Map[Double, Double])
  extends FeatureStats[Double] with DiscreteFeatureSampler

private[explainers] trait DiscreteFeatureSampler extends FeatureStats[Double] {
  self: DiscreteFeatureStats =>

  /**
    * Returns the cumulative density function (CDF) of the given frequency table.
    */
  private def cdf[V](freq: Seq[(V, Double)]): Seq[(V, Double)] = {
    freq.map(_._1) zip freq.map(_._2).scanLeft(0d)(_ + _).drop(1)
  }

  private val cdfTable: Seq[(Double, Double)] = {
    cdf(this.freq.toSeq)
  }

  override def sample(instance: Double)(implicit randBasis: RandBasis): Double = {
    val r = Uniform(0d, freq.values.sum).sample
    cdfTable.find(r <= _._2).get._1
  }

  override def getDistance(instance: Double, state: Double): Double = {
    if (instance == state) 0d else 1d
  }
}