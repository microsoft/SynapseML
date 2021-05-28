package com.microsoft.ml.spark.explainers

import breeze.linalg.{norm, DenseVector => BDV}
import breeze.numerics.abs
import breeze.stats.distributions.{RandBasis, Uniform}
import com.microsoft.ml.spark.explainers.RowUtils.RowCanGetAsDouble
import org.apache.spark.sql.Row
import org.apache.spark.ml.linalg.{Vector => SV, DenseVector => SDV}

private[explainers] trait Sampler[T] extends Serializable {
  /**
    * Generates some samples based on the specified instance.
    */
  def sample(instance: T)(implicit randBasis: RandBasis): (T, Double)
}

private[explainers] trait FeatureStats extends Sampler[Double] {
  def fieldIndex: Int
}

private trait ContinuousFeatureSampler extends Sampler[Double] {
  self: ContinuousFeatureStats =>

  override def sample(value: Double)
                     (implicit randBasis: RandBasis): (Double, Double) = {
    val sample = randBasis.gaussian(value, self.stddev).sample()
    val distance = if (self.stddev == 0d) {
      0d
    } else {
      // Normalize by stddev
      abs(sample - value) / self.stddev
    }

    (sample, distance)
  }
}

private trait DiscreteFeatureSampler extends Sampler[Double] {
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

  override def sample(instance: Double)(implicit randBasis: RandBasis): (Double, Double) = {
    val r = Uniform(0d, freq.values.sum).sample()
    val sample = cdfTable.find(r <= _._2).get._1
    (sample, if (sample == instance) 0d else 1d)
  }
}

private final case class ContinuousFeatureStats
  (override val fieldIndex: Int, stddev: Double)
  extends FeatureStats with ContinuousFeatureSampler

private final case class DiscreteFeatureStats
  (override val fieldIndex: Int, freq: Map[Double, Double])
  extends FeatureStats with DiscreteFeatureSampler

private[explainers] class LIMEVectorSampler(featureStats: Seq[FeatureStats])
  extends Sampler[SV] {
  override def sample(instance: SV)(implicit randBasis: RandBasis): (SV, Double) = {
    val (samples, distances) = featureStats.map {
      stats =>
        val value = instance(stats.fieldIndex)
        stats.sample(value)
    }.unzip match {
      case (samples, distances) => (new SDV(samples.toArray), BDV(distances: _*))
    }

    val n = featureStats.size

    // Set distance to normalized Euclidean distance
    (samples, norm(distances, 2) / math.sqrt(n))
  }
}

private[explainers] class LIMETabularSampler(featureStats: Seq[FeatureStats])
  extends Sampler[Row] {

  override def sample(instance: Row)(implicit randBasis: RandBasis): (Row, Double) = {
    val (samples, distances) = featureStats.map {
      stats: FeatureStats =>
        val value = instance.getAsDouble(stats.fieldIndex)
        val (sample, distance) = stats.sample(value)
        (sample, distance)
    }.unzip

    val n = featureStats.size

    // Set distance to normalized Euclidean distance
    (Row.fromSeq(samples), norm(BDV(distances: _*), 2) / math.sqrt(n))
  }
}