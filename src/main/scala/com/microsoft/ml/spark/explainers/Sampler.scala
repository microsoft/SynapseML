package com.microsoft.ml.spark.explainers

import breeze.linalg.{norm, DenseVector => BDV}
import breeze.numerics.abs
import breeze.stats.distributions.{RandBasis, Uniform}
import com.microsoft.ml.spark.explainers.RowUtils.RowCanGetAsDouble
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

private[explainers] trait Sampler[T] extends Serializable {
  /**
    * Generates some samples based on the specified instance.
    */
  def sample(instance: T)(implicit randBasis: RandBasis): (T, Double)
}

//private[explainers] class KernelSHAPSampler[T](coalition: BDV[Int], background: T) extends Sampler[T] {
//  override def sample(instance: T): T = {
//    ???
//  }
//}

private[explainers] object RowUtils {
  implicit class RowCanGetAsDouble(row: Row) {
    def getAsDouble(col: String): Double = {
      val id = row.fieldIndex(col)
      row.get(id) match {
        case v: Byte => v.toDouble
        case v: Short => v.toDouble
        case v: Int => v.toDouble
        case v: Long => v.toDouble
        case v: Float => v.toDouble
        case v: Double => v
        case v => throw new Exception(s"Cannot convert $v to Double.")
      }
    }
  }
}

private[explainers] trait FeatureStats extends Sampler[Double] {
  def name: String

  def toStructField: StructField = StructField(this.name, DoubleType)
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
    // TODO: Move this logic upwards to reduce memory cost.
    // Sort by frequency in descending order, take top 1000, then order by key.
    val maxFeatureMembers: Int = 1000
    val processedFreq = this.freq.toSeq.sortBy(-_._2).take(maxFeatureMembers)
    cdf(processedFreq)
  }

  override def sample(instance: Double)(implicit randBasis: RandBasis): (Double, Double) = {
    val r = Uniform(0d, freq.values.sum).sample()
    val sample = cdfTable.find(r <= _._2).get._1
    (sample, if (sample == instance) 0d else 1d)
  }
}

private final case class ContinuousFeatureStats
  (override val name: String, stddev: Double)
  extends FeatureStats with ContinuousFeatureSampler

private final case class DiscreteFeatureStats
  (override val name: String, freq: Map[Double, Double])
  extends FeatureStats with DiscreteFeatureSampler

private[explainers] class LIMEVectorSampler(featureStats: Seq[FeatureStats])
  extends Sampler[BDV[Double]] {
  override def sample(instance: BDV[Double])(implicit randBasis: RandBasis): (BDV[Double], Double) = {
    val r = instance.mapPairs {
      case (idx, value) => featureStats(idx).sample(value)
    }

    val n = featureStats.size
    (r.mapValues(_._1), norm(r.mapValues(_._2), 2) / math.sqrt(n))
  }
}

private[explainers] class LIMETabularSampler(featureStats: Seq[FeatureStats])
  extends Sampler[Row] {

  override def sample(instance: Row)(implicit randBasis: RandBasis): (Row, Double) = {
    val (samples, distances) = featureStats.map {
      stats: FeatureStats =>
        val value = instance.getAsDouble(stats.name)
        val (sample, distance) = stats.sample(value)
        // (stats.fromDouble(sample), distance)
        (sample, distance)
    }.unzip

    val n = featureStats.size

    // Set distance to normalized Euclidean distance
    (Row.fromSeq(samples), norm(BDV(distances: _*), 2) / math.sqrt(n))
  }
}