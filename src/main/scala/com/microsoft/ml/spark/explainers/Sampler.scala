package com.microsoft.ml.spark.explainers

import breeze.linalg.{DenseVector => BDV}
import breeze.stats.distributions.{RandBasis, Uniform}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

private[explainers] trait Sampler[T] {
  /**
    * Generates some samples based on the specified instance.
    */
  def sample(instance: T)(implicit randBasis: RandBasis): T
}

//private[explainers] class KernelSHAPSampler[T](coalition: BDV[Int], background: T) extends Sampler[T] {
//  override def sample(instance: T): T = {
//    ???
//  }
//}

private abstract class FeatureStats(dataType: DataType) extends Sampler[Double] {
  def parseFrom(value: Any): Double = {
    dataType match {
      case _: ByteType => value.asInstanceOf[Byte].toDouble
      case _: ShortType => value.asInstanceOf[Short].toDouble
      case _: IntegerType => value.asInstanceOf[Int].toDouble
      case _: LongType => value.asInstanceOf[Long].toDouble
      case _: FloatType => value.asInstanceOf[Float].toDouble
      case _: DoubleType => value.asInstanceOf[Double]
    }
  }

  def convertDouble(value: Double): Any = {
    dataType match {
      case _: ByteType => value.toByte
      case _: ShortType => value.toShort
      case _: IntegerType => value.toInt
      case _: LongType => value.toLong
      case _: FloatType => value.toFloat
      case _: DoubleType => value
    }
  }
}

private trait ContinuousFeatureSampler extends Sampler[Double] {
  self: ContinuousFeatureStats =>

  override def sample(value: Double)(implicit randBasis: RandBasis): Double = {
    randBasis.gaussian(value, self.std).sample()
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

  override def sample(instance: Double)(implicit randBasis: RandBasis): Double = {
    val r = Uniform(0d, freq.values.sum).sample()
    cdfTable.find(r <= _._2).get._1
  }
}

private final case class ContinuousFeatureStats(std: Double, dataType: DataType)
  extends FeatureStats(dataType) with ContinuousFeatureSampler

private final case class DiscreteFeatureStats(val freq: Map[Double, Double], dataType: DataType)
  extends FeatureStats(dataType) with DiscreteFeatureSampler

private[explainers] class LIMEVectorSampler(featureStats: Seq[FeatureStats])
  extends Sampler[BDV[Double]] {
  override def sample(instance: BDV[Double])(implicit randBasis: RandBasis): BDV[Double] = {
    instance.mapPairs {
      case (idx, value) => featureStats(idx).sample(value)
    }
  }
}

private[explainers] class LIMETabularSampler(featureStats: Seq[FeatureStats])
  extends Sampler[Row] {

  override def sample(instance: Row)(implicit randBasis: RandBasis): Row = {
    val samples = featureStats.zipWithIndex.map {
      case (stats: FeatureStats, idx: Int) =>
        val value = instance.get(idx)
        val valueAsDouble = stats.parseFrom(value)
        val sample = stats.sample(valueAsDouble)
        stats.convertDouble(sample)
    }

    Row.fromSeq(samples)
  }
}
