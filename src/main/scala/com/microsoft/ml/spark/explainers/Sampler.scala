package com.microsoft.ml.spark.explainers

import breeze.linalg.{BitVector, axpy, norm, DenseVector => BDV}
import breeze.numerics.abs
import breeze.stats.distributions.{RandBasis, Uniform}
import com.microsoft.ml.spark.explainers.BreezeUtils._
import com.microsoft.ml.spark.explainers.RowUtils.RowCanGetAsDouble
import com.microsoft.ml.spark.io.image.ImageUtils
import com.microsoft.ml.spark.lime.{Superpixel, SuperpixelData}
import org.apache.spark.sql.Row
import org.apache.spark.ml.linalg.{DenseVector => SDV, Vector => SV}

import java.awt.image.BufferedImage

private[explainers] trait Sampler[TObservation, TFeature] extends Serializable {
  /**
    * Generates a sample based on the specified instance, together with the distance metric between
    * the generated sample and the original instance.
    */
  def sample(instance: TObservation)(implicit randBasis: RandBasis): (TObservation, TFeature, Double)
}

private[explainers] trait FeatureStats[TObservation, TFeature] extends Sampler[TObservation, TFeature] {
  def fieldIndex: Int
}

private final case class ContinuousFeatureStats
(override val fieldIndex: Int, stddev: Double)
  extends FeatureStats[Double, Double] with ContinuousFeatureSampler

private trait ContinuousFeatureSampler extends Sampler[Double, Double] {
  self: ContinuousFeatureStats =>

  override def sample(value: Double)
                     (implicit randBasis: RandBasis): (Double, Double, Double) = {
    val sample = randBasis.gaussian(value, self.stddev).sample()
    val distance = if (self.stddev == 0d) {
      0d
    } else {
      // Normalize by stddev
      abs(sample - value) / self.stddev
    }

    (sample, sample, distance)
  }
}

private final case class DiscreteFeatureStats
(override val fieldIndex: Int, freq: Map[Double, Double])
  extends FeatureStats[Double, Double] with DiscreteFeatureSampler

private trait DiscreteFeatureSampler extends Sampler[Double, Double] {
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

  override def sample(instance: Double)(implicit randBasis: RandBasis): (Double, Double, Double) = {
    val r = Uniform(0d, freq.values.sum).sample()
    val sample = cdfTable.find(r <= _._2).get._1
    (sample, sample, if (sample == instance) 0d else 1d)
  }
}

private case class ImageFormat(origin: Option[String],
                                height: Int,
                                width: Int,
                                nChannels: Int,
                                mode: Int,
                                data: Array[Byte])

private final case class ImageFeature(cellSize: Double, modifier: Double,
                                      samplingFraction: Double, image: ImageFormat)
  extends FeatureStats[ImageFormat, SV] with ImageFeatureSampler {
  override def fieldIndex: Int = 0

  protected lazy val bi: BufferedImage = ImageUtils.toBufferedImage(
    image.data, image.width, image.height, image.nChannels
  )

  protected lazy val spd: SuperpixelData = SuperpixelData.fromSuperpixel(new Superpixel(bi, cellSize, modifier))

  protected lazy val numClusters: Int = spd.clusters.size
}

private trait ImageFeatureSampler extends Sampler[ImageFormat, SV] {
  self: ImageFeature =>
  override def sample(instance: ImageFormat)(implicit randBasis: RandBasis): (ImageFormat, SV, Double) = {

    val mask: BitVector = BDV.rand(self.numClusters, randBasis.uniform) <:= samplingFraction

    val maskAsDouble = BDV.zeros[Double](self.numClusters)
    axpy(1.0, mask, maskAsDouble) // equivalent to: maskAsDouble += 1.0 * mask

    val outputImage = Superpixel.maskImage(self.bi, self.spd, mask.toArray)

    val (path, height, width, nChannels, mode, decoded) = ImageUtils.toSparkImageTuple(outputImage)
    val imageFormat = ImageFormat(path, height, width, nChannels, mode, decoded)

    // Set distance to normalized Euclidean distance
    // 1 in the mask means keep the superpixel, 0 means replace with background color,
    // so a vector of all 1 means the original observation.
    val distance = norm(1.0 - maskAsDouble, 2) / math.sqrt(maskAsDouble.size)

    (imageFormat, maskAsDouble.toSpark, distance)
  }
}


private[explainers] class LIMEVectorSampler(featureStats: Seq[FeatureStats[Double, Double]])
  extends Sampler[SV, SV] {

  override def sample(instance: SV)(implicit randBasis: RandBasis): (SV, SV, Double) = {
    val (samples, features, distances) = featureStats.map {
      stats =>
        val value = instance(stats.fieldIndex)
        stats.sample(value)
    }.unzip3 match {
      case (samples, features, distances) =>
        (new SDV(samples.toArray), new SDV(features.toArray), BDV(distances: _*))
    }

    val n = featureStats.size

    // Set distance to normalized Euclidean distance
    (samples, features, norm(distances, 2) / math.sqrt(n))
  }
}

private[explainers] class LIMETabularSampler(featureStats: Seq[FeatureStats[Double, Double]])
  extends Sampler[Row, SV] {

  override def sample(instance: Row)(implicit randBasis: RandBasis): (Row, SV, Double) = {
    val (samples, features, distances) = featureStats.map {
      stats: FeatureStats[Double, Double] =>
        val value = instance.getAsDouble(stats.fieldIndex)
        val (sample, features, distance) = stats.sample(value)
        (sample, features, distance)
    }.unzip3

    val n = featureStats.size

    val featuresVector = new SDV(features.toArray)

    // Set distance to normalized Euclidean distance
    (Row.fromSeq(samples), featuresVector, norm(BDV(distances: _*), 2) / math.sqrt(n))
  }
}
