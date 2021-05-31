package com.microsoft.ml.spark.explainers

import breeze.linalg.{BitVector, axpy, norm, DenseVector => BDV}
import breeze.numerics.abs
import breeze.stats.distributions.{RandBasis, Uniform}
import com.microsoft.ml.spark.explainers.BreezeUtils._
import com.microsoft.ml.spark.explainers.RowUtils.RowCanGetAsDouble
import com.microsoft.ml.spark.lime.{Superpixel, SuperpixelData}
import org.apache.spark.ml.linalg.{Vector => SV, Vectors => SVS}
import org.apache.spark.sql.Row

import java.awt.image.BufferedImage

private[explainers] trait RandomSampler[TObservation, TState, TDistance] extends Serializable {
  /**
    * Generates a sample based on the specified instance, together with the perturbed state, and
    * distance metric between the generated sample and the original instance.
    */
  def sample(instance: TObservation)(implicit randBasis: RandBasis): (TObservation, TState, TDistance) = {
    val state = nextState(instance)
    val newSample = createNewSample(instance, state)
    val distance = getDistance(instance, state)
    (newSample, state, distance)
  }

  def nextState(instance: TObservation)(implicit randBasis: RandBasis): TState

  def createNewSample(instance: TObservation, state: TState): TObservation

  def getDistance(instance: TObservation, state: TState): TDistance
}

private[explainers] trait FeatureStats[TObservation, TState, TDistance]
  extends RandomSampler[TObservation, TState, TDistance] {
  def fieldIndex: Int
}

private final case class ContinuousFeatureStats(override val fieldIndex: Int, stddev: Double)
  extends FeatureStats[Double, Double, Double] with ContinuousFeatureSampler

private trait ContinuousFeatureSampler extends RandomSampler[Double, Double, Double] {
  self: ContinuousFeatureStats =>

  override def nextState(instance: Double)(implicit randBasis: RandBasis): Double = {
    randBasis.gaussian(instance, self.stddev).sample
  }

  override def createNewSample(instance: Double, state: Double): Double = {
    state
  }

  override def getDistance(instance: Double, state: Double): Double = {
    if (self.stddev == 0d) {
      0d
    } else {
      // Normalize by stddev
      abs(state - instance) / self.stddev
    }
  }
}

private final case class DiscreteFeatureStats(override val fieldIndex: Int, freq: Map[Double, Double])
  extends FeatureStats[Double, Double, Double] with DiscreteFeatureSampler

private trait DiscreteFeatureSampler extends RandomSampler[Double, Double, Double] {
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

  override def createNewSample(instance: Double, state: Double): Double = {
    state
  }

  override def nextState(instance: Double)(implicit randBasis: RandBasis): Double = {
    val r = Uniform(0d, freq.values.sum).sample
    cdfTable.find(r <= _._2).get._1
  }

  override def getDistance(instance: Double, state: Double): Double = {
    if (instance == state) 0d else 1d
  }
}

private case class ImageFormat(origin: Option[String],
                                height: Int,
                                width: Int,
                                nChannels: Int,
                                mode: Int,
                                data: Array[Byte])

private final case class ImageFeature(samplingFraction: Double, spd: SuperpixelData)
  extends FeatureStats[BufferedImage, SV, Double] with ImageFeatureSampler {
  override def fieldIndex: Int = 0
  protected lazy val numClusters: Int = spd.clusters.size
}

private trait ImageFeatureSampler extends RandomSampler[BufferedImage, SV, Double] {
  self: ImageFeature =>

  override def nextState(instance: BufferedImage)(implicit randBasis: RandBasis): SV = {
    val mask: BitVector = BDV.rand(self.numClusters, randBasis.uniform) <:= self.samplingFraction
    val maskAsDouble = BDV.zeros[Double](self.numClusters)
    axpy(1.0, mask, maskAsDouble) // equivalent to: maskAsDouble += 1.0 * mask
    maskAsDouble.toSpark
  }

  override def createNewSample(instance: BufferedImage, state: SV): BufferedImage = {
    val mask = state.toArray.map(_ == 1.0)
    val outputImage = Superpixel.maskImage(instance, self.spd, mask)
    outputImage
  }

  override def getDistance(instance: BufferedImage, state: SV): Double = {
    // Set distance to normalized Euclidean distance
    // 1 in the mask means keep the superpixel, 0 means replace with background color,
    // so a vector of all 1 means the original observation.
    norm(1.0 - state.toBreeze, 2) / math.sqrt(state.size)
  }
}

private final case class TextFeature(samplingFraction: Double, seed: Int = 0)
  extends FeatureStats[Seq[String], SV, Double] with TextFeatureSampler {
  override def fieldIndex: Int = 0
}

private trait TextFeatureSampler extends RandomSampler[Seq[String], SV, Double] {
  self: TextFeature =>

  override def nextState(instance: Seq[String])(implicit randBasis: RandBasis): SV = {
    val mask: BitVector = BDV.rand(instance.size, randBasis.uniform) <:= self.samplingFraction
    val maskAsDouble = BDV.zeros[Double](instance.size)
    axpy(1.0, mask, maskAsDouble) // equivalent to: maskAsDouble += 1.0 * mask
    maskAsDouble.toSpark
  }

  override def createNewSample(instance: Seq[String], state: SV): Seq[String] = {
    val mask = state.toArray.map(_ == 1.0)
    (instance, mask).zipped.collect {
      case (token, state) if state => token
    }.toSeq
  }

  override def getDistance(instance: Seq[String], state: SV): Double = {
    // Set distance to normalized Euclidean distance
    // 1 in the mask means keep the token, 0 means remove the token,
    // so a vector of all 1 means the original observation.
    norm(1.0 - state.toBreeze, 2) / math.sqrt(state.size)
  }
}

private[explainers] class LIMEVectorSampler(featureStats: Seq[FeatureStats[Double, Double, Double]])
  extends RandomSampler[SV, SV, Double] {

  override def nextState(instance: SV)(implicit randBasis: RandBasis): SV = {
    val states = featureStats.map {
      feature =>
        val value = instance(feature.fieldIndex)
        feature.nextState(value)
    }

    SVS.dense(states.toArray)
  }

  override def createNewSample(instance: SV, state: SV): SV = {
    state
  }

  override def getDistance(instance: SV, state: SV): Double = {
    // Set distance to normalized Euclidean distance

    val n = featureStats.size

    val distances = featureStats.map {
      feature =>
        feature.getDistance(instance(feature.fieldIndex), state(feature.fieldIndex))
    }

    norm(BDV(distances: _*), 2) / math.sqrt(n)
  }
}

private[explainers] class LIMETabularSampler(featureStats: Seq[FeatureStats[Double, Double, Double]])
  extends RandomSampler[Row, SV, Double] {

  override def nextState(instance: Row)(implicit randBasis: RandBasis): SV = {
    val states = featureStats.map {
      feature =>
        val value = instance.getAsDouble(feature.fieldIndex)
        feature.nextState(value)
    }

    SVS.dense(states.toArray)
  }

  override def createNewSample(instance: Row, state: SV): Row = {
    Row.fromSeq(state.toArray)
  }

  override def getDistance(instance: Row, state: SV): Double = {
    // Set distance to normalized Euclidean distance

    val n = featureStats.size

    val distances = featureStats.map {
      feature =>
        val value = instance.getAsDouble(feature.fieldIndex)
        val s = state(feature.fieldIndex)
        feature.getDistance(value, s)
    }

    norm(BDV(distances: _*), 2) / math.sqrt(n)
  }
}

private[explainers] class KernelSHAPTabularSampler(features: Seq[Int], background: Row, numSamples: Int)
  extends RandomSampler[Row, SV, Unit] with KernelSHAPSupport {

  // TODO: Kernel weight function for KernelSHAP samplers

  private lazy val coalitionGenerator: Iterator[BDV[Int]] = {
    this.generateCoalitions(features.size, numSamples)
  }

  override def nextState(instance: Row)(implicit randBasis: RandBasis): SV = {
    coalitionGenerator.next.mapValues(_.toDouble).toSpark
  }

  override def createNewSample(instance: Row, state: SV): Row = {
    // Merge instance with background based on coalition
    val newRow = Row.fromSeq(
      features.map {
        i =>
          val row = if (state(i) == 1.0) instance else background
          row.get(i)
      }
    )

    newRow
  }

  override def getDistance(instance: Row, state: SV): Unit = ()
}