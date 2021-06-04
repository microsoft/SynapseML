package com.microsoft.ml.spark.explainers

import breeze.linalg.{norm, DenseVector => BDV}
import breeze.stats.distributions.RandBasis
import com.microsoft.ml.spark.explainers.BreezeUtils._
import com.microsoft.ml.spark.explainers.RowUtils.RowCanGetAsDouble
import com.microsoft.ml.spark.lime.{Superpixel, SuperpixelData}
import org.apache.spark.ml.linalg.{Vector => SV, Vectors => SVS}
import org.apache.spark.sql.Row

import java.awt.image.BufferedImage

private[explainers] trait Sampler[TObservation, TState] extends Serializable {
  def instance: TObservation

  /**
    * Generates a sample based on the specified instance, together with the perturbed state, and
    * distance metric between the generated sample and the original instance.
    */
  def sample: (TObservation, TState, Double) = {
    val state = nextState
    val newSample = createNewSample(instance, state)
    val distance = getDistance(state)
    (newSample, state, distance)
  }

  def nextState: TState

  def createNewSample(instance: TObservation, state: TState): TObservation

  def getDistance(state: TState): Double
}

private[explainers] case class ImageFormat(origin: Option[String],
                                height: Int,
                                width: Int,
                                nChannels: Int,
                                mode: Int,
                                data: Array[Byte])

private[explainers] trait ImageSampler extends Sampler[BufferedImage, SV] {
  def spd: SuperpixelData

  override def createNewSample(instance: BufferedImage, state: SV): BufferedImage = {
    val mask = state.toArray.map(_ == 1.0)
    val outputImage = Superpixel.maskImage(instance, this.spd, mask)
    outputImage
  }
}

private[explainers] class LIMEImageSampler(val instance: BufferedImage,
                                           val samplingFraction: Double,
                                           val spd: SuperpixelData)
                                          (implicit val randBasis: RandBasis)
  extends ImageSampler
    with LIMESamplerSupport {
  override def featureSize: Int = spd.clusters.size
}

private[explainers] class KernelSHAPImageSampler(val instance: BufferedImage,
                                                 val spd: SuperpixelData,
                                                 val numSamples: Int)
  extends ImageSampler
    with KernelSHAPSamplerSupport {
  override protected def featureSize: Int = spd.clusters.size
}

private[explainers] trait TextSampler extends Sampler[Seq[String], SV] {
  def createNewSample(instance: Seq[String], state: SV): Seq[String] = {
    val mask = state.toArray.map(_ == 1.0)
    (instance, mask).zipped.collect {
      case (token, state) if state => token
    }.toSeq
  }
}

private[explainers] class LIMETextSampler(val instance: Seq[String], val samplingFraction: Double)
                                         (implicit val randBasis: RandBasis)
  extends TextSampler
    with LIMESamplerSupport {
  override def featureSize: Int = instance.size
}

private[explainers] class KernelSHAPTextSampler(val instance: Seq[String],
                                                val numSamples: Int)
  extends TextSampler
    with KernelSHAPSamplerSupport {
  override protected def featureSize: Int = instance.size
}

private[explainers] class LIMEVectorSampler(val instance: SV, val featureStats: Seq[FeatureStats[Double]])
                                           (implicit val randBasis: RandBasis)
  extends Sampler[SV, SV] {

  override def nextState: SV = {
    val states = featureStats.zipWithIndex.map {
      case (feature, i) =>
        val value = instance(i)
        feature.sample(value)
    }

    SVS.dense(states.toArray)
  }

  override def createNewSample(instance: SV, state: SV): SV = {
    state
  }

  override def getDistance(state: SV): Double = {
    // Set distance to normalized Euclidean distance

    val n = featureStats.size

    val distances = featureStats.zipWithIndex.map {
      case (feature, i) =>
        feature.getDistance(instance(i), state(i))
    }

    norm(BDV(distances: _*), 2) / math.sqrt(n)
  }
}

private[explainers] class LIMETabularSampler(val instance: Row, val featureStats: Seq[FeatureStats[Double]])
                                            (implicit val randBasis: RandBasis)
  extends Sampler[Row, SV] {

  override def sample: (Row, SV, Double) = {
    val (newSample, states, distance) = super.sample

    val originalValues = (0 until instance.size).map(instance.getAsDouble)

    // For categorical features, set state to 1 if it matches with original instance, otherwise set to 0.
    val newStates = (featureStats, states.toArray, originalValues).zipped map {
      case (_: DiscreteFeatureStats, state, orig) => if (state == orig) 1d else 0d
      case (_, state, _) => state
    }

    (newSample, SVS.dense(newStates.toArray), distance)
  }

  override def nextState: SV = {
    val states = featureStats.zipWithIndex.map {
      case (feature, i) =>
        val value = instance.getAsDouble(i)
        feature.sample(value)
    }

    SVS.dense(states.toArray)
  }

  override def createNewSample(instance: Row, state: SV): Row = {
    Row.fromSeq(state.toArray)
  }

  override def getDistance(state: SV): Double = {
    // Set distance to normalized Euclidean distance

    val n = featureStats.size

    val distances = featureStats.zipWithIndex.map {
      case (feature, i) =>
        val value = instance.getAsDouble(i)
        val s = state(i)
        feature.getDistance(value, s)
    }

    norm(BDV(distances: _*), 2) / math.sqrt(n)
  }
}

private[explainers] class KernelSHAPTabularSampler(val instance: Row,
                                                   val background: Row,
                                                   val numSamples: Int)
  extends Sampler[Row, SV] with KernelSHAPSamplerSupport {

  override protected def featureSize: Int = background.size

  override def createNewSample(instance: Row, state: SV): Row = {
    // Merge instance with background based on coalition
    val newRow = Row.fromSeq(
      (0 until featureSize).map {
        i =>
          val row = if (state(i) == 1.0) instance else background
          row.get(i)
      }
    )

    newRow
  }
}

private[explainers] class KernelSHAPVectorSampler(val instance: SV,
                                                  val background: SV,
                                                  val numSamples: Int)
  extends Sampler[SV, SV]
    with KernelSHAPSamplerSupport {

  override protected def featureSize: Int = background.size

  override def createNewSample(instance: SV, state: SV): SV = {
    val mask = state.toBreeze
    val result = (mask *:* instance.toBreeze) + ((1.0 - mask) *:* background.toBreeze)
    result.toSpark
  }
}