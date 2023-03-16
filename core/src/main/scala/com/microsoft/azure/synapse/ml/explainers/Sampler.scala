// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers

import breeze.linalg.{norm, DenseVector => BDV}
import breeze.stats.distributions.RandBasis
import com.microsoft.azure.synapse.ml.core.utils.BreezeUtils._
import com.microsoft.azure.synapse.ml.explainers.RowUtils.RowCanGetAsDouble
import com.microsoft.azure.synapse.ml.image.{Superpixel, SuperpixelData}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.Row

import java.awt.image.BufferedImage

private[explainers] trait Sampler[TObservation, TState] extends Serializable {
  def instance: TObservation

  /**
    * Generates a sample based on the specified instance, together with the perturbed state, and
    * distance metric between the generated sample and the original instance.
    */
  def sample: (TObservation, TState, Double)

  protected def createNewSample(instance: TObservation, state: TState): TObservation
}

private[explainers] case class ImageFormat(origin: Option[String],
                                height: Int,
                                width: Int,
                                nChannels: Int,
                                mode: Int,
                                data: Array[Byte])

private[explainers] trait ImageSampler extends Sampler[BufferedImage, Vector] {
  def spd: SuperpixelData

  override def createNewSample(instance: BufferedImage, state: Vector): BufferedImage = {
    val mask = state.toArray.map(_ == 1.0)
    val outputImage = Superpixel.maskImage(instance, this.spd, mask)
    outputImage
  }
}

private[explainers] class LIMEImageSampler(val instance: BufferedImage,
                                           val samplingFraction: Double,
                                           val spd: SuperpixelData)
                                          (implicit val randBasis: RandBasis)
  extends LIMEOnOffSampler[BufferedImage]
    with ImageSampler {
  override def featureSize: Int = spd.clusters.size
}

private[explainers] class KernelSHAPImageSampler(val instance: BufferedImage,
                                                 val spd: SuperpixelData,
                                                 val numSamples: Int,
                                                 val infWeight: Double)
  extends KernelSHAPSampler[BufferedImage]
    with KernelSHAPSamplerSupport
    with ImageSampler {
  override protected def featureSize: Int = spd.clusters.size
}

private[explainers] trait TextSampler extends Sampler[Seq[String], Vector] {
  def createNewSample(instance: Seq[String], state: Vector): Seq[String] = {
    val mask = state.toArray.map(_ == 1.0)
    (instance, mask).zipped.collect {
      case (token, state) if state => token
    }.toSeq
  }
}

private[explainers] class LIMETextSampler(val instance: Seq[String], val samplingFraction: Double)
                                         (implicit val randBasis: RandBasis)
  extends LIMEOnOffSampler[Seq[String]]
    with TextSampler {
  override def featureSize: Int = instance.size
}

private[explainers] class KernelSHAPTextSampler(val instance: Seq[String],
                                                val numSamples: Int,
                                                val infWeight: Double)
  extends KernelSHAPSampler[Seq[String]]
    with KernelSHAPSamplerSupport
    with TextSampler {
  override protected def featureSize: Int = instance.size
}

private[explainers] class LIMEVectorSampler(val instance: Vector, val featureStats: Seq[ContinuousFeatureStats])
                                           (implicit val randBasis: RandBasis)
  extends LIMESampler[Vector] {

  override def nextState: Vector = {
    val states = featureStats.zipWithIndex.map {
      case (feature, i) =>
        feature.getRandomState(instance(i))
    }

    Vectors.dense(states.toArray)
  }

  override def createNewSample(instance: Vector, state: Vector): Vector = {
    val values: Seq[Double] = featureStats.zip(state.toArray).map {
      case (feature, s) =>
        feature.sample(s)
    }

    Vectors.dense(values.toArray)
  }

  override def getDistance(state: Vector): Double = {
    // Set distance to normalized Euclidean distance

    val n = featureStats.size

    val distances = featureStats.zipWithIndex.map {
      case (feature, i) =>
        val value = instance(i)
        val sample = feature.sample(state(i))
        feature.getDistance(value, sample)
    }

    norm(BDV(distances: _*), 2) / math.sqrt(n)
  }
}

private[explainers] class LIMETabularSampler(val instance: Row, val featureStats: Seq[FeatureStats[_]])
                                            (implicit val randBasis: RandBasis)
  extends LIMESampler[Row] {

  override def sample: (Row, Vector, Double) = {
    val (newSample, states, distance) = super.sample

    val originalValues = (0 until instance.size).map(instance.get)

    // For categorical features, set state to 1 if it matches with original instance, otherwise set to 0.
    val newStates = (featureStats, states.toArray, originalValues).zipped map {
      case (f: DiscreteFeatureStats[_], state, orig) => if (f.sample(state) == orig) 1d else 0d
      case (_, state, _) => state
    }

    (newSample, Vectors.dense(newStates.toArray), distance)
  }

  /**
   * Create a sample that's identical to the instance, with states set to 1 for categorical vars
   * and original value for numerical vars. Distance is set to 0.
   */
  def sampleIdentity: (Row, Vector, Double) = {
    val (identityRow, identityState) = featureStats.zipWithIndex.map {
      case (_: DiscreteFeatureStats[Any], i) =>
        (instance.get(i), 1d)
      case (_: ContinuousFeatureStats, i) =>
        (instance.getAsDouble(i), instance.getAsDouble(i))
      case (_, _) =>
        throw new NotImplementedError("invalid state")
    }.unzip

    (Row.fromSeq(identityRow), Vectors.dense(identityState.toArray), 0d)
  }

  override def nextState: Vector = {
    val states = featureStats.zipWithIndex.map {
      case (feature: DiscreteFeatureStats[Any], i) =>
        feature.getRandomState(instance.get(i))
      case (feature, i) =>
        feature.asInstanceOf[ContinuousFeatureStats]
          .getRandomState(instance.getAsDouble(i))
    }

    Vectors.dense(states.toArray)
  }

  override def createNewSample(instance: Row, state: Vector): Row = {
    val values = featureStats.zip(state.toArray).map {
      case (feature, s) =>
        feature.sample(s)
    }

    Row.fromSeq(values)
  }

  override def getDistance(state: Vector): Double = {
    // Set distance to normalized Euclidean distance
    val n = featureStats.size

    val distances = featureStats.zipWithIndex.map {
      case (feature: DiscreteFeatureStats[Any], i) =>
        val value = instance.get(i)
        val sample = feature.sample(state(i))
        feature.getDistance(value, sample)
      case (feature, i) =>
        val cf = feature.asInstanceOf[ContinuousFeatureStats]
        val value = instance.getAsDouble(i)
        cf.getDistance(value, cf.sample(state(i)))
    }

    norm(BDV(distances: _*), 2) / math.sqrt(n)
  }
}

private[explainers] class KernelSHAPTabularSampler(val instance: Row,
                                                   val background: Row,
                                                   val numSamples: Int,
                                                   val infWeight: Double)
  extends KernelSHAPSampler[Row] with KernelSHAPSamplerSupport {

  override protected def featureSize: Int = background.size

  override def createNewSample(instance: Row, state: Vector): Row = {
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

private[explainers] class KernelSHAPVectorSampler(val instance: Vector,
                                                  val background: Vector,
                                                  val numSamples: Int,
                                                  val infWeight: Double)
  extends KernelSHAPSampler[Vector] with KernelSHAPSamplerSupport {

  override protected def featureSize: Int = background.size

  override def createNewSample(instance: Vector, state: Vector): Vector = {
    val mask = state.toBreeze
    val result = (mask *:* instance.toBreeze) + ((1.0 - mask) *:* background.toBreeze)
    result.toSpark
  }
}
