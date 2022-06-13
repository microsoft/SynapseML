// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers

import breeze.linalg.{BitVector, axpy, norm, DenseVector => BDV}
import breeze.stats.distributions.RandBasis
import com.microsoft.azure.synapse.ml.core.utils.BreezeUtils._
import org.apache.spark.ml.linalg.Vector

private[explainers] trait LIMESampler[TObservation] extends Sampler[TObservation, Vector] {
  def sample: (TObservation, Vector, Double) = {
    val state = nextState
    val newSample = createNewSample(instance, state)
    val distance = getDistance(state)
    (newSample, state, distance)
  }

  def randBasis: RandBasis

  protected def nextState: Vector

  protected def getDistance(state: Vector): Double = {
    // Set distance to normalized Euclidean distance
    // 1 in the mask means keep the superpixel, 0 means replace with background color,
    // so a vector of all 1 means the original observation.
    norm(1.0 - state.toBreeze, 2) / math.sqrt(state.size)
  }
}

private[explainers] trait LIMEOnOffSampler[TObservation] extends LIMESampler[TObservation] {
  def featureSize: Int
  def samplingFraction: Double
  private lazy val randomStateGenerator: Iterator[Vector] = new Iterator[Vector] {
    override def hasNext: Boolean = true

    override def next(): Vector = {
      val mask: BitVector = BDV.rand(featureSize, randBasis.uniform) <:= samplingFraction
      val maskAsDouble = BDV.zeros[Double](featureSize)
      axpy(1.0, mask, maskAsDouble) // equivalent to: maskAsDouble += 1.0 * mask
      maskAsDouble.toSpark
    }
  }

  protected def nextState: Vector = this.randomStateGenerator.next
}
