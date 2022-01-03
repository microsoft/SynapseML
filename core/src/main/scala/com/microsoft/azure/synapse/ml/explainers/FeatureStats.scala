// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers

import breeze.numerics.abs
import breeze.stats.distributions.{RandBasis, Uniform}

private[explainers] trait FeatureStats[T] {

  def getRandomState(instance: T)(implicit randBasis: RandBasis): Double

  def sample(state: Double): T

  def getDistance(instance: T, sample: T): Double
}

private[explainers] final case class ContinuousFeatureStats(stddev: Double)
  extends FeatureStats[Double] {
  override def getRandomState(instance: Double)(implicit randBasis: RandBasis): Double = {
    randBasis.gaussian(instance, this.stddev).sample
  }

  override def sample(state: Double): Double = {
    state
  }

  override def getDistance(instance: Double, sample: Double): Double = {
    if (this.stddev == 0d) {
      0d
    } else {
      // Normalize by stddev
      abs(sample - instance) / this.stddev
    }
  }
}

private[explainers] final case class DiscreteFeatureStats[V](freq: Map[V, Double])
  extends FeatureStats[V] {

  /**
    * Returns the cumulative density function (CDF) of the given frequency table.
    */
  private def cdf[T](freq: Seq[(T, Double)]): Seq[(T, Double)] = {
    freq.map(_._1) zip freq.map(_._2).scanLeft(0d)(_ + _).drop(1)
  }

  private lazy val cdfTable: Seq[(V, Double)] = {
    val freq = this.freq.toSeq
    cdf(freq)
  }

  override def getRandomState(instance: V)(implicit randBasis: RandBasis): Double = {
    Uniform(0d, freq.values.sum).sample
  }

  override def sample(state: Double): V = {
    cdfTable.find(state <= _._2).get._1
  }

  override def getDistance(instance: V, sample: V): Double = {
    if (instance == sample) 0d else 1d
  }
}
