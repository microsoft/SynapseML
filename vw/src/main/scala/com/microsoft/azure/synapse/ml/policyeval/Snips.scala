// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.policyeval

import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

/**
  * SNIPS off policy estimator: the Self-Normalized Estimator for Counterfactual Learning
  *
  * See https://papers.nips.cc/paper/2015/file/39027dfad5138c9ca0c474d71db915c3-Paper.pdf
  */
class Snips
  extends Aggregator[SnipsInput, SnipsBuffer, Float]
    with Serializable
    with SynapseMLLogging {
  override val uid: String = Identifiable.randomUID("BanditEstimatorSnips")

  logClass(FeatureNames.VowpalWabbit)

  def zero: SnipsBuffer = SnipsBuffer(0, 0)

  def reduce(acc: SnipsBuffer, x: SnipsInput): SnipsBuffer = {
    val w = x.probabilityPredicted / x.probabilityLogged

    SnipsBuffer(
      acc.weightedExampleCount + w * x.count,
      acc.weightedReward + x.reward * w * x.count)
  }

  def merge(acc1: SnipsBuffer, acc2: SnipsBuffer): SnipsBuffer =
    SnipsBuffer(
      acc1.weightedExampleCount + acc2.weightedExampleCount,
      acc1.weightedReward + acc2.weightedReward)

  def finish(acc: SnipsBuffer): Float = {
    logVerb("aggregate", {
      if (acc.weightedExampleCount == 0)
        -1f // TODO: how to return null?
      else
        (acc.weightedReward / acc.weightedExampleCount).toFloat
    })
  }

  def bufferEncoder: Encoder[SnipsBuffer] = Encoders.product[SnipsBuffer]
  def outputEncoder: Encoder[Float] = Encoders.scalaFloat
}

final case class SnipsBuffer(weightedExampleCount: Double, weightedReward: Double)

final case class SnipsInput(probabilityLogged: Float,
                            reward: Float,
                            probabilityPredicted: Float,
                            count: Float)
