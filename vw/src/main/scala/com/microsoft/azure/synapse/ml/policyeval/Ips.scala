// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.policyeval

import com.microsoft.azure.synapse.ml.logging.BasicLogging
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

// TODO: prob doc
// inverse propensity score
// https://towardsdatascience.com/contextual-bandits-and-reinforcement-learning-6bdfeaece72a
class Ips
  extends Aggregator[IpsInput, IpsBuffer, Float]
    with Serializable
    with BasicLogging {
  override val uid: String = Identifiable.randomUID("BanditEstimatorIps")

  logClass()

  def zero: IpsBuffer = IpsBuffer(0, 0)

  def reduce(acc: IpsBuffer, x: IpsInput): IpsBuffer = {
    val w = x.probPred / x.probLog

    IpsBuffer(
      acc.exampleCount + x.count,
      acc.weightedReward + x.reward * w * x.count)
  }

  def merge(acc1: IpsBuffer, acc2: IpsBuffer): IpsBuffer = {
    IpsBuffer(
      acc1.exampleCount + acc2.exampleCount,
      acc1.weightedReward + acc2.weightedReward)
  }

  def finish(acc: IpsBuffer): Float =
    logVerb("aggregate", {
      if (acc.exampleCount == 0)
        -1 // TODO: how to return null?
      else
        acc.weightedReward / acc.exampleCount
    })

  def bufferEncoder: Encoder[IpsBuffer] = Encoders.product[IpsBuffer]
  def outputEncoder: Encoder[Float] = Encoders.scalaFloat
}

final case class IpsInput(probLog: Float, reward: Float, probPred: Float, count: Float)

final case class IpsBuffer(exampleCount: Float, weightedReward: Float)
