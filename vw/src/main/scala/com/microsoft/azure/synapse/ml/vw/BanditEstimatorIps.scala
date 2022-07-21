// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.logging.BasicLogging
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

class BanditEstimatorIps
  extends Aggregator[BanditEstimatorIpsInput, BanditEstimatorIpsBuffer, Float]
    with Serializable {
  // TODO: doesn't work
  //    with BasicLogging {
  //  logClass()

//  override val uid: String = Identifiable.randomUID("BanditEstimatorIps")

  def zero: BanditEstimatorIpsBuffer = BanditEstimatorIpsBuffer(0, 0)

  def reduce(acc: BanditEstimatorIpsBuffer, x: BanditEstimatorIpsInput): BanditEstimatorIpsBuffer = {
    val w = x.probPred / x.probLog

    BanditEstimatorIpsBuffer(
      acc.exampleCount + x.count,
      acc.weightedReward + x.reward * w * x.count)
  }

  def merge(acc1: BanditEstimatorIpsBuffer, acc2: BanditEstimatorIpsBuffer): BanditEstimatorIpsBuffer = {
    BanditEstimatorIpsBuffer(
      acc1.exampleCount + acc2.exampleCount,
      acc1.weightedReward + acc2.weightedReward)
  }

  def finish(acc: BanditEstimatorIpsBuffer): Float =
    if (acc.exampleCount == 0)
      -1 // TODO: how to return null?
    else
      acc.weightedReward / acc.exampleCount

  def bufferEncoder: Encoder[BanditEstimatorIpsBuffer] = Encoders.product[BanditEstimatorIpsBuffer]
  def outputEncoder: Encoder[Float] = Encoders.scalaFloat
}

final case class BanditEstimatorIpsInput(probLog: Float, reward: Float, probPred: Float, count: Float)

final case class BanditEstimatorIpsBuffer(exampleCount: Float, weightedReward: Float)
