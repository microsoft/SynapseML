// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.logging.BasicLogging
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

class BanditEstimatorSnips
  extends Aggregator[BanditEstimatorSnipsInput, BanditEstimatorSnipsBuffer, Float]
    with Serializable {
  // TODO: doesn't work
//    with BasicLogging {
//  logClass()

//  override val uid: String = Identifiable.randomUID("BanditEstimatorSnips")

  def zero: BanditEstimatorSnipsBuffer = BanditEstimatorSnipsBuffer(0, 0)

  def reduce(acc: BanditEstimatorSnipsBuffer, x: BanditEstimatorSnipsInput): BanditEstimatorSnipsBuffer = {
    val w = x.probabilityPredicted / x.probabilityLogged

    BanditEstimatorSnipsBuffer(
      acc.weightedExampleCount + w * x.count,
      acc.weightedReward + x.reward * w * x.count)
  }

  def merge(acc1: BanditEstimatorSnipsBuffer, acc2: BanditEstimatorSnipsBuffer): BanditEstimatorSnipsBuffer =
    BanditEstimatorSnipsBuffer(
      acc1.weightedExampleCount + acc2.weightedExampleCount,
      acc1.weightedReward + acc2.weightedReward)

  def finish(acc: BanditEstimatorSnipsBuffer): Float = {
    if (acc.weightedExampleCount == 0)
      -1f // TODO: how to return null?
    else
      (acc.weightedReward / acc.weightedExampleCount).toFloat
  }

  def bufferEncoder: Encoder[BanditEstimatorSnipsBuffer] = Encoders.product[BanditEstimatorSnipsBuffer]
  def outputEncoder: Encoder[Float] = Encoders.scalaFloat
}

final case class BanditEstimatorSnipsBuffer(weightedExampleCount: Double, weightedReward: Double)

final case class BanditEstimatorSnipsInput(probabilityLogged: Float,
                                           reward: Float,
                                           probabilityPredicted: Float,
                                           count: Float)
