package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.logging.BasicLogging
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

case class BanditEstimatorIpsBuffer(exampleCount: Float, weightedReward: Float) {
  def add(probLog: Float, reward: Float, probPred: Float, count: Float): BanditEstimatorIpsBuffer = {
    val w = probPred / probLog

    BanditEstimatorIpsBuffer(
      exampleCount + count,
      weightedReward + reward * w * count)
  }

  def add(other: BanditEstimatorIpsBuffer): BanditEstimatorIpsBuffer = {
    BanditEstimatorIpsBuffer(
      exampleCount + other.exampleCount,
      weightedReward + other.weightedReward)
  }

  def get(): Float =
    if (exampleCount == 0)
      -1 // TODO: how to return null?
    else
      weightedReward / exampleCount
}

case class BanditEstimatorIpsInput(probLog: Float, reward: Float, probPred: Float, count: Float)

class BanditEstimatorIps
  extends Aggregator[BanditEstimatorIpsInput, BanditEstimatorIpsBuffer, Float]
    with Serializable {
  // TODO: doesn't work
  //    with BasicLogging {
  //  logClass()

//  override val uid: String = Identifiable.randomUID("BanditEstimatorIps")

  def zero: BanditEstimatorIpsBuffer = BanditEstimatorIpsBuffer(0, 0)

  def reduce(acc: BanditEstimatorIpsBuffer, x: BanditEstimatorIpsInput): BanditEstimatorIpsBuffer =
    acc.add(x.probLog, x.reward, x.probPred, x.count)

  def merge(acc1: BanditEstimatorIpsBuffer, acc2: BanditEstimatorIpsBuffer): BanditEstimatorIpsBuffer =
    acc1.add(acc2)

  def finish(acc: BanditEstimatorIpsBuffer): Float = acc.get

  def bufferEncoder: Encoder[BanditEstimatorIpsBuffer] = Encoders.product[BanditEstimatorIpsBuffer]
  def outputEncoder: Encoder[Float] = Encoders.scalaFloat
}