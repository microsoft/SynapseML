package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.logging.BasicLogging
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

case class BanditEstimatorSnipsBuffer(weightedExampleCount: Float, weightedReward: Float) {
  def add(probLog: Float, reward: Float, probPred: Float, count: Float): BanditEstimatorSnipsBuffer = {
    val w = probPred / probLog

    BanditEstimatorSnipsBuffer(
      weightedExampleCount + w * count,
      weightedReward + reward * w * count)
  }

  def add(other: BanditEstimatorSnipsBuffer): BanditEstimatorSnipsBuffer = {
    BanditEstimatorSnipsBuffer(
      weightedExampleCount + other.weightedExampleCount,
      weightedReward + other.weightedReward)
  }

  def get(): Float =
    if (weightedExampleCount == 0)
      -1 // TODO: how to return null?
    else
      weightedReward / weightedExampleCount
}

case class BanditEstimatorSnipsInput(probLog: Float, reward: Float, probPred: Float, count: Float)

class BanditEstimatorSnips
  extends Aggregator[BanditEstimatorSnipsInput, BanditEstimatorSnipsBuffer, Float]
    with Serializable {
  // TODO: doesn't work
//    with BasicLogging {
//  logClass()

//  override val uid: String = Identifiable.randomUID("BanditEstimatorSnips")

  def zero: BanditEstimatorSnipsBuffer = BanditEstimatorSnipsBuffer(0, 0)

  def reduce(acc: BanditEstimatorSnipsBuffer, x: BanditEstimatorSnipsInput): BanditEstimatorSnipsBuffer =
    acc.add(x.probLog, x.reward, x.probPred, x.count)

  def merge(acc1: BanditEstimatorSnipsBuffer, acc2: BanditEstimatorSnipsBuffer): BanditEstimatorSnipsBuffer =
    acc1.add(acc2)

  // multiple outputs: https://stackoverflow.com/questions/63206872/custom-spark-aggregator-returning-row
  def finish(acc: BanditEstimatorSnipsBuffer): Float = acc.get

  def bufferEncoder: Encoder[BanditEstimatorSnipsBuffer] = Encoders.product[BanditEstimatorSnipsBuffer]
  def outputEncoder: Encoder[Float] = Encoders.scalaFloat
}