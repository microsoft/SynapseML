package com.microsoft.azure.synapse.ml.vw

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

case class BanditEstimatorCressieReadIntervalBuffer(wMin: Float,
                                            wMax: Float,
                                            n: KahanSum,
                                            sumw: KahanSum,
                                            sumwsq: KahanSum,
                                            sumwr: KahanSum,
                                            sumwrsqr: KahanSum,
                                            sumr: KahanSum)

case class BanditEstimatorCressieReadIntervalInput(probLog: Float,
                                           reward: Float,
                                           probPred: Float,
                                           count: Float,
                                           wMin: Float,
                                           wMax: Float)

case class BanditEstimatorCressieReadIntervalOutput(bounds: Array[Double])

class BanditEstimatorCressieReadInterval
  extends Aggregator[
    BanditEstimatorCressieReadIntervalInput,
    BanditEstimatorCressieReadIntervalBuffer,
    BanditEstimatorCressieReadIntervalOutput]
    with Serializable {
  // TODO: doesn't work
  //    with BasicLogging {
  //  logClass()

  //  override val uid: String = Identifiable.randomUID("BanditEstimatorIps")

  def zero: BanditEstimatorCressieReadIntervalBuffer =
    BanditEstimatorCressieReadIntervalBuffer(0, 0, 0, 0, 0, 0, 0, 0)

  def reduce(acc: BanditEstimatorCressieReadIntervalBuffer,
             x: BanditEstimatorCressieReadIntervalInput): BanditEstimatorCressieReadIntervalBuffer = {

    zero
//    val w = x.probPred / x.probLog
//    val countW = x.count * w
//    val countWsq = countW * countW
//
//    BanditEstimatorCressieReadBuffer(
//      Math.min(acc.wMin, x.wMin),
//      Math.max(acc.wMax, x.wMax),
//      acc.n + x.count,
//      acc.sumw + countW,
//      acc.sumwsq + countWsq,
//      acc.sumwr + countW * x.reward,
//      acc.sumwrsqr + countWsq * x.reward,
//      acc.sumr + x.count * x.reward
//    )
  }

  def merge(acc1: BanditEstimatorCressieReadIntervalBuffer,
            acc2: BanditEstimatorCressieReadIntervalBuffer): BanditEstimatorCressieReadIntervalBuffer = {

    zero
//    BanditEstimatorCressieReadBuffer(
//      Math.min(acc1.wMin, acc2.wMin),
//      Math.max(acc1.wMax, acc2.wMax),
//      acc1.n + acc2.n,
//      acc1.sumw + acc2.sumw,
//      acc1.sumwsq + acc2.sumwsq,
//      acc1.sumwr + acc2.sumwr,
//      acc1.sumwrsqr + acc2.sumwrsqr,
//      acc1.sumr + acc2.sumr)
  }

  def finish(acc: BanditEstimatorCressieReadIntervalBuffer): BanditEstimatorCressieReadIntervalOutput = {
    BanditEstimatorCressieReadIntervalOutput(Array(1f, 2f, 3f))
  }

  def bufferEncoder: Encoder[BanditEstimatorCressieReadIntervalBuffer] =
    Encoders.product[BanditEstimatorCressieReadIntervalBuffer]
  def outputEncoder: Encoder[BanditEstimatorCressieReadIntervalOutput] =
    Encoders.product[BanditEstimatorCressieReadIntervalOutput]
}