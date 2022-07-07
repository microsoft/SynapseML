package com.microsoft.azure.synapse.ml.vw

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

case class BanditEstimatorCressieReadBuffer(wMin: Float,
                                            wMax: Float,
                                            n: KahanSum,
                                            sumw: KahanSum,
                                            sumwsq: KahanSum,
                                            sumwr: KahanSum,
                                            sumwrsqr: KahanSum,
                                            sumr: KahanSum)

case class BanditEstimatorCressieReadInput(probLog: Float,
                                           reward: Float,
                                           probPred: Float,
                                           count: Float,
                                           wMin: Float,
                                           wMax: Float)

// ported from
// https://github.com/VowpalWabbit/estimators/blob/03c8ba619d68f54849d4fa2da2b1a148e6cdb990/estimators/bandits/cressieread.py#L25
class BanditEstimatorCressieRead
  extends Aggregator[BanditEstimatorCressieReadInput, BanditEstimatorCressieReadBuffer, Double]
  with Serializable {
  // TODO: doesn't work
  //    with BasicLogging {
  //  logClass()

  //  override val uid: String = Identifiable.randomUID("BanditEstimatorIps")

  def zero: BanditEstimatorCressieReadBuffer =
    BanditEstimatorCressieReadBuffer(0, 0, 0, 0, 0, 0, 0, 0)

  def reduce(acc: BanditEstimatorCressieReadBuffer,
             x: BanditEstimatorCressieReadInput): BanditEstimatorCressieReadBuffer = {

    val w = x.probPred / x.probLog
    val countW = x.count * w
    val countWsq = countW * countW

    BanditEstimatorCressieReadBuffer(
      Math.min(acc.wMin, x.wMin),
      Math.max(acc.wMax, x.wMax),
      acc.n + x.count,
      acc.sumw + countW,
      acc.sumwsq + countWsq,
      acc.sumwr + countW * x.reward,
      acc.sumwrsqr + countWsq * x.reward,
      acc.sumr + x.count * x.reward
    )
  }

  def merge(acc1: BanditEstimatorCressieReadBuffer,
            acc2: BanditEstimatorCressieReadBuffer): BanditEstimatorCressieReadBuffer = {

    BanditEstimatorCressieReadBuffer(
      Math.min(acc1.wMin, acc2.wMin),
      Math.max(acc1.wMax, acc2.wMax),
      acc1.n + acc2.n,
      acc1.sumw + acc2.sumw,
      acc1.sumwsq + acc2.sumwsq,
      acc1.sumwr + acc2.sumwr,
      acc1.sumwrsqr + acc2.sumwrsqr,
      acc1.sumr + acc2.sumr)
  }

  def finish(acc: BanditEstimatorCressieReadBuffer): Double = {
    val n = acc.n.toDouble

    val sumw = acc.sumw.toDouble
    val sumwsq = acc.sumwsq.toDouble
    val sumwr = acc.sumwr.toDouble
    val sumwsqr = acc.sumwrsqr.toDouble
    val sumr = acc.sumr.toDouble

    val wfake = if (sumw < n) acc.wMax else acc.wMin

    val (gamma, beta) = {
      if (wfake.isInfinity) (-(1 + n) / n, 0.0)
      else {
        val a = (wfake + sumw) / (1 + n)
        val b = (wfake * wfake + sumwsq) / (1 + n)

        assert(a * a < b)

        ((b - a) / (a * a - b), (1 - a) / (a * a - b))
      }
    }

    val vhat = (-gamma * sumwr - beta * sumwsqr) / (1 + n)
    val missing = Math.max(0, 1 - (-gamma * sumw - beta * sumwsq) / (1 + n))
    val rhatmissing = sumr / n

    vhat + missing * rhatmissing
  }

  def bufferEncoder: Encoder[BanditEstimatorCressieReadBuffer] = Encoders.product[BanditEstimatorCressieReadBuffer]
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}