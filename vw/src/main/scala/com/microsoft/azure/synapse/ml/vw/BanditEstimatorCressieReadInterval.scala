// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import breeze.stats.distributions.FDistribution
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

class BanditEstimatorCressieReadInterval(empiricalBounds: Boolean)
  extends Aggregator[BanditEstimatorCressieReadIntervalInput,
                     BanditEstimatorCressieReadIntervalBuffer,
                     BanditEstimatorCressieReadIntervalOutput]
    with Serializable {
  // TODO: doesn't work
  //    with BasicLogging {
  //  logClass()

  //  override val uid: String = Identifiable.randomUID("BanditEstimatorIps")

  def zero: BanditEstimatorCressieReadIntervalBuffer =
    BanditEstimatorCressieReadIntervalBuffer()

  def reduce(acc: BanditEstimatorCressieReadIntervalBuffer,
             x: BanditEstimatorCressieReadIntervalInput): BanditEstimatorCressieReadIntervalBuffer = {

    val w = x.probPred / x.probLog
    val countW = x.count * w
    val countWsq = countW * countW
    val countWsqr = countWsq * x.reward

    val (rMax, rMin) = if (empiricalBounds)
      (Math.max(acc.rMax, x.reward), Math.min(acc.rMin, x.reward))
    else {
      if (x.reward > x.rMax || x.reward < x.rMin)
        throw new IllegalArgumentException(s"Reward is out of bounds: ${x.rMin} < ${x.reward} < ${x.rMax}")
      (0f, 0f)
    }

    BanditEstimatorCressieReadIntervalBuffer(
      wMin = Math.min(acc.wMin, x.wMin),
      wMax = Math.max(acc.wMax, x.wMax),
      rMin = rMin,
      rMax = rMax,
      n = acc.n + x.count,
      sumw = acc.sumw + countW,
      sumwsq = acc.sumwsq + countWsq,
      sumwr = acc.sumwr + countW * x.reward,
      sumwsqr = acc.sumwsqr + countWsqr,
      sumwsqrsq = acc.sumwsqrsq + countWsqr * x.reward)
  }

  def merge(acc1: BanditEstimatorCressieReadIntervalBuffer,
            acc2: BanditEstimatorCressieReadIntervalBuffer): BanditEstimatorCressieReadIntervalBuffer = {

    BanditEstimatorCressieReadIntervalBuffer(
      wMin = Math.min(acc1.wMin, acc2.wMin),
      wMax = Math.max(acc1.wMax, acc2.wMax),
      rMin = Math.min(acc1.rMin, acc2.rMin),
      rMax = Math.max(acc1.rMax, acc2.rMax),
      n = acc1.n + acc2.n,
      sumw = acc1.sumw + acc2.sumw,
      sumwsq = acc1.sumwsq + acc2.sumwsq,
      sumwr = acc1.sumwr + acc2.sumwr,
      sumwsqr = acc1.sumwsqr + acc2.sumwsqr,
      sumwsqrsq = acc1.sumwsqrsq + acc2.sumwsqrsq)
  }

  def finish(acc: BanditEstimatorCressieReadIntervalBuffer): BanditEstimatorCressieReadIntervalOutput = {
    if (acc.n == 0)
      BanditEstimatorCressieReadIntervalOutput(-1, -1)
    else {
      val n = acc.n.toDouble
      val sumw = acc.sumw.toDouble
      val sumwsq = acc.sumwsq.toDouble
      val sumwr = acc.sumwr.toDouble
      val sumwsqr = acc.sumwsqr.toDouble
      val sumwsqrsq = acc.sumwsqrsq.toDouble

      val uncwfake = if (sumw < n) acc.wMax else acc.wMin

      val uncgstar =
        if (uncwfake.isInfinity)
           1 + 1 / n
        else {
          val unca = (uncwfake + sumw) / (1 + n)
          val uncb = (uncwfake * uncwfake + sumwsq) / (1 + n)
          (1 + n) * (unca - 1) * (unca - 1) / (uncb - unca * unca)
        }

      val Fdist = new FDistribution(numeratorDegreesOfFreedom = 1, denominatorDegreesOfFreedom = n)
      val alpha = 0.05 // TODO: parameter?
      val atol = 1e-9

      //      delta = f.isf(q=alpha, dfn=1, dfd=n)
      val delta = 1 - Fdist.inverseCdf(alpha)

      val phi = (-uncgstar - delta) / (2 * (1 + n))

      def computeBound(r: Double, sign: Int) = {
        def computeBoundInner(wfake: Double): Option[Double] = {
          if (wfake.isInfinity) {
            val x = sign * (r + (sumwr - sumw * r) / n)
            val rsumw_sumwr = (r * sumw - sumwr)
            var y = (rsumw_sumwr * rsumw_sumwr / (n * (1 + n))
              - (r *r  * sumwsq - 2 * r * sumwsqr + sumwsqrsq) / (1 + n)
              )
            val z = phi + 1 / (2 * n)
            // if isclose(y * z, 0, abs_tol=1e-9):
            if (Math.abs(y*z) < atol)
              y = 0

            // if isclose(y * z, 0, abs_tol=atol * atol):
            if (Math.abs(y * z) < atol * atol)
              Some(x - Math.sqrt(2) * atol)
            else if (z <= 0 && y * z >= 0)
              Some(x - Math.sqrt(2 * y * z))

            None
          }
          else {
            val barw = (wfake + sumw) / (1 + n)
            val barwsq = (wfake * wfake + sumwsq) / (1 + n)
            val barwr = sign * (wfake * r + sumwr) / (1 + n)
            val barwsqr = sign * (wfake * wfake * r + sumwsqr) / (1 + n)
            val barwsqrsq = (wfake * wfake * r * r + sumwsqrsq) / (1 + n)

            if (barwsq <= barw * barw) None
            else {
              val x = barwr + ((1 - barw) * (barwsqr - barw * barwr) / (barwsq - barw * barw))
              val y = (barwsqr - barw * barwr) * (barwsqr - barw * barwr) /
                (barwsq - barw * barw) - (barwsqrsq - barwr * barwr)
              val z = phi + (1 / 2) * (1 - barw) * (1 - barw) / (barwsq - barw * barw)

              // if isclose(y * z, 0, abs_tol = atol * atol):
              if (Math.abs(y * x) < atol * atol)
                Some(x - Math.sqrt(2) * atol)
              else if (z <= 0 && y * z >= 0)
                Some(x - Math.sqrt(2 * y *z))
              else
                None
            }
          }
        }

        val best = Seq(computeBoundInner(acc.wMin), computeBoundInner(acc.wMax)).flatten.min

        Math.min(acc.rMax, Math.max(acc.rMin, sign * best))
      }

      BanditEstimatorCressieReadIntervalOutput(
        computeBound(acc.rMin, 1),
        computeBound(acc.rMax, -1))
    }
  }

  def bufferEncoder: Encoder[BanditEstimatorCressieReadIntervalBuffer] =
    Encoders.product[BanditEstimatorCressieReadIntervalBuffer]
  def outputEncoder: Encoder[BanditEstimatorCressieReadIntervalOutput] =
    Encoders.product[BanditEstimatorCressieReadIntervalOutput]
}

final case class BanditEstimatorCressieReadIntervalBuffer(wMin: Float = 0,
                                                          wMax: Float = 0,
                                                          rMin: Float = 0,
                                                          rMax: Float = 0,
                                                          n: KahanSum = 0,
                                                          sumw: KahanSum = 0,
                                                          sumwsq: KahanSum = 0,
                                                          sumwr: KahanSum = 0,
                                                          sumwsqr: KahanSum = 0,
                                                          sumwsqrsq: KahanSum = 0)

final case class BanditEstimatorCressieReadIntervalInput(probLog: Float,
                                                         reward: Float,
                                                         probPred: Float,
                                                         count: Float,
                                                         wMin: Float,
                                                         wMax: Float,
                                                         rMin: Float,
                                                         rMax: Float)

case class BanditEstimatorCressieReadIntervalOutput(lower: Double, upper: Double)
