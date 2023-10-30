// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.policyeval

import breeze.stats.distributions.FDistribution
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.policyeval
import com.microsoft.azure.synapse.ml.vw.KahanSum
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

/**
  * Cressie-Read with intervals off-policy evaluation metric.
  *
  * Background http://www.machinedlearnings.com/2020/12/distributionally-robust-contextual.html
  */
class CressieReadInterval(empiricalBounds: Boolean)
  extends Aggregator[CressieReadIntervalInput,
                     CressieReadIntervalBuffer,
                     BanditEstimator]
    with Serializable
    with SynapseMLLogging {
  override val uid: String = Identifiable.randomUID("BanditEstimatorCressieReadInterval")

  logClass(FeatureNames.VowpalWabbit)

  val alpha = 0.05
  val atol = 1e-9

  def zero: CressieReadIntervalBuffer = policyeval.CressieReadIntervalBuffer()

  def reduce(acc: CressieReadIntervalBuffer, x: CressieReadIntervalInput): CressieReadIntervalBuffer = {
    val w = x.probPred / x.probLog
    val countW = x.count * w
    val countWsq = countW * countW
    val countWsqr = countWsq * x.reward

    val (rMax, rMin) = if (empiricalBounds)
      (Math.max(acc.rewardMax, x.reward), Math.min(acc.rewardMin, x.reward))
    else {
      if (x.reward > x.rewardMax || x.reward < x.rewardMin)
        throw new IllegalArgumentException(s"Reward is out of bounds: ${x.rewardMin} < ${x.reward} < ${x.rewardMax}")
      (x.rewardMax, x.rewardMin)
    }

    CressieReadIntervalBuffer(
      wMin = Math.min(acc.wMin, x.wMin),
      wMax = Math.max(acc.wMax, x.wMax),
      rewardMin = rMin,
      rewardMax = rMax,
      n = acc.n + x.count,
      sumw = acc.sumw + countW,
      sumwsq = acc.sumwsq + countWsq,
      sumwr = acc.sumwr + countW * x.reward,
      sumwsqr = acc.sumwsqr + countWsqr,
      sumwsqrsq = acc.sumwsqrsq + countWsqr * x.reward)
  }

  def merge(acc1: CressieReadIntervalBuffer,
            acc2: CressieReadIntervalBuffer): CressieReadIntervalBuffer = {
    CressieReadIntervalBuffer(
      wMin = Math.min(acc1.wMin, acc2.wMin),
      wMax = Math.max(acc1.wMax, acc2.wMax),
      rewardMin = Math.min(acc1.rewardMin, acc2.rewardMin),
      rewardMax = Math.max(acc1.rewardMax, acc2.rewardMax),
      n = acc1.n + acc2.n,
      sumw = acc1.sumw + acc2.sumw,
      sumwsq = acc1.sumwsq + acc2.sumwsq,
      sumwr = acc1.sumwr + acc2.sumwr,
      sumwsqr = acc1.sumwsqr + acc2.sumwsqr,
      sumwsqrsq = acc1.sumwsqrsq + acc2.sumwsqrsq)
  }

  private def computeBoundInfinity(n: Double,
                                   sign: Double,
                                   r: Double,
                                   sumw: Double,
                                   sumwsq: Double,
                                   sumwr: Double,
                                   sumwsqr: Double,
                                   sumwsqrsq: Double,
                                   phi: Double): Option[Double] = {
    val x = sign * (r + (sumwr - sumw * r) / n)
    val rSumwSumwr = r * sumw - sumwr
    var y = (rSumwSumwr * rSumwSumwr / (n * (1 + n))
      - (r * r * sumwsq - 2 * r * sumwsqr + sumwsqrsq) / (1 + n)
      )
    val z = phi + 1 / (2 * n)
    // if isclose(y * z, 0, abs_tol=1e-9):
    if (Math.abs(y * z) < atol)
      y = 0

    // if isclose(y * z, 0, abs_tol=atol * atol):
    if (Math.abs(y * z) < atol * atol)
      Some(x - Math.sqrt(2) * atol)
    else if (z <= 0 && y * z >= 0)
      Some(x - Math.sqrt(2 * y * z))

    None
  }

  private def computeBoundInner(wfake: Double,
                                n: Double,
                                sign: Double,
                                r: Double,
                                sumw: Double,
                                sumwsq: Double,
                                sumwr: Double,
                                sumwsqr: Double,
                                sumwsqrsq: Double,
                                phi: Double): Option[Double] = {
    if (wfake.isInfinity)
      computeBoundInfinity(n, sign, r, sumw, sumwsq, sumwr, sumwsqr, sumwsqrsq, phi)
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
          Some(x - Math.sqrt(2 * y * z))
        else
          None
      }
    }
  }

  def finish(acc: CressieReadIntervalBuffer): BanditEstimator = {
    logVerb("aggregate", {
    val n = acc.n.toDouble

    if (n == 0)
        BanditEstimator(-1, -1)
      else {
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

        val fDistribution = new FDistribution(numeratorDegreesOfFreedom = 1, denominatorDegreesOfFreedom = n)

        //      delta = f.isf(q=alpha, dfn=1, dfd=n)
        val delta = 1 - fDistribution.inverseCdf(alpha)

        val phi = (-uncgstar - delta) / (2 * (1 + n))

        def computeBound(r: Double, sign: Int) = {
          val lower = computeBoundInner(wfake = acc.wMin, n, sign, r, sumw, sumwsq, sumwr, sumwsqr, sumwsqrsq, phi)
          val upper = computeBoundInner(wfake = acc.wMax, n, sign, r, sumw, sumwsq, sumwr, sumwsqr, sumwsqrsq, phi)

          val best = Seq(lower, upper).flatten.min

          Math.min(acc.rewardMax, Math.max(acc.rewardMin, sign * best))
        }

        BanditEstimator(
          computeBound(acc.rewardMin, 1),
          computeBound(acc.rewardMax, -1))
      }
    })
  }

  def bufferEncoder: Encoder[CressieReadIntervalBuffer] =
    Encoders.product[CressieReadIntervalBuffer]
  def outputEncoder: Encoder[BanditEstimator] =
    Encoders.product[BanditEstimator]
}

final case class CressieReadIntervalBuffer(wMin: Float = 0,
                                           wMax: Float = 0,
                                           rewardMin: Float = 0,
                                           rewardMax: Float = 0,
                                           n: KahanSum = 0,
                                           sumw: KahanSum = 0,
                                           sumwsq: KahanSum = 0,
                                           sumwr: KahanSum = 0,
                                           sumwsqr: KahanSum = 0,
                                           sumwsqrsq: KahanSum = 0)

// w is the ratio of x.probPred / x.probLog, we would like to know the min/max of that ratio
// from the user
final case class CressieReadIntervalInput(probLog: Float,
                                          reward: Float,
                                          probPred: Float,
                                          count: Float,
                                          wMin: Float,
                                          wMax: Float,
                                          rewardMin: Float,
                                          rewardMax: Float)

case class BanditEstimator(lower: Double, upper: Double)
