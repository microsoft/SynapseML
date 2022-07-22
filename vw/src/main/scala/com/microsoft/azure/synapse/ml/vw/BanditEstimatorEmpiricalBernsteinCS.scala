// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.logging.BasicLogging
import org.apache.commons.math3.analysis.UnivariateFunction
import org.apache.commons.math3.analysis.solvers.BrentSolver
import org.apache.commons.math3.special.Gamma
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

class BanditEstimatorEmpiricalBernsteinCS(rho: Double = 1, alpha: Double =0.05)
  extends Aggregator[BanditEstimatorEmpiricalBernsteinCSInput,
                     BanditEstimatorEmpiricalBernsteinCSBuffer,
                     BanditEstimatorEmpiricalBernsteinCSOutput]
    with Serializable {
//    with BasicLogging {
//  logClass()
//
//  override val uid: String = Identifiable.randomUID("BanditEstimatorEmpiricalBernsteinCS")

  if (rho <= 0)
    throw new IllegalArgumentException(s"rho ($rho) must be > 0")

  //  override val uid: String = Identifiable.randomUID("BanditEstimatorEmpiricalBernsteinCS")

  def zero: BanditEstimatorEmpiricalBernsteinCSBuffer = BanditEstimatorEmpiricalBernsteinCSBuffer()

  def reduce(acc: BanditEstimatorEmpiricalBernsteinCSBuffer,
             x: BanditEstimatorEmpiricalBernsteinCSInput): BanditEstimatorEmpiricalBernsteinCSBuffer = {
    val rmin = Math.min(x.rmin, acc.rmin)
    val rmax = Math.max(x.rmax, acc.rmax)

    val w = x.probPred / x.probLog

    assert (w >= 0)

    //    val xhatlow = (acc.sumXlow + 1/2) / (acc.t + 1)
//    val xhathigh = (acc.sumXhigh + 1/2) / (acc.t + 1)
//
//    val sumvlow = (w * x.reward - Math.min(1, xhatlow))
//    val sumvhigh = (w * (1 - x.reward) - Math.min(1, xhathigh))

    val sumXlow = (acc.sumwr.toDouble - acc.sumw.toDouble * rmin) / (rmax - rmin)
    val Xhatlow = (sumXlow + 1/2) / (acc.t + 1)
    val sumXhigh = (acc.sumw.toDouble * rmax - acc.sumwr.toDouble) / (rmax - rmin)
    val Xhathigh = (sumXhigh + 1/2) / (acc.t + 1)

    BanditEstimatorEmpiricalBernsteinCSBuffer(
      sumwsqrsq = acc.sumwsqrsq + (w * x.reward)*(w * x.reward),
      sumwsqr = acc.sumwsqr + w * w * x.reward,
      sumwsq = acc.sumwsq + w * w,
      sumwr = acc.sumwr + w * x.reward,
      sumw = acc.sumw + w,
      sumwrxhatlow = acc.sumwrxhatlow + w * x.reward * Xhatlow,
      sumwxhatlow = acc.sumwxhatlow + w * Xhatlow,
      sumxhatlowsq = acc.sumxhatlowsq + Xhatlow * Xhatlow,
      sumwrxhathigh = acc.sumwrxhathigh + w * x.reward * Xhathigh,
      sumwxhathigh = acc.sumwxhathigh + w * Xhathigh,
      sumxhathighsq = acc.sumxhathighsq + Xhathigh * Xhathigh,
      rmin = rmin,
      rmax = rmax,
      t = acc.t + 1)
  }

  def merge(acc1: BanditEstimatorEmpiricalBernsteinCSBuffer,
            acc2: BanditEstimatorEmpiricalBernsteinCSBuffer): BanditEstimatorEmpiricalBernsteinCSBuffer = {
    BanditEstimatorEmpiricalBernsteinCSBuffer(
      sumwsqrsq = acc1.sumwsqrsq + acc2.sumwsqrsq,
      sumwsqr = acc1.sumwsqr + acc2.sumwsqr,
      sumwsq = acc1.sumwsq + acc2.sumwsq,
      sumwr = acc1.sumwr + acc2.sumwr,
      sumw = acc1.sumw + acc2.sumw,
      sumwrxhatlow = acc1.sumwrxhatlow + acc2.sumwrxhatlow,
      sumwxhatlow = acc1.sumwxhatlow + acc2.sumwxhatlow,
      sumxhatlowsq = acc1.sumxhatlowsq + acc2.sumxhatlowsq,
      sumwrxhathigh = acc1.sumwrxhathigh + acc2.sumwrxhathigh,
      sumwxhathigh = acc1.sumwxhathigh + acc2.sumwxhathigh,
      sumxhathighsq = acc1.sumxhathighsq + acc2.sumxhathighsq,
      rmin = Math.min(acc1.rmin, acc2.rmin),
      rmax = Math.max(acc1.rmax, acc2.rmax),
      t = acc1.t + acc2.t
    )
  }

  // log(sc.gammainc(a, x)) + sc.loggamma(a)
  private def loggammalowerinc(a: Double, x: Double) = Math.log(Gamma.regularizedGammaP(a, x)) + Gamma.logGamma(a)

  val rhoLogRho = rho * Math.log(rho)
  val loggammalowerincrhorho = loggammalowerinc(rho, rho)

  def finish(acc: BanditEstimatorEmpiricalBernsteinCSBuffer): BanditEstimatorEmpiricalBernsteinCSOutput = {
    def logwealth(s: Double, v: Double): Double = {
      assert (s + v + rho > 0, s"$s + $v + $rho > 0")

      (s + v
        + rhoLogRho
        + (v + rho) * Math.log(s + v + rho)
        + loggammalowerinc(v + rho, s + v + rho)
        - loggammalowerincrhorho)
    }

    def lblogwealth(t: Long, sumXt: Double, v: Double, alpha: Double) = {
      assert (0 < alpha && alpha < 1)

      val thres = -Math.log(alpha)
      val logwealthminmu = logwealth(sumXt, v)

      val minmu = 0
      if (logwealthminmu <= thres)
        minmu
      else {
        val maxmu = Math.min(1, sumXt / t)

        val logwealthmaxmu = logwealth(sumXt - t * maxmu, v)

//        assert (logwealthmaxmu <= thres, s"$logwealthmaxmu < $thres")
        if (logwealthmaxmu >= thres)
          maxmu
        else {
          val optimizer = new BrentSolver()
          class Func extends UnivariateFunction {
            override def value(mu: Double): Double = logwealth(sumXt - t * mu, v) - thres
          }

          optimizer.solve(100, new Func(), minmu, maxmu)
        }
      }
    }
    
    if (acc.t == 0 || acc.rmin == acc.rmax)
      BanditEstimatorEmpiricalBernsteinCSOutput(acc.rmin, acc.rmax)
    else {
      val rrange = acc.rmax - acc.rmin
      val sumvlow = (
        acc.sumwsqrsq.toDouble -
          2 * acc.rmin * acc.sumwsqr.toDouble +
          (acc.rmin*acc.rmin * acc.sumwsq.toDouble) / (rrange * rrange)
          - 2 * (acc.sumwrxhatlow.toDouble - acc.rmin * acc.sumwxhatlow.toDouble) / rrange
          + acc.sumxhatlowsq.toDouble)

      val sumXlow = (acc.sumwr.toDouble - acc.sumw.toDouble * acc.rmin) / rrange
      val l = lblogwealth(t=acc.t, sumXt=sumXlow, v=sumvlow, alpha=alpha/2)

      val sumvhigh = (
        acc.sumwsqrsq.toDouble -
          2 * acc.rmax * acc.sumwsqr.toDouble +
          (acc.rmax * acc.rmax * acc.sumwsq.toDouble) / (rrange * rrange) +
          2 * (acc.sumwrxhathigh.toDouble - acc.rmax * acc.sumwxhathigh.toDouble) / rrange +
          acc.sumxhathighsq.toDouble)

      val sumXhigh = (acc.sumw.toDouble * acc.rmax - acc.sumwr.toDouble) / rrange
      val u = 1 - lblogwealth(t=acc.t, sumXt=sumXhigh, v=sumvhigh, alpha=alpha/2)

      BanditEstimatorEmpiricalBernsteinCSOutput(
        acc.rmin + l * rrange,
        acc.rmin + u * rrange)
  //    logVerb("aggregate", {
//        BanditEstimatorEmpiricalBernsteinCSOutput(
//          lblogwealth(acc.t, acc.sumXlow, acc.sumvlow, alpha / 2),
//          1 - lblogwealth(acc.t, acc.sumXhigh, acc.sumvhigh, alpha / 2))
  //    })
    }

  }

  def bufferEncoder: Encoder[BanditEstimatorEmpiricalBernsteinCSBuffer] =
    Encoders.product[BanditEstimatorEmpiricalBernsteinCSBuffer]
  def outputEncoder: Encoder[BanditEstimatorEmpiricalBernsteinCSOutput] =
    Encoders.product[BanditEstimatorEmpiricalBernsteinCSOutput]
}

final case class BanditEstimatorEmpiricalBernsteinCSBuffer(sumwsqrsq: KahanSum = 0,
                                                           sumwsqr: KahanSum = 0,
                                                           sumwsq: KahanSum = 0,
                                                           sumwr: KahanSum = 0,
                                                           sumw: KahanSum = 0,
                                                           sumwrxhatlow: KahanSum = 0,
                                                           sumwxhatlow: KahanSum = 0,
                                                           sumxhatlowsq: KahanSum = 0,
                                                           sumwrxhathigh: KahanSum = 0,
                                                           sumwxhathigh: KahanSum = 0,
                                                           sumxhathighsq: KahanSum = 0,
                                                           rmin: Double = 0,
                                                           rmax: Double = 0,
                                                           t: Long = 0)

final case class BanditEstimatorEmpiricalBernsteinCSInput(probLog: Float,
                                                          reward: Float,
                                                          probPred: Float,
                                                          count: Float,
                                                          rmin: Float,
                                                          rmax: Float)

case class BanditEstimatorEmpiricalBernsteinCSOutput(lower: Double, upper: Double)
