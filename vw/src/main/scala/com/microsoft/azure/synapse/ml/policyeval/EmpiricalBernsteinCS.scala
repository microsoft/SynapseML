// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.policyeval

import com.microsoft.azure.synapse.ml.logging.BasicLogging
import com.microsoft.azure.synapse.ml.policyeval
import com.microsoft.azure.synapse.ml.vw.KahanSum
import org.apache.commons.math3.analysis.UnivariateFunction
import org.apache.commons.math3.analysis.solvers.BrentSolver
import org.apache.commons.math3.special.Gamma
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

class EmpiricalBernsteinCS(rho: Double = 1, alpha: Double = 0.05)
  extends Aggregator[EmpiricalBernsteinCSInput,
    EmpiricalBernsteinCSBuffer,
    EmpiricalBernsteinCSOutput]
    with Serializable
    with BasicLogging {
  override val uid: String = Identifiable.randomUID("BanditEstimatorEmpiricalBernsteinCS")
  logClass()

  val solverMaxIterations = 100

  if (rho <= 0)
    throw new IllegalArgumentException(s"rho ($rho) must be > 0")

  def zero: EmpiricalBernsteinCSBuffer = policyeval.EmpiricalBernsteinCSBuffer()

  def reduce(acc: EmpiricalBernsteinCSBuffer,
             x: EmpiricalBernsteinCSInput): EmpiricalBernsteinCSBuffer = {
    val rmin = Math.min(x.rewardMin, acc.rewardMin)
    val rmax = Math.max(x.rewardMax, acc.rewardMax)

    val w = x.probPred / x.probLog

    assert(w >= 0)

    val sumXlow = (acc.sumwr.toDouble - acc.sumw.toDouble * rmin) / (rmax - rmin)
    val xhatlow = (sumXlow + 1 / 2) / (acc.t + 1)
    val sumXhigh = (acc.sumw.toDouble * rmax - acc.sumwr.toDouble) / (rmax - rmin)
    val xhathigh = (sumXhigh + 1 / 2) / (acc.t + 1)

    EmpiricalBernsteinCSBuffer(
      sumwsqrsq = acc.sumwsqrsq + (w * x.reward) * (w * x.reward),
      sumwsqr = acc.sumwsqr + w * w * x.reward,
      sumwsq = acc.sumwsq + w * w,
      sumwr = acc.sumwr + w * x.reward,
      sumw = acc.sumw + w,
      sumwrxhatlow = acc.sumwrxhatlow + w * x.reward * xhatlow,
      sumwxhatlow = acc.sumwxhatlow + w * xhatlow,
      sumxhatlowsq = acc.sumxhatlowsq + xhatlow * xhatlow,
      sumwrxhathigh = acc.sumwrxhathigh + w * x.reward * xhathigh,
      sumwxhathigh = acc.sumwxhathigh + w * xhathigh,
      sumxhathighsq = acc.sumxhathighsq + xhathigh * xhathigh,
      rewardMin = rmin,
      rewardMax = rmax,
      t = acc.t + 1)
  }

  def merge(acc1: EmpiricalBernsteinCSBuffer,
            acc2: EmpiricalBernsteinCSBuffer): EmpiricalBernsteinCSBuffer = {
    EmpiricalBernsteinCSBuffer(
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
      rewardMin = Math.min(acc1.rewardMin, acc2.rewardMin),
      rewardMax = Math.max(acc1.rewardMax, acc2.rewardMax),
      t = acc1.t + acc2.t
    )
  }

  // log(sc.gammainc(a, x)) + sc.loggamma(a)
  private def loggammalowerinc(a: Double, x: Double) = Math.log(Gamma.regularizedGammaP(a, x)) + Gamma.logGamma(a)

  private val rhoLogRho = rho * Math.log(rho)
  private val loggammalowerincrhorho = loggammalowerinc(rho, rho)

  private def logwealth(s: Double, v: Double): Double = {
    assert(s + v + rho > 0, s"$s + $v + $rho > 0")

    (s + v
      + rhoLogRho
      + (v + rho) * Math.log(s + v + rho)
      + loggammalowerinc(v + rho, s + v + rho)
      - loggammalowerincrhorho)
  }

  private def lblogwealth(t: Long, sumXt: Double, v: Double, alpha: Double) = {
    assert(0 < alpha && alpha < 1)

    val thres = -Math.log(alpha)
    val logwealthminmu = logwealth(sumXt, v)

    val minmu = 0
    if (logwealthminmu <= thres)
      minmu
    else {
      val maxmu = Math.min(1, sumXt / t)

      val logwealthmaxmu = logwealth(sumXt - t * maxmu, v)

      if (logwealthmaxmu >= thres)
        maxmu
      else {
        val optimizer = new BrentSolver()
        class Func extends UnivariateFunction {
          override def value(mu: Double): Double = logwealth(sumXt - t * mu, v) - thres
        }

        optimizer.solve(solverMaxIterations, new Func(), minmu, maxmu)
      }
    }
  }

  def finish(acc: EmpiricalBernsteinCSBuffer): EmpiricalBernsteinCSOutput = {
    logVerb("aggregate", {
      if (acc.t == 0 || acc.rewardMin == acc.rewardMax)
        EmpiricalBernsteinCSOutput(acc.rewardMin, acc.rewardMax)
      else {
        val rrange = acc.rewardMax - acc.rewardMin
        val sumvlow = (
          acc.sumwsqrsq.toDouble -
            2 * acc.rewardMin * acc.sumwsqr.toDouble +
            (acc.rewardMin * acc.rewardMin * acc.sumwsq.toDouble) / (rrange * rrange)
            - 2 * (acc.sumwrxhatlow.toDouble - acc.rewardMin * acc.sumwxhatlow.toDouble) / rrange
            + acc.sumxhatlowsq.toDouble)

        val sumXlow = (acc.sumwr.toDouble - acc.sumw.toDouble * acc.rewardMin) / rrange
        val l = lblogwealth(t = acc.t, sumXt = sumXlow, v = sumvlow, alpha = alpha / 2)

        val sumvhigh =
          acc.sumwsqrsq.toDouble -
            2 * acc.rewardMax * acc.sumwsqr.toDouble +
            (acc.rewardMax * acc.rewardMax * acc.sumwsq.toDouble) / (rrange * rrange) +
            2 * (acc.sumwrxhathigh.toDouble - acc.rewardMax * acc.sumwxhathigh.toDouble) / rrange +
            acc.sumxhathighsq.toDouble

        val sumXhigh = (acc.sumw.toDouble * acc.rewardMax - acc.sumwr.toDouble) / rrange
        val u = 1 - lblogwealth(t = acc.t, sumXt = sumXhigh, v = sumvhigh, alpha = alpha / 2)

        EmpiricalBernsteinCSOutput(
          acc.rewardMin + l * rrange,
          acc.rewardMin + u * rrange)
      }
    })
  }

  def bufferEncoder: Encoder[EmpiricalBernsteinCSBuffer] =
    Encoders.product[EmpiricalBernsteinCSBuffer]

  def outputEncoder: Encoder[EmpiricalBernsteinCSOutput] =
    Encoders.product[EmpiricalBernsteinCSOutput]
}

final case class EmpiricalBernsteinCSBuffer(sumwsqrsq: KahanSum = 0,
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
                                            rewardMin: Double = 0,
                                            rewardMax: Double = 0,
                                            t: Long = 0)

final case class EmpiricalBernsteinCSInput(probLog: Float,
                                           reward: Float,
                                           probPred: Float,
                                           count: Float,
                                           rewardMin: Float,
                                           rewardMax: Float)

case class EmpiricalBernsteinCSOutput(lower: Double, upper: Double)
