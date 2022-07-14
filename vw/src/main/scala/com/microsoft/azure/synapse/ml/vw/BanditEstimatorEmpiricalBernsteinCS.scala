package com.microsoft.azure.synapse.ml.vw

import org.apache.commons.math3.analysis.UnivariateFunction
import org.apache.commons.math3.analysis.solvers.BrentSolver
import org.apache.commons.math3.special.Gamma
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

case class BanditEstimatorEmpiricalBernsteinCSBuffer(sumXlow: Double = 0,
                                                     sumXhigh: Double = 0,
                                                     sumvlow: Double = 0,
                                                     sumvhigh: Double = 0,
                                                     t: Long = 0)

case class BanditEstimatorEmpiricalBernsteinCSInput(probLog: Float, reward: Float, probPred: Float, count: Float)

case class BanditEstimatorEmpiricalBernsteinCSOutput(lower: Double, upper: Double)

class BanditEstimatorEmpiricalBernsteinCS(rho: Double = 1, alpha: Double =0.05)
  extends Aggregator[
    BanditEstimatorEmpiricalBernsteinCSInput,
    BanditEstimatorEmpiricalBernsteinCSBuffer,
    BanditEstimatorEmpiricalBernsteinCSOutput]
    with Serializable {
  // TODO: doesn't work
  //    with BasicLogging {
  //  logClass()

  if (rho <= 0)
    throw new IllegalArgumentException(s"rho ($rho) must be > 0")

  //  override val uid: String = Identifiable.randomUID("BanditEstimatorEmpiricalBernsteinCS")

  def zero: BanditEstimatorEmpiricalBernsteinCSBuffer = BanditEstimatorEmpiricalBernsteinCSBuffer()

  def reduce(acc: BanditEstimatorEmpiricalBernsteinCSBuffer,
             x: BanditEstimatorEmpiricalBernsteinCSInput): BanditEstimatorEmpiricalBernsteinCSBuffer = {
    val w = x.probPred / x.probLog
    val xhatlow = (acc.sumXlow + 1/2) / (acc.t + 1)
    val xhathigh = (acc.sumXhigh + 1/2) / (acc.t + 1)

    val sumvlow = (w * x.reward - Math.min(1, xhatlow))
    val sumvhigh = (w * (1 - x.reward) - Math.min(1, xhathigh))

    BanditEstimatorEmpiricalBernsteinCSBuffer(
      sumXlow = acc.sumXlow + w * x.reward,
      sumXhigh = acc.sumXhigh + w * (1 - x.reward),
      sumvlow = acc.sumvlow + sumvlow * sumvlow,
      sumvhigh = acc.sumvhigh + sumvhigh * sumvhigh,
      t = acc.t + 1)
  }

  def merge(acc1: BanditEstimatorEmpiricalBernsteinCSBuffer, acc2: BanditEstimatorEmpiricalBernsteinCSBuffer): BanditEstimatorEmpiricalBernsteinCSBuffer = {
    BanditEstimatorEmpiricalBernsteinCSBuffer(
      sumXlow = acc1.sumXlow + acc2.sumXlow,
      sumXhigh = acc1.sumXhigh + acc2.sumXhigh,
      sumvlow = acc1.sumvlow + acc2.sumvlow,
      sumvhigh = acc1.sumvhigh + acc2.sumvhigh,
      t = acc1.t + acc2.t)
  }

  // log(sc.gammainc(a, x)) + sc.loggamma(a)
  private def loggammalowerinc(a: Double, x: Double) = Math.log(Gamma.regularizedGammaP(a, x)) + Gamma.logGamma(a)

  val rhoLogRho = rho * Math.log(rho)
  val loggammalowerincrhorho = loggammalowerinc(rho, rho)

  def finish(acc: BanditEstimatorEmpiricalBernsteinCSBuffer): BanditEstimatorEmpiricalBernsteinCSOutput = {
    def logwealth(s: Double, v: Double): Double = {
      assert (s + v + rho > 0)

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
      if (logwealthminmu < thres)
        minmu
      else {
        val maxmu = Math.min(1, sumXt / t)

        val logwealthmaxmu = logwealth(sumXt - t * maxmu, v)

        assert (logwealthmaxmu <= thres, s"$logwealthmaxmu < $thres")

        val optimizer = new BrentSolver()
        class Func extends UnivariateFunction {
          override def value(mu: Double): Double = logwealth(sumXt - t * mu, v) - thres
        }

        optimizer.solve(100, new Func(), minmu, maxmu)
      }
    }

    BanditEstimatorEmpiricalBernsteinCSOutput(
      lblogwealth(acc.t, acc.sumXlow, acc.sumvlow, alpha/2),
      1 - lblogwealth(acc.t, acc.sumXhigh, acc.sumvhigh, alpha/2))
  }

  def bufferEncoder: Encoder[BanditEstimatorEmpiricalBernsteinCSBuffer] =
    Encoders.product[BanditEstimatorEmpiricalBernsteinCSBuffer]
  def outputEncoder: Encoder[BanditEstimatorEmpiricalBernsteinCSOutput] =
    Encoders.product[BanditEstimatorEmpiricalBernsteinCSOutput]
}