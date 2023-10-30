// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.policyeval

import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.policyeval
import com.microsoft.azure.synapse.ml.vw.KahanSum
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

/**
  * Cressie-Read off-policy evaluation metric.
  *
  * Background http://www.machinedlearnings.com/2020/12/distributionally-robust-contextual.html
  */
class CressieRead
  extends Aggregator[CressieReadInput, CressieReadBuffer, Double]
    with Serializable
    with SynapseMLLogging {
  override val uid: String = Identifiable.randomUID("BanditEstimatorCressieRead")

  logClass(FeatureNames.VowpalWabbit)

  def zero: CressieReadBuffer = policyeval.CressieReadBuffer()

  def reduce(acc: CressieReadBuffer, x: CressieReadInput): CressieReadBuffer = {
    val w = x.probPred / x.probLog
    val countW = x.count * w
    val countWsq = countW * countW

    CressieReadBuffer(
      wMin = Math.min(acc.wMin, x.wMin),
      wMax = Math.max(acc.wMax, x.wMax),
      n = acc.n + x.count,
      sumw = acc.sumw + countW,
      sumwsq = acc.sumwsq + countWsq,
      sumwr = acc.sumwr + countW * x.reward,
      sumwrsqr = acc.sumwrsqr + countWsq * x.reward,
      sumr = acc.sumr + x.count * x.reward
    )
  }

  def merge(acc1: CressieReadBuffer,
            acc2: CressieReadBuffer): CressieReadBuffer = {
    CressieReadBuffer(
      // min of min, max of max
      wMin = Math.min(acc1.wMin, acc2.wMin),
      wMax = Math.max(acc1.wMax, acc2.wMax),
      // sum up
      n = acc1.n + acc2.n,
      sumw = acc1.sumw + acc2.sumw,
      sumwsq = acc1.sumwsq + acc2.sumwsq,
      sumwr = acc1.sumwr + acc2.sumwr,
      sumwrsqr = acc1.sumwrsqr + acc2.sumwrsqr,
      sumr = acc1.sumr + acc2.sumr)
  }

  def finish(acc: CressieReadBuffer): Double = {
    logVerb("aggregate", {
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

          assert(a * a <= b)

          ((b - a) / (a * a - b), (1 - a) / (a * a - b))
        }
      }

      val vhat = (-gamma * sumwr - beta * sumwsqr) / (1 + n)
      val missing = Math.max(0, 1 - (-gamma * sumw - beta * sumwsq) / (1 + n))
      val rhatmissing = sumr / n

      vhat + missing * rhatmissing
    })
  }

  def bufferEncoder: Encoder[CressieReadBuffer] = Encoders.product[CressieReadBuffer]
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

// Internal buffer state
final case class CressieReadBuffer(wMin: Float = 0,
                                   wMax: Float = 0,
                                   n: KahanSum = 0,
                                   sumw: KahanSum = 0,
                                   sumwsq: KahanSum = 0,
                                   sumwr: KahanSum = 0,
                                   sumwrsqr: KahanSum = 0,
                                   sumr: KahanSum = 0)

// Input fields to aggregate over
final case class CressieReadInput(probLog: Float,
                                  reward: Float,
                                  probPred: Float,
                                  count: Float,
                                  wMin: Float,
                                  wMax: Float)
