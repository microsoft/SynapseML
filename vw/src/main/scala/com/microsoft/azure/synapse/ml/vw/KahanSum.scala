// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.logging.BasicLogging
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

/**
  * Implementation of Kahan summation (https://en.wikipedia.org/wiki/Kahan_summation_algorithm)
  *
  * Kahan-Babushka-Neumaier sum aggregator make sure lots of small numbers are accumulated numerically stable.
  */
class KahanSumAggregator
  extends Aggregator[Float, KahanSum, Float]
    with Serializable
    with BasicLogging {
  override val uid: String = Identifiable.randomUID("BanditEstimatorIps")

  logClass()

  def zero: KahanSum = KahanSum()

  def reduce(acc: KahanSum, x: Float): KahanSum = acc + x

  def merge(acc1: KahanSum, acc2: KahanSum): KahanSum =
    acc1 + acc2

  def finish(acc: KahanSum): Float = acc.toDouble.toFloat

  def bufferEncoder: Encoder[KahanSum] = Encoders.product[KahanSum]
  def outputEncoder: Encoder[Float] = Encoders.scalaFloat
}

/**
  * Aggregator state.
  *
  * @param sum the accumulator
  * @param c a running compensation for lost low-order bits
  * @note cannot use generics for double vs float due to perf:
  *       https://stackoverflow.com/questions/4753629/how-do-i-make-a-class-generic-for-all-numeric-types
  */
final case class KahanSum(sum: Double = 0, c: Double = 0) {
  def +(x: Double): KahanSum = {
    val newSum = sum + x

    val newC = c + (
      if (math.abs(sum) >= math.abs(x.abs))
        (sum  - newSum) + x
      else
        (x - newSum) + sum
      )

    KahanSum(newSum, newC)
  }

  def toDouble: Double = sum + c

  def +(other: KahanSum): KahanSum =
    KahanSum(this.sum, this.c) + other.sum + other.c
}

object KahanSum {
  implicit def double2KahanSum(x: Double): KahanSum = KahanSum(x)
}
