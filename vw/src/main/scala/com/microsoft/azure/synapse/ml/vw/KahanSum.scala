// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

// KahanBabushkaNeumaierSum
final case class KahanSum(sum: Double = 0, c: Double = 0) {
  // TODO: should be done w/ Float too? how to write the generics?
  def +(x: Double): KahanSum = {
    val newSum = sum + x

    val newC = c + (
      if (Math.abs(sum) >= Math.abs(x))
        (sum  - newSum) + x
      else
        (x - newSum) + sum
      )

    KahanSum(newSum, newC)
  }

  def toDouble(): Double = sum + c

  def +(other: KahanSum): KahanSum =
    KahanSum(this.sum, this.c) + other.sum + other.c
}

object KahanSum {
  implicit def double2KahanSum(x: Double): KahanSum = KahanSum(x)
}

class KahanBabushkaNeumaierSumAggregator
  extends Aggregator[Float, KahanSum, Float]
    with Serializable {
  // TODO: doesn't work
  //    with BasicLogging {
  //  logClass()

  //  override val uid: String = Identifiable.randomUID("BanditEstimatorIps")

  def zero: KahanSum = KahanSum(0, 0)

  def reduce(acc: KahanSum, x: Float): KahanSum = acc + x

  def merge(acc1: KahanSum, acc2: KahanSum): KahanSum =
    acc1 + acc2

  def finish(acc: KahanSum): Float = acc.toDouble.toFloat

  def bufferEncoder: Encoder[KahanSum] = Encoders.product[KahanSum]
  def outputEncoder: Encoder[Float] = Encoders.scalaFloat
}