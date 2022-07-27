// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

// TODO: add doc
// KahanBabushkaNeumaierSum
final case class KahanSum[T:Numeric](sum: T = 0, c: T = 0) {

  // TODO: should be done w/ Float too? how to write the generics?
  def +(x: T): KahanSum[T] = {
    import Numeric.Implicits._
    import Ordering.Implicits._

    val newSum = sum + x

    val newC = c + (
      if (sum.abs() >= x.abs())
        (sum  - newSum) + x
      else
        (x - newSum) + sum
      )

    KahanSum(newSum, newC)
  }

  def toDouble: Double = {
    import Numeric.Implicits._

    (sum + c).toDouble
  }

  def +(other: KahanSum[T]): KahanSum[T] =
    KahanSum(this.sum, this.c) + other.sum + other.c
}

object KahanSum {

  implicit def double2KahanSum(x: Double): KahanSum[Double] = KahanSum(x)
}

/**
  * Kahan-Babushka-Neumaier sum aggregator make sure lots of small numbers are accumulated numerically stable.
  */
class KahanSumAggregator
  extends Aggregator[Float, KahanSum[Double], Float]
    with Serializable {
  // TODO: doesn't work
  //    with BasicLogging {
  //  logClass()

  //  override val uid: String = Identifiable.randomUID("BanditEstimatorIps")

  def zero: KahanSum[Double] = KahanSum()

  def reduce(acc: KahanSum[Double], x: Float): KahanSum[Double] = acc + x

  def merge(acc1: KahanSum[Double], acc2: KahanSum[Double]): KahanSum[Double] =
    acc1 + acc2

  def finish(acc: KahanSum[Double]): Float = acc.toDouble.toFloat

  def bufferEncoder: Encoder[KahanSum[Double]] = Encoders.product[KahanSum[Double]]
  def outputEncoder: Encoder[Float] = Encoders.scalaFloat
}
