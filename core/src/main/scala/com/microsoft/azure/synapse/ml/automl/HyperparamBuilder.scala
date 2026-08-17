// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.automl

import org.apache.spark.ml.param._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

abstract class RangeHyperParam[T](val min: T, val max: T, val seed: Long) extends Dist[T] {

  val random = new Random(seed)

}

class IntRangeHyperParam(min: Int, max: Int, seed: Long = 0)
    extends RangeHyperParam[Int](min, max, seed) {

  def getNext(): Int = {
    val range = max - min
    random.nextInt(range) + min
  }

}

class LongRangeHyperParam(min: Long, max: Long, seed: Long = 0)
    extends RangeHyperParam[Long](min, max, seed) {

  def getNext(): Long = {
    require(max >= min, s"LongRangeHyperParam requires max >= min, got min=$min, max=$max")
    val range = max - min
    // nextLong() spans the full 64-bit range, so multiplying it by the range overflows and
    // escapes [min, max) entirely. Reduce into the range first, as nextInt(range) does.
    if (range == 0) {
      min
    } else if (range < 0) {
      // The span is wider than Long.MaxValue so it cannot be represented as a Long. Draw from
      // the whole 64-bit range instead; more than half of it lands in bounds, so this is cheap.
      @tailrec def drawInBounds(): Long = {
        val v = random.nextLong()
        if (v >= min && v < max) v else drawInBounds()
      }
      drawInBounds()
    } else {
      boundedNextLong(range) + min
    }
  }

  /** Uniform draw from [0, bound), rejecting the partial final block so the result carries no
    * modulo bias. Mirrors what java.util.Random.nextInt(bound) already does for Int.
    */
  private def boundedNextLong(bound: Long): Long = {
    val m = bound - 1
    if ((bound & m) == 0L) {
      random.nextLong() & m // bound is a power of two, so the low bits are already uniform
    } else {
      @tailrec def draw(): Long = {
        val u = random.nextLong() >>> 1
        val r = u % bound
        if (u + m - r < 0L) draw() else r
      }
      draw()
    }
  }

}

class FloatRangeHyperParam(min: Float, max: Float, seed: Long = 0)
    extends RangeHyperParam[Float](min, max, seed) {

  val doubleRange = new DoubleRangeHyperParam(min.toDouble, max.toDouble, seed)
  def getNext(): Float = {
    doubleRange.getNext().toFloat
  }

}

class DoubleRangeHyperParam(min: Double, max: Double, seed: Long = 0)
    extends RangeHyperParam[Double](min, max, seed) {

  def getNext(): Double = {
    val range = max - min
    (random.nextDouble() * range) + min
  }

}

object HyperParamUtils {
  /** Returns a range hyper param by matching to the given input type.
    * @param min The min value of the range
    * @param max The max value of the range
    * @param seed The random number seed.
    * @return A RangeHyperParam matched to the given type for min and max values.
    */
  def getRangeHyperParam(min: Any, max: Any, seed: Long = 0): RangeHyperParam[_] = {
    (min, max) match {
      case (minD: Double, maxD: Double) => new DoubleRangeHyperParam(minD, maxD, seed)
      case (minF: Float, maxF: Float) => new FloatRangeHyperParam(minF, maxF, seed)
      case (minL: Long, maxL: Long) => new LongRangeHyperParam(minL, maxL, seed)
      case (minI: Int, maxI: Int) => new IntRangeHyperParam(minI, maxI, seed)
      case _ => throw new Exception("Could not match RangeHyperParam constructor to the given type")
    }
  }

  /** Returns a discrete hyper param given a Java ArrayList through JavaConversions.
    * @param values The list of values from Java.
    * @param seed The random number seed.
    * @return A RangeHyperParam matched to the given type for min and max values.
    */
  def getDiscreteHyperParam(values: java.util.ArrayList[_], seed: Long = 0): DiscreteHyperParam[_] = {
    val valuesList = values.asScala.toList
    new DiscreteHyperParam(valuesList, seed)
  }
}

class DiscreteHyperParam[T](values: List[T], seed: Long = 0) extends Dist[T] {

  val random = new Random(seed)

  def getNext(): T = {
    values(random.nextInt(values.length))
  }

  /**
   * Return the list of values as an java List for py4j side.
   */
  def getValues: java.util.List[T] = {
    values.asJava
  }
}

/** Specifies the search space for hyperparameters.
  */
class HyperparamBuilder {

  private val hyperparams = mutable.Map.empty[Param[_], Dist[_]]

  /** Adds a param to the search space.
    */
  def addHyperparam[T](param: Param[T], values: Dist[T]): this.type = {
    hyperparams.put(param, values)
    this
  }

  /** Builds the search space of hyperparameters.
    * @return The map of hyperparameters to search through.
    */
  def build(): Array[(Param[_], Dist[_])] = hyperparams.toArray

}
