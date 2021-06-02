package com.microsoft.ml.spark.explainers

import breeze.linalg.{BitVector, axpy, norm, DenseVector => BDV}
import breeze.stats.distributions.{Rand, RandBasis}
import org.apache.commons.math3.util.CombinatoricsUtils.{binomialCoefficientDouble => comb}
import org.apache.spark.ml.linalg.{Vector => SV}
import com.microsoft.ml.spark.explainers.BreezeUtils._

import scala.annotation.tailrec
import scala.util.Random

private[explainers] trait SamplerSupport {
  def nextState: SV

  def getDistance(state: SV): Double = {
    // Set distance to normalized Euclidean distance
    // 1 in the mask means keep the superpixel, 0 means replace with background color,
    // so a vector of all 1 means the original observation.
    norm(1.0 - state.toBreeze, 2) / math.sqrt(state.size)
  }
}

private[explainers] trait LIMESamplerSupport extends SamplerSupport {
  def featureSize: Int

  def randBasis: RandBasis

  def samplingFraction: Double

  private lazy val randomStateGenerator: Iterator[SV] = new Iterator[SV] {
    override def hasNext: Boolean = true

    override def next(): SV = {
      val mask: BitVector = BDV.rand(featureSize, randBasis.uniform) <:= samplingFraction
      val maskAsDouble = BDV.zeros[Double](featureSize)
      axpy(1.0, mask, maskAsDouble) // equivalent to: maskAsDouble += 1.0 * mask
      maskAsDouble.toSpark
    }
  }

  override def nextState: SV = this.randomStateGenerator.next
}

private[explainers] trait KernelSHAPSamplerSupport extends SamplerSupport {
  protected def featureSize: Int

  protected def numSamples: Int

  protected lazy val randomStateGenerator: Iterator[SV] = {
    this.generateCoalitions(featureSize, numSamples).map {
      v => v.mapValues(_.toDouble).toSpark
    }
  }

  override def nextState: SV = this.randomStateGenerator.next

  private def generateSampleSizes(m: Int, nSamples: Int): List[Int] = {
    assert(m > 0)
    assert(nSamples >= 0)

    def generateCoalitionLengths(n: Int, k: Int, b: Int): List[Int] = {
      @tailrec
      def recurse(n: Int, k: Int, b: Int, acc: List[Int]): List[Int] = {
        assert(b >= 0)
        if (k > n / 2) {
          // Do not exceed half depth
          acc
        } else {
          val c = comb(n, k).toInt
          if (k * 2 == n) {
            // mid depth for even n
            acc :+ math.min(b, c)
          } else if (b <= 2 * c) {
            // Not enough budget left, then divide the budget between (n, k) and (n, n-k)
            if (b == 0) {
              acc
            } else if (b % 2 == 0) {
              acc :+ (b / 2) :+ (b / 2)
            } else {
              acc :+ (b / 2 + 1) :+ (b / 2)
            }
          } else {
            val newAcc = acc :+ c :+ c
            // Recurse to solve the next depth. Deduct budget for (n, k) and (n, n-k)
            recurse(n, k + 1, b - 2 * c, newAcc)
          }
        }
      }

      recurse(n, k, b, List.empty)
    }

    generateCoalitionLengths(m, 0, nSamples)
  }

  private def randomComb(n: Int, k: Int): BDV[Int] = {
    val indices = Rand.permutation(n).sample.take(k)
    val v = BDV.zeros[Int](n)
    v(indices) := 1
    v
  }

  protected def generateCoalitions(m: Int, nSamples: Int): Iterator[BDV[Int]] = {
    assert(m > 0)
    assert(nSamples > 0)

    val sizes = generateSampleSizes(m, nSamples).toIterator

    sizes.zipWithIndex.map {
      case (size, i) =>
        val k = if (i % 2 == 0) i / 2 else m - (i - 1) / 2
        (m, k, size)
    }.flatMap {
      case (n, k, size) =>
        // Generate all possible combinations
        val allCombs = (0 until n).combinations(k)
        val combs = if (size == comb(n, k)) {
          allCombs
        } else {
          // Randomly sample {size} number of combinations
          Random.shuffle(allCombs).take(size)
        }

        combs.map {
          comb =>
            val v = BDV.zeros[Int](m)
            v(comb) := 1
            v
        }
    }
  }
}
