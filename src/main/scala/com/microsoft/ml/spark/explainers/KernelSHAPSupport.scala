package com.microsoft.ml.spark.explainers

import breeze.linalg.{DenseVector => BDV}
import breeze.stats.distributions.{Rand, RandBasis}
import org.apache.commons.math3.util.CombinatoricsUtils.{binomialCoefficientDouble => comb}

import scala.annotation.tailrec

private[explainers] trait KernelSHAPSupport {
  protected def featureSize: Int
  protected def numSamples: Int

  protected lazy val coalitionGenerator: Iterator[BDV[Int]] = {
    this.generateCoalitions(featureSize, numSamples)
  }

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
        if (size == comb(n, k)) {
          // Generate all possible combinations
          (0 until n).combinations(k).map {
            comb =>
              val v = BDV.zeros[Int](m)
              v(comb) := 1
              v
          }
        } else {
          // Randomly sample {size} number of combinations
          (0 until size).map {
            _ =>
              randomComb(n, k)
          }
        }
    }
  }
}
