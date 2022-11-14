// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers

import breeze.linalg.{sum, DenseVector => BDV}
import com.microsoft.azure.synapse.ml.core.utils.BreezeUtils._
import org.apache.commons.math3.util.CombinatoricsUtils.{binomialCoefficientDouble => comb}
import org.apache.spark.ml.linalg.Vector

import scala.annotation.tailrec
import scala.util.Random

private[explainers]trait KernelSHAPSampler[TObservation] extends Sampler[TObservation, Vector] {
  def sample: (TObservation, Vector, Double) = {
    val (state, weight) = nextState
    val newSample = createNewSample(instance, state)
    (newSample, state, weight)
  }

  protected def nextState: (Vector, Double)
}

private[explainers] trait KernelSHAPSamplerSupport {
  protected def featureSize: Int

  protected def numSamples: Int

  protected def infWeight: Double

  protected def nextState: (Vector, Double) = randomStateGenerator.next

  private lazy val randomStateGenerator: Iterator[(Vector, Double)] = {
    this.generateCoalitions.map {
      case (v, w) => (v.mapValues(_.toDouble).toSpark, w)
    }
  }

  private def rescale(v: BDV[Double]): BDV[Double] = {
    v /:/ sum(v)
  }

  //scalastyle:off method.length
  private[explainers] def generateSampleSizes(m: Int, numSamples: Int)
                                             (kernelWeightFunc: Int => Double): Array[(Int, Double)] = {
    assert(numSamples <= math.pow(2, m) - 2)
    assert(numSamples > 0)
    assert(m > 0)

    val (numSubsets, numPairedSubset) = (m / 2, (m - 1) / 2)
    val weightsSeq = (1 to numSubsets) map (i => (m - 1).toDouble / (i * (m - i)))
    val weights = new BDV[Double](weightsSeq.toArray)
    weights(0 until numPairedSubset) *= 2.0

    @tailrec
    def recurse(k: Int, samplesLeft: Int, acc: Array[(Int, Double)]): Array[(Int, Double)] = {
      assert(samplesLeft > 0)
      if (k > numSubsets) {
        acc
      } else {
        val kernelWeight = kernelWeightFunc(k)
        val rescaledWeights = rescale(weights(k - 1 to -1))
        val paired = k <= numPairedSubset
        val combo = if (paired) comb(m, k).toLong * 2 else comb(m, k).toLong
        val allocation = rescaledWeights(0) * samplesLeft
        val subsetSizes = {
          if (allocation >= combo) {
            // subset of 'comb' size is filled up.
            if (paired) {
              (combo / 2).toInt :: (combo / 2).toInt :: Nil
            } else {
              combo.toInt :: Nil
            }
          } else {
            Nil
          }
        }

        if (subsetSizes.isEmpty) {
          acc
        } else {
          val newSamplesLeft = samplesLeft - sum(subsetSizes)
          if (newSamplesLeft > 0) {
            recurse(k + 1, newSamplesLeft, acc ++ subsetSizes.map(s => (s, kernelWeight)))
          } else {
            acc ++ subsetSizes.map(s => (s, kernelWeight))
          }
        }
      }
    }

    @tailrec
    def allocateRemainingSamples(k: Int, samplesLeft: Int, acc: Array[Int]): Array[Int] = {
      val rescaledWeights = rescale(weights(k - 1 to -1))
      val paired = k <= numPairedSubset
      val allocation = rescaledWeights(0) * samplesLeft
      val subsetSizes = {
        if (paired) {
          if (allocation >= 1) {
            val half = math.ceil(allocation / 2).toInt
            half :: half :: Nil
          } else {
            1 :: 0 :: Nil
          }
        } else {
          allocation.toInt :: Nil
        }
      }

      val newSamplesLeft = samplesLeft - sum(subsetSizes)
      if (newSamplesLeft > 0) {
        allocateRemainingSamples(k + 1, newSamplesLeft, acc ++ subsetSizes)
      } else {
        acc ++ subsetSizes
      }
    }

    val result = recurse(1, numSamples, Array.empty)

    val remainingSamples = numSamples - sum(result.map(_._1))

    if (remainingSamples == 0) {
      result
    } else {
      result ++ allocateRemainingSamples(result.length / 2 + 1, remainingSamples, Array.empty).map(s => (s, 1.0))
    }
  }
  //scalastyle:on method.length

  private[explainers] def generateCoalitions: Iterator[(BDV[Int], Double)] = {
    val (m, nSamples) = (featureSize, numSamples)
    assert(m > 0)
    assert(nSamples >= 2)
    assert(nSamples <= math.pow(2, m))

    val kernelFunc = (k: Int) => (nSamples - 1).toDouble / (k * (nSamples - k)) / comb(m, k)
    val sizeWeightArray: Array[(Int, Double)] = Array((1, infWeight), (1, infWeight)) ++
      generateSampleSizes(featureSize, numSamples - 2)(kernelFunc)

    sizeWeightArray.toIterator.zipWithIndex.map {
      case ((size, weight), i) =>
        val k = if (i % 2 == 0) i / 2 else m - (i - 1) / 2
        (m, k, size, weight)
    }.flatMap {
      case (n, k, size, weight) =>
        // Generate all possible combinations
        val allCombs = (0 until n).combinations(k)
        val combs = if (size == comb(n, k).toLong) {
          allCombs
        } else {
          // Randomly sample {size} number of combinations, each taking k indices from (0 until n).
          (0 until size).map(_ => Random.shuffle((0 until n).toList).take(k))
        }

        combs.map {
          comb =>
            val v = BDV.zeros[Int](m)
            v(comb) := 1
            (v, weight)
        }
    }
  }
}
