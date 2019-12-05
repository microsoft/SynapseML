// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.nn

import breeze.linalg.DenseVector
import com.microsoft.ml.spark.core.test.benchmarks.Benchmarks

class ConditionalBallTreeTest extends Benchmarks with BallTreeTestBase {
  def naiveSearch(haystack: IndexedSeq[DenseVector[Double]],
                  labels: IndexedSeq[Int],
                  conditioner: Set[Int],
                  needle: DenseVector[Double],
                  k: Int): Seq[BestMatch] = {
    haystack
      .zipWithIndex
      .zip(labels)
      .filter(vl => conditioner(vl._2))
      .map(vl => vl._1)
      .map(p => (p._2, p._1 dot needle))
      .sorted(Ordering.by({ p: (Int, Double) => (p._2, p._1) }).reverse)
      .take(k)
      .map { case (i, d) => BestMatch(i, d) }
  }

  def assertEquivalent(r1: Seq[BestMatch],
                       r2: Seq[BestMatch],
                       conditioner: Set[Int],
                       labels: IndexedSeq[Int]): Unit = {
    r1.zip(r2).foreach { case (cm, gt) =>
      assert(cm.distance === gt.distance)
      assert(conditioner(labels(cm.index)))
      assert(conditioner(labels(gt.index)))
    }
  }

  def compareToNaive(haystack: IndexedSeq[DenseVector[Double]],
                     labels: IndexedSeq[Int],
                     conditioner: Set[Int],
                     k: Int,
                     needle: DenseVector[Double]): Unit = {
    val tree = ConditionalBallTree(haystack, haystack.indices, labels)
    val btMatches = time(tree.findMaximumInnerProducts(needle, conditioner, k))
    val groundTruth = time(naiveSearch(haystack, labels, conditioner, needle, k))
    assertEquivalent(btMatches, groundTruth, conditioner, labels)
  }

  test("search should be exact") {
    val labels = twoClassLabels(uniformData)
    val needle = DenseVector(9.0, 10.0, 11.0)
    val conditioners = Seq(Set(1), Set(2), Set(1, 2))
    val ks = Seq(1, 5, 10, 100)
    for (conditioner <- conditioners; k <- ks) {
      compareToNaive(uniformData, labels, conditioner, k, needle)
    }
  }

  test("Balltree with uniform data") {
    val labels = twoClassLabels(uniformData)
    val keys = Seq(DenseVector(0.0, 0.0, 0.0),
      DenseVector(2.0, 2.0, 20.0),
      DenseVector(9.0, 10.0, 11.0))
    val conditioners = Seq(Set(1), Set(2), Set(1, 2))
    for (key <- keys; conditioner <- conditioners) {
      compareToNaive(uniformData, labels, conditioner, 5, key)
    }
  }

  test("simple structure print") {
    val labels = uniformData.map(dv => if (dv.data(0) < 10.0) {
      0
    } else {
      1
    })
    val bt = ConditionalBallTree(uniformData, uniformData.indices, labels)
    assert(bt.getStructure.head._3.values.sum === 10 * 10 * 10)
  }

  test("Balltree with random data should be correct") {
    val labels = twoClassLabels(randomData)
    compareToNaive(randomData, labels, Set(1), 5, DenseVector(randomData(3).toArray))
  }

  test("Balltree with random data and number of best matches should be correct") {
    val labels = twoClassLabels(randomData)
    val needle = DenseVector(randomData(3).toArray)
    compareToNaive(randomData, labels, Set(1), 5, needle)
  }

}
