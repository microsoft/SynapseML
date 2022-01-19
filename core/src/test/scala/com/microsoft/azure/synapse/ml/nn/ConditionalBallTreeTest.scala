// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nn

import breeze.linalg.DenseVector
import com.microsoft.azure.synapse.ml.core.test.benchmarks.Benchmarks

import scala.collection.mutable
import scala.util.Random

class TreeVizualizer[V](cbt: ConditionalBallTree[String, V]) {

  private def getInfoForNode(node: Node): Seq[(String, V)] = {
    node match {
      case ln: LeafNode => ln.pointIdx.map(i => (cbt.labels(i), cbt.values(i)))
      case in: InnerNode => getInfoForNode(in.leftChild) ++ getInfoForNode(in.rightChild)
    }
  }

  type NodeInformation = (String, String, Map[String, Int], Seq[V])

  private def linearizeNodesRecursive(node: Node,
                                      parent: String,
                                      dataSoFar: mutable.ListBuffer[NodeInformation]): Unit = {
    val id = dataSoFar.length.toString
    val nodeInfo = getInfoForNode(node)
    dataSoFar.append((id, parent,
      nodeInfo.map(_._1).foldLeft(Map[String, Int]()) {
        case (state, label) => state.updated(label, state.getOrElse(label, 0) + 1)
      },
      nodeInfo.map(_._2)))
    node match {
      case ln: LeafNode => ()
      case in: InnerNode =>
        linearizeNodesRecursive(in.rightChild, id, dataSoFar)
        linearizeNodesRecursive(in.leftChild, id, dataSoFar)
    }
  }

  def getStructure: List[NodeInformation] = {
    val data = mutable.ListBuffer[NodeInformation]()
    linearizeNodesRecursive(cbt.root, "", data)
    data.toList
  }

  private[ml] def getRandomizedStructure: List[NodeInformation] = {
    new TreeVizualizer(ConditionalBallTree(
      cbt.keys, cbt.values, new Random(1).shuffle(cbt.labels), cbt.leafSize)).getStructure
  }

}

class ConditionalBallTreeTest extends Benchmarks with BallTreeTestBase {
  def naiveSearch[L](haystack: IndexedSeq[DenseVector[Double]],
                  labels: IndexedSeq[L],
                  conditioner: Set[L],
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

  def assertEquivalent[L](r1: Seq[BestMatch],
                       r2: Seq[BestMatch],
                       conditioner: Set[L],
                       labels: IndexedSeq[L]): Unit = {
    assert(r1.length == r2.length)
    r1.zip(r2).foreach { case (cm, gt) =>
      assert(cm.distance === gt.distance, s"$cm, $gt, $conditioner, ${labels(cm.index)}, ${labels(gt.index)}")
      assert(conditioner(labels(cm.index)))
      assert(conditioner(labels(gt.index)))
    }
  }

  def compareToNaive[L](haystack: IndexedSeq[DenseVector[Double]],
                     labels: IndexedSeq[L],
                     conditioner: Set[L],
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

  test("string conditioner") {
    val labels = twoClassStringLabels(uniformData)
    val needle = DenseVector(9.0, 10.0, 11.0)
    val conditioners = Seq(Set("1"), Set("2"), Set("1", "2"))
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
