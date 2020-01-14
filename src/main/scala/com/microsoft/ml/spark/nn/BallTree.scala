// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.nn

import java.io.Serializable
import java.util

import breeze.linalg.functions.euclideanDistance
import breeze.linalg.{DenseVector, norm, _}

import scala.collection.mutable
import scala.util.Random
import collection.JavaConversions._
import collection.JavaConverters._

private case class Query(point: DenseVector[Double],
                         normOfQueryPoint: Double,
                         bestMatches: BoundedPriorityQueue[BestMatch]) extends Serializable {

  def this(point: DenseVector[Double], bestK: Int) = this(point, norm(point), Query.createQueue(bestK))

  override def toString: String = {
    s"Query with point ${point}} \n " +
      s"and bestMatches of size of ${bestMatches.size} (bestMatch example: ${bestMatches.take(1)}})"
  }
}

private object Query {
  def createQueue(k: Int): BoundedPriorityQueue[BestMatch] = {
    val bestMatches = new BoundedPriorityQueue[BestMatch](k)(Ordering.by(_.distance))
    bestMatches += BestMatch(-1, Double.NegativeInfinity)
  }
}

trait BallTreeBase[V] {
  val keys: IndexedSeq[DenseVector[Double]]
  val values: IndexedSeq[V]
  val leafSize: Int
  protected val epsilon = 0.000001

  //using java version of Random() cause the scala version is only serializable since scala version 2.11
  protected val randomIntGenerator = new java.util.Random(1)
  protected val dim: Int = keys(0).length
  protected val pointIdx: Range = keys.indices

  private def mean(pointIdx: Seq[Int]): DenseVector[Double] = {
    (1.0 / pointIdx.length) * pointIdx.map(keys(_)).reduce(_ + _)
  }

  private def radius(pointIdx: Seq[Int], point: DenseVector[Double]): Double = {
    pointIdx.map { idx =>
      euclideanDistance(keys(idx), point)
    }.max
  }

  protected def upperBoundMaximumInnerProduct(query: Query, node: Node[_]): Double = {
    (query.point dot node.ball.mu) + (node.ball.radius * query.normOfQueryPoint)
  }

  private def makeBallSplit(pointIdx: Seq[Int]): (Int, Int) = {
    //finding two points in Set that have largest distance
    val randPoint = keys(pointIdx(randomIntGenerator.nextInt(pointIdx.length)))
    val randSubset = pointIdx

    //TODO: Check if not using squared euclidean distance is ok
    val pivotPoint1: Int = randSubset.map { idx: Int => {
      val ed = euclideanDistance(randPoint, keys(idx))
      (idx, ed * ed)
    }
    }.maxBy(_._2)._1
    val pivotPoint2: Int = randSubset.map { idx: Int => {
      val ed = euclideanDistance(keys(pivotPoint1), keys(idx))
      (idx, ed * ed)
    }
    }.maxBy(_._2)._1
    (pivotPoint1, pivotPoint2)
  }

  private def divideSet(pointIdx: Seq[Int], pivot1: Int, pivot2: Int): (Seq[Int], Seq[Int]) = {
    pointIdx.partition { idx =>
      val d1 = euclideanDistance(keys(idx), keys(pivot1))
      val d2 = euclideanDistance(keys(idx), keys(pivot2))
      d1 <= d2
    }
  }

  protected def makeBallTree[T](pointIdx: Seq[Int]): Node[T] = {
    val mu = mean(pointIdx)
    val r = radius(pointIdx, mu)
    val ball = Ball(mu, r)
    if (pointIdx.length <= leafSize || r < epsilon) {
      //Leaf Node
      LeafNode[T](pointIdx, ball, None, None)
    } else {
      //split set
      val (pivot1, pivot2) = makeBallSplit(pointIdx)
      val (leftSubSet, rightSubSet) = divideSet(pointIdx, pivot1, pivot2)
      val leftChild = makeBallTree[T](leftSubSet)
      val rightChild = makeBallTree[T](rightSubSet)
      InnerNode[T](ball, None, None, leftChild, rightChild)
    }
  }

}

/** Performs fast lookups of nearest neighbors using the Ball Tree algorithm for space partitioning
  *
  * Note that this code borrows heavily from
  * https://github.com/felixmaximilian/mips
  *
  * @author Felix Maximilian
  */
case class BallTree[V](override val keys: IndexedSeq[DenseVector[Double]],
                       override val values: IndexedSeq[V],
                       override val leafSize: Int = 50) extends Serializable with BallTreeBase[V] {

  private val root: Node[Nothing] = makeBallTree(pointIdx)

  private def linearSearch(query: Query, node: LeafNode[_]): Unit = {
    val bestMatchesCandidates = node.pointIdx.map { idx =>
      BestMatch(idx, query.point dot keys(idx))
    }
    query.bestMatches ++= bestMatchesCandidates
  }

  private def traverseTree(query: Query, node: Node[_] = root): Unit = {
    if (query.bestMatches.head.distance <= upperBoundMaximumInnerProduct(query, node)) {
      //This node has potential
      node match {
        case ln: LeafNode[_] => linearSearch(query, ln)
        case InnerNode(_, _, _, leftChild, rightChild) =>
          val boundLeft = upperBoundMaximumInnerProduct(query, leftChild)
          val boundRight = upperBoundMaximumInnerProduct(query, rightChild)
          if (boundLeft <= boundRight) {
            traverseTree(query, rightChild)
            traverseTree(query, leftChild)
          } else {
            traverseTree(query, leftChild)
            traverseTree(query, rightChild)
          }
        case x => throw new RuntimeException(
          s"default case in match has been visited for type${x.getClass}: " + x.toString)
      }
    } else {
      //ignoring this subtree
    }
  }

  def findMaximumInnerProducts(queryPoint: DenseVector[Double], k: Int = 1): Seq[BestMatch] = {
    val query = new Query(queryPoint, k)
    traverseTree(query)
    query.bestMatches.toArray
      .filter(_.index != -1)
      .sorted(Ordering.by({ bm: BestMatch => (bm.distance, bm.index) }).reverse)
  }

  override def toString: String = {
    s"Balltree with data size of ${keys.length}"
  }
}

object ConditionalBallTree {

  def apply[L, V](keys: java.util.ArrayList[java.util.ArrayList[Double]],
                  values: java.util.ArrayList[V],
                  labels: java.util.ArrayList[L],
                  leafSize: Int): ConditionalBallTree[L, V] = {
    ConditionalBallTree(
      keys.toIndexedSeq.map(vals => DenseVector(vals.toList.toArray)),
      values.toIndexedSeq,
      labels.toIndexedSeq,
      leafSize)
  }
}

case class ConditionalBallTree[L, V](override val keys: IndexedSeq[DenseVector[Double]],
                                     override val values: IndexedSeq[V],
                                     labels: IndexedSeq[L],
                                     override val leafSize: Int = 50) extends Serializable with BallTreeBase[V] {


  private val root: Node[L] = addStats(makeBallTree(pointIdx))

  private def addStats(node: Node[L]): Node[L] = {
    node match {
      case l: LeafNode[L] =>
        LeafNode[L](
          l.pointIdx,
          l.ball,
          Some(l.pointIdx.map(labels).toSet),
          l.counts
        )
      case i: InnerNode[L] =>
        val lc = addStats(i.leftChild)
        val rc = addStats(i.rightChild)
        InnerNode(i.ball,
          Some(lc.stats.get | rc.stats.get),
          i.counts,
          lc,
          rc)
    }
  }

  private def linearSearch(query: Query, conditioner: Set[L], node: LeafNode[L]): Unit = {
    val bestMatchesCandidates = node.pointIdx
      .filter(idx => conditioner(labels(idx)))
      .map(idx => BestMatch(idx, query.point dot keys(idx)))

    query.bestMatches ++= bestMatchesCandidates
  }

  private def traverseTree(query: Query, conditioner: Set[L], node: Node[L] = root): Unit = {
    if ((node.stats.get & conditioner).nonEmpty &
      query.bestMatches.head.distance <= upperBoundMaximumInnerProduct(query, node)) {

      //This node has potential
      node match {
        case ln: LeafNode[L] => linearSearch(query, conditioner, ln)
        case InnerNode(_, _, _, leftChild, rightChild) =>
          val boundLeft = upperBoundMaximumInnerProduct(query, leftChild)
          val boundRight = upperBoundMaximumInnerProduct(query, rightChild)
          if (boundLeft <= boundRight) {
            traverseTree(query, conditioner, rightChild)
            traverseTree(query, conditioner, leftChild)
          } else {
            traverseTree(query, conditioner, leftChild)
            traverseTree(query, conditioner, rightChild)
          }
        case x => throw new RuntimeException(
          s"default case in match has been visited for type${x.getClass}: " + x.toString)
      }
    } else {
      //ignoring this subtree
    }
  }

  def findMaximumInnerProducts(queryPoint: java.util.ArrayList[Double],
                               conditioner: Set[L],
                               k: Int): Seq[BestMatch] = {
    findMaximumInnerProducts(new DenseVector(queryPoint.toList.toArray), conditioner, k)
  }

  def findMaximumInnerProducts(queryPoint: DenseVector[Double],
                               conditioner: Set[L],
                               k: Int = 1): Seq[BestMatch] = {
    val query = new Query(queryPoint, k)
    traverseTree(query, conditioner)
    query.bestMatches.toArray
      .filter(_.index != -1)
      .sorted(Ordering.by({ bm: BestMatch => (bm.distance, bm.index) }).reverse)
  }

  private def addCounts(node: Node[L]): Node[L] = {
    node match {
      case l: LeafNode[L] =>
        LeafNode[L](
          l.pointIdx,
          l.ball,
          l.stats,
          Some(l.pointIdx.map(labels).foldLeft(Map[L, Int]()) { case (stats, label) =>
            stats.updated(label, stats.getOrElse(label, 0) + 1)
          })
        )
      case i: InnerNode[L] =>
        val lc = addCounts(i.leftChild)
        val rc = addCounts(i.rightChild)
        val mergedCounts = lc.counts.get.foldLeft(rc.counts.get) { case (counts, (key, value)) =>
          counts.updated(key, counts.getOrElse(key, 0) + value)
        }
        InnerNode(i.ball, i.stats, Some(mergedCounts), lc, rc)
    }
  }

  private def getValuesForNode(node: Node[_]): Seq[V] = {
    node match {
      case ln: LeafNode[_] => ln.pointIdx.map(values)
      case in: InnerNode[_] => getValuesForNode(in.leftChild) ++ getValuesForNode(in.rightChild)
    }
  }

  private def linearizeNodesRecursive(node: Node[L],
                                      parent: String,
                                      dataSoFar: mutable.ListBuffer[(String, String, Map[L, Int], Seq[V])]): Unit = {
    val id = dataSoFar.length.toString
    dataSoFar.append((id, parent, node.counts.get, getValuesForNode(node)))
    node match {
      case ln: LeafNode[L] => ()
      case in: InnerNode[L] =>
        linearizeNodesRecursive(in.rightChild, id, dataSoFar)
        linearizeNodesRecursive(in.leftChild, id, dataSoFar)
    }
  }

  def getStructure: List[(String, String, Map[L, Int], Seq[V])] = {
    val data = mutable.ListBuffer[(String, String, Map[L, Int], Seq[V])]()
    val rootWithCounts = addCounts(root)
    linearizeNodesRecursive(rootWithCounts, "", data)
    data.toList
  }

  private[ml] def getRandomizedStructure: List[(String, String, Map[L, Int], Seq[V])] = {
    ConditionalBallTree(keys, values, new Random(1).shuffle(labels), leafSize).getStructure
  }

  override def toString: String = {
    s"Conditional Balltree with data size of ${keys.length}"
  }
}
