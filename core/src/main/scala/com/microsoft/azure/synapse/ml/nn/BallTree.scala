// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nn

import breeze.linalg.{DenseVector, norm, _}
import com.microsoft.azure.synapse.ml.core.env.StreamUtilities.using

import java.io._
import scala.collection.JavaConverters._

private case class Query(point: DenseVector[Double],
                         normOfQueryPoint: Double,
                         bestMatches: BoundedPriorityQueue[BestMatch]) extends Serializable {

  def this(point: DenseVector[Double], bestK: Int) = this(point, norm(point), Query.createQueue(bestK))

  override def toString: String = {
    s"Query with point {$point}} \n " +
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

  protected def upperBoundMaximumInnerProduct(query: Query, node: Node): Double = {
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

  protected def makeBallTree(pointIdx: Seq[Int]): Node = {
    val mu = mean(pointIdx)
    val r = radius(pointIdx, mu)
    val ball = Ball(mu, r)
    if (pointIdx.length <= leafSize || r < epsilon) {
      //Leaf Node
      LeafNode(pointIdx, ball)
    } else {
      //split set
      val (pivot1, pivot2) = makeBallSplit(pointIdx)
      val (leftSubSet, rightSubSet) = divideSet(pointIdx, pivot1, pivot2)
      val leftChild = makeBallTree(leftSubSet)
      val rightChild = makeBallTree(rightSubSet)
      InnerNode(ball, leftChild, rightChild)
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
                       override val leafSize: Int = 50)  //scalastyle:ignore magic.number
                       extends Serializable with BallTreeBase[V] {

  private val root: Node = makeBallTree(pointIdx)

  private def linearSearch(query: Query, node: LeafNode): Unit = {
    val bestMatchesCandidates = node.pointIdx.map { idx =>
      BestMatch(idx, query.point dot keys(idx))
    }
    query.bestMatches ++= bestMatchesCandidates
  }

  private def traverseTree(query: Query, node: Node = root): Unit = {
    if (query.bestMatches.head.distance <= upperBoundMaximumInnerProduct(query, node)) {
      //This node has potential
      node match {
        case ln: LeafNode => linearSearch(query, ln)
        case InnerNode(_, leftChild, rightChild) =>
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
      keys.asScala.map(vals => DenseVector(vals.asScala.toArray)).toIndexedSeq,
      values.asScala.toIndexedSeq,
      labels.asScala.toIndexedSeq,
      leafSize)
  }

  def load[L, V](filename: String): ConditionalBallTree[L, V] = {
    using(new FileInputStream(filename)) { fileIn =>
      using(new ObjectInputStream(fileIn)) { in =>
        in.readObject().asInstanceOf[ConditionalBallTree[L, V]]
      }
    }.get.get
  }

}

class ReverseIndex[L](ballTree: Node, labels: IndexedSeq[L]) extends Serializable {

  private def makeIndexRecursive(node: Node): Map[L, Set[Node]] = {
    node match {
      case ln: LeafNode =>
        ln.pointIdx.map(labels).toSet.map { label: L => (label, Set(node)) }.toMap
      case InnerNode(_, lc, rc) =>
        makeIndexRecursive(lc).foldLeft(makeIndexRecursive(rc)) { case (state, (label, nodes)) =>
          state.updated(label, state.getOrElse(label, Set()) | nodes)
        }.map { case (label, nodes) => (label, nodes | Set(node)) }
    }
  }

  private val index: Map[L, Set[Node]] = makeIndexRecursive(ballTree)

  def nodeSubset(conditioner: Set[L]): Set[Node] = {
    conditioner.map(index).reduce(_ | _)
  }

}

case class ConditionalBallTree[L, V](override val keys: IndexedSeq[DenseVector[Double]],
                                     override val values: IndexedSeq[V],
                                     labels: IndexedSeq[L],
                                     override val leafSize: Int = 50)  //scalastyle:ignore magic.number
  extends Serializable with BallTreeBase[V] {

  private[ml] val root: Node = makeBallTree(pointIdx)
  private[ml] val reverseIndex = new ReverseIndex[L](root, labels)

  private def linearSearch(query: Query, conditioner: Set[L], node: LeafNode): Unit = {
    val bestMatchesCandidates = node.pointIdx
      .filter(idx => conditioner(labels(idx)))
      .map(idx => BestMatch(idx, query.point dot keys(idx)))

    query.bestMatches ++= bestMatchesCandidates
  }

  private def traverseTree(query: Query, conditioner: Set[L], nodeSubset: Set[Node], node: Node = root): Unit = {
    if (nodeSubset(node) &
      query.bestMatches.head.distance <= upperBoundMaximumInnerProduct(query, node)) {

      //This node has potential
      node match {
        case ln: LeafNode => linearSearch(query, conditioner, ln)
        case InnerNode(_, leftChild, rightChild) =>
          val boundLeft = upperBoundMaximumInnerProduct(query, leftChild)
          val boundRight = upperBoundMaximumInnerProduct(query, rightChild)
          if (boundLeft <= boundRight) {
            traverseTree(query, conditioner, nodeSubset, rightChild)
            traverseTree(query, conditioner, nodeSubset, leftChild)
          } else {
            traverseTree(query, conditioner, nodeSubset, leftChild)
            traverseTree(query, conditioner, nodeSubset, rightChild)
          }
        case x => throw new RuntimeException(
          s"default case in match has been visited for type${x.getClass}: " + x.toString)
      }
    } else {
      //ignoring this subtree
    }
  }

  def findMaximumInnerProducts(queryPoint: java.util.ArrayList[Double],
                               conditioner: java.util.Set[L],
                               k: Int): java.util.List[BestMatch] = {
    findMaximumInnerProducts(new DenseVector(queryPoint.asScala.toArray), conditioner.asScala.toSet, k).asJava
  }

  def findMaximumInnerProducts(queryPoint: DenseVector[Double],
                               conditioner: Set[L],
                               k: Int = 1): Seq[BestMatch] = {
    val query = new Query(queryPoint, k)
    traverseTree(query, conditioner, reverseIndex.nodeSubset(conditioner))
    query.bestMatches.toArray
      .filter(_.index != -1)
      .sorted(Ordering.by({ bm: BestMatch => (bm.distance, bm.index) }).reverse)
  }

  def save(filename: String): Unit = {
    using(new FileOutputStream(filename)) { fileOut =>
      using(new ObjectOutputStream(fileOut)) { out =>
        out.writeObject(this)
      }
    }
  }

  override def toString: String = {
    s"Conditional Balltree with data size of ${keys.length}"
  }
}
