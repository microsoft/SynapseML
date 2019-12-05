// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.nn

import java.io.Serializable
import breeze.linalg.DenseVector

private[ml] case class InnerNode[T](override val ball: Ball,
                                    override val stats: Option[Set[T]],
                                    override val counts: Option[Map[T, Int]],
                                    leftChild: Node[T],
                                    rightChild: Node[T]) extends Node[T] {
  override def toString: String = {
    s"InnerNode with ${ball.toString}}."
  }
}

private[ml] case class LeafNode[T](pointIdx: Seq[Int],
                                   override val ball: Ball,
                                   override val stats: Option[Set[T]],
                                   override val counts: Option[Map[T, Int]]) extends Node[T] {
  override def toString: String = {
    s"LeafNode with ${ball.toString}} \n " +
      s"and data size of ${pointIdx.length} (example point: ${pointIdx.take(1)}})"
  }
}

private[ml] trait Node[T] extends Serializable {
  def ball: Ball

  def stats: Option[Set[T]]

  def counts: Option[Map[T, Int]]

}

private[ml] case class Ball(mu: DenseVector[Double], radius: Double) extends Serializable

case class BestMatch(index: Int, distance: Double) extends Serializable
