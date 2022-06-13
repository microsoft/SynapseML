// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nn

import breeze.linalg.DenseVector

import java.io.Serializable

private[ml] case class InnerNode(override val ball: Ball,
                                 leftChild: Node,
                                 rightChild: Node) extends Node {
  override def toString: String = {
    s"InnerNode with ${ball.toString}}."
  }
}

private[ml] case class LeafNode(pointIdx: Seq[Int],
                                override val ball: Ball
                               ) extends Node {
  override def toString: String = {
    s"LeafNode with ${ball.toString}} \n " +
      s"and data size of ${pointIdx.length} (example point: ${pointIdx.take(1)}})"
  }
}

private[ml] trait Node extends Serializable {
  def ball: Ball

}

private[ml] case class Ball(mu: DenseVector[Double], radius: Double) extends Serializable

case class BestMatch(index: Int, distance: Double) extends Serializable
