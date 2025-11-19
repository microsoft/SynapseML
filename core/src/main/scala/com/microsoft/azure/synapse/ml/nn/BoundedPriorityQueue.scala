// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nn

/*
This file is taken from the Apache Spark project and is licensed under Apache License version 2.0.
Original file: https://github.com/apache/spark/core/src/main/scala/org/apache/spark/util/BoundedPriorityQueue.scala
*/

import java.io.Serializable
import java.util.{PriorityQueue => JPriorityQueue}

/**
  * Bounded priority queue. This class wraps the original PriorityQueue
  * class and modifies it such that only the top K elements are retained.
  * The top K elements are defined by an implicit Ordering[A].
  */
class BoundedPriorityQueue[A](maxSize: Int)(implicit ord: Ordering[A])
  extends Iterable[A] with Serializable {

  import scala.collection.JavaConverters._

  private val underlying = new JPriorityQueue[A](maxSize, ord)

  override def iterator: Iterator[A] = underlying.iterator.asScala

  override def size: Int = underlying.size

  //scalastyle:off method.name
  def ++=(xs: IterableOnce[A]): this.type = {
    xs.foreach {
      this += _
    }
    this
  }

  def +=(elem: A): this.type = {
    if (size < maxSize) {
      underlying.offer(elem)
    } else {
      maybeReplaceLowest(elem)
    }
    this
  }

  def +=(elem1: A, elem2: A, elems: A*): this.type = {
    this += elem1 += elem2 ++= elems
  }
  //scalastyle:on method.name

  def clear(): Unit = {
    underlying.clear()
  }

  private def maybeReplaceLowest(a: A): Boolean = {
    val head = underlying.peek()
    if (head != null && ord.gt(a, head)) {
      underlying.poll()
      underlying.offer(a)
    } else {
      false
    }
  }
}
