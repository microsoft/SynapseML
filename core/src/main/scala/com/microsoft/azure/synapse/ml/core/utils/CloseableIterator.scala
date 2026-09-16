// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.utils

//scalastyle:off no.finalize
class CloseableIterator[+T](delegate: Iterator[T], cleanup: => Unit) extends Iterator[T] {
  override def hasNext: Boolean = delegate.hasNext

  override def next(): T = {
    val t = delegate.next()

    if (!delegate.hasNext) {
      // Clean up after fetching the last row, without requiring another call from the consumer.
      cleanup
    }

    t
  }

  override def finalize(): Unit = {
    try {
      // Make sure resource is cleaned up.
      cleanup
    }
    catch {
      case _: Throwable =>
    }
  }
}
//scalastyle:on no.finalize
