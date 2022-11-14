// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.utils

//scalastyle:off no.finalize
class CloseableIterator[+T](delegate: Iterator[T], cleanup: => Unit) extends Iterator[T] {
  override def hasNext: Boolean = delegate.hasNext

  override def next(): T = {
    val t = delegate.next()

    if (!delegate.hasNext) {
      // Cleanup the resource if there is no more rows, but iterator does not have to exhaust.
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

    super.finalize()
  }
}
//scalastyle:on no.finalize

