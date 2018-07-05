// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import scala.concurrent.{Await, ExecutionContext, Future, TimeoutException}
import scala.concurrent.duration.Duration

object AsyncUtils {
  def bufferedAwait[T](it: Iterator[Future[T]],
                                 concurrency: Int,
                                 timeout: Duration)
                                (implicit ec: ExecutionContext): Iterator[T] = {
    bufferedAwaitSafe(it, concurrency, timeout).map(_.get)
  }

  private def safeAwait[T](f: Future[T], timeout: Duration): Option[T] = {
    try {
      Some(Await.result(f, timeout))
    } catch{
      case _: TimeoutException => None
    }
  }

  def bufferedAwaitSafe[T](it: Iterator[Future[T]],
                       concurrency: Int,
                       timeout: Duration)
                      (implicit ec: ExecutionContext): Iterator[Option[T]] = {
    if (concurrency > 1) {
      val slidingIterator = it.sliding(concurrency - 1).withPartial(true)
      // `hasNext` will auto start the nth future in the batch
      val (initIterator, tailIterator) = slidingIterator.span(_ => slidingIterator.hasNext)
      initIterator.map(futureBatch => safeAwait(futureBatch.head, timeout)) ++
        tailIterator.flatMap(lastBatch => lastBatch.map(safeAwait(_, timeout)))
    } else if (concurrency == 1) {
      it.map(f => safeAwait(f, timeout))
    } else {
      throw new IllegalArgumentException(
        s"Concurrency needs to be at least 1, got: $concurrency")
    }
  }

  private def safeAwaitWithContext[T, C](f: (Future[T], C), timeout: Duration): (Option[T], C) = {
    try {
      (Some(Await.result(f._1, timeout)), f._2)
    } catch{
      case _: TimeoutException => (None, f._2)
    }
  }

  def bufferedAwaitSafeWithContext[T,C](it: Iterator[(Future[T],C)],
                           concurrency: Int,
                           timeout: Duration)
                          (implicit ec: ExecutionContext): Iterator[(Option[T],C)] = {
    if (concurrency > 1) {
      val slidingIterator = it.sliding(concurrency - 1).withPartial(true)
      // `hasNext` will auto start the nth future in the batch
      val (initIterator, tailIterator) = slidingIterator.span(_ => slidingIterator.hasNext)
      initIterator.map(futureBatch => safeAwaitWithContext(futureBatch.head, timeout)) ++
        tailIterator.flatMap(lastBatch => lastBatch.map(safeAwaitWithContext(_, timeout)))
    } else if (concurrency == 1) {
      it.map(f => safeAwaitWithContext(f, timeout))
    } else {
      throw new IllegalArgumentException(
        s"Concurrency needs to be at least 1, got: $concurrency")
    }
  }
}
