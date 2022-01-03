// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.utils

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object AsyncUtils {
  def bufferedAwait[T](it: Iterator[Future[T]],
                                 concurrency: Int,
                                 timeout: Duration)
                                (implicit ec: ExecutionContext): Iterator[T] = {
    bufferedAwaitSafe(it, concurrency, timeout).map{
      case Success(data) => data
      case f: Failure[T] => throw f.exception
    }
  }

  private def safeAwait[T](f: Future[T], timeout: Duration): Try[T] = {
    Try(Await.result(f, timeout))
  }

  def bufferedAwaitSafe[T](it: Iterator[Future[T]],
                       concurrency: Int,
                       timeout: Duration)
                      (implicit ec: ExecutionContext): Iterator[Try[T]] = {
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

  private def safeAwaitWithContext[T, C](f: (Future[T], C), timeout: Duration): (Try[T], C) = {
    (Try(Await.result(f._1, timeout)), f._2)
  }

  def bufferedAwaitSafeWithContext[T,C](it: Iterator[(Future[T],C)],
                           concurrency: Int,
                           timeout: Duration)
                          (implicit ec: ExecutionContext): Iterator[(Try[T],C)] = {
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
