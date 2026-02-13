// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.utils

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class VerifyAsyncUtils extends TestBase {

  test("bufferedAwaitSafe with concurrency=1 returns all successful results") {
    val futures = (1 to 5).map(i => Future.successful(i)).iterator
    val results = AsyncUtils.bufferedAwaitSafe(futures, concurrency = 1, timeout = Duration.Inf).toList
    assert(results.length == 5)
    assert(results.forall(_.isSuccess))
    assert(results.map(_.get) == List(1, 2, 3, 4, 5))
  }

  test("bufferedAwaitSafe with concurrency>1 returns all successful results") {
    val futures = (1 to 10).map(i => Future.successful(i)).iterator
    val results = AsyncUtils.bufferedAwaitSafe(futures, concurrency = 3, timeout = Duration.Inf).toList
    assert(results.length == 10)
    assert(results.forall(_.isSuccess))
    assert(results.map(_.get) == (1 to 10).toList)
  }

  test("bufferedAwaitSafe with mixed success/failure futures returns Try results") {
    val futures = (1 to 5).map { i =>
      if (i == 3) Future.failed[Int](new RuntimeException("fail"))
      else Future.successful(i)
    }.iterator
    val results = AsyncUtils.bufferedAwaitSafe(futures, concurrency = 2, timeout = Duration.Inf).toList
    assert(results.length == 5)
    assert(results.count(_.isSuccess) == 4)
    assert(results.count(_.isFailure) == 1)
    assert(results(2).isFailure)
  }

  test("bufferedAwait with all successful futures returns unwrapped results") {
    val futures = (1 to 5).map(i => Future.successful(i)).iterator
    val results = AsyncUtils.bufferedAwait(futures, concurrency = 2, timeout = Duration.Inf).toList
    assert(results == List(1, 2, 3, 4, 5))
  }

  test("bufferedAwait throws on failed futures") {
    val futures = (1 to 5).map { i =>
      if (i == 3) Future.failed[Int](new RuntimeException("fail"))
      else Future.successful(i)
    }.iterator
    assertThrows[RuntimeException] {
      AsyncUtils.bufferedAwait(futures, concurrency = 2, timeout = Duration.Inf).toList
    }
  }

  test("bufferedAwaitSafe throws IllegalArgumentException for concurrency=0") {
    val futures = (1 to 3).map(i => Future.successful(i)).iterator
    assertThrows[IllegalArgumentException] {
      AsyncUtils.bufferedAwaitSafe(futures, concurrency = 0, timeout = Duration.Inf).toList
    }
  }

  test("bufferedAwaitSafe handles empty iterator") {
    val futures = Iterator[Future[Int]]()
    val results = AsyncUtils.bufferedAwaitSafe(futures, concurrency = 2, timeout = Duration.Inf).toList
    assert(results.isEmpty)
  }

  test("bufferedAwaitSafeWithContext preserves context values") {
    val futures = (1 to 5).map(i => (Future.successful(i * 10), s"ctx-$i")).iterator
    val results = AsyncUtils.bufferedAwaitSafeWithContext(futures, concurrency = 2, timeout = Duration.Inf).toList
    assert(results.length == 5)
    assert(results.forall(_._1.isSuccess))
    assert(results.map { case (t, c) => (t.get, c) } ==
      List((10, "ctx-1"), (20, "ctx-2"), (30, "ctx-3"), (40, "ctx-4"), (50, "ctx-5")))
  }

  test("bufferedAwaitSafeWithContext throws IllegalArgumentException for concurrency=0") {
    val futures = (1 to 3).map(i => (Future.successful(i), s"ctx-$i")).iterator
    assertThrows[IllegalArgumentException] {
      AsyncUtils.bufferedAwaitSafeWithContext(futures, concurrency = 0, timeout = Duration.Inf).toList
    }
  }
}
