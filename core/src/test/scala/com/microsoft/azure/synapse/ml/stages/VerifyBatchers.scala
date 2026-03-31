// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

class VerifyBatchers extends TestBase {

  test("FixedBatcher batches elements into correct sizes") {
    val input = (1 to 10).toList
    val batcher = new FixedBatcher(input.iterator, batchSize = 3)
    val batches = batcher.toList
    assert(batches.take(3).forall(_.size == 3))
  }

  test("FixedBatcher handles last batch being smaller") {
    val input = (1 to 10).toList
    val batcher = new FixedBatcher(input.iterator, batchSize = 3)
    val batches = batcher.toList
    assert(batches.length == 4)
    assert(batches.last.size == 1)
    assert(batches.flatMap(identity) == input)
  }

  test("FixedBatcher with batch size 1 returns individual elements") {
    val input = (1 to 5).toList
    val batcher = new FixedBatcher(input.iterator, batchSize = 1)
    val batches = batcher.toList
    assert(batches.length == 5)
    assert(batches.forall(_.size == 1))
    assert(batches.flatMap(identity) == input)
  }

  test("FixedBatcher hasNext returns false when iterator exhausted") {
    val input = (1 to 3).toList
    val batcher = new FixedBatcher(input.iterator, batchSize = 3)
    assert(batcher.hasNext)
    batcher.next()
    assert(!batcher.hasNext)
  }

  test("FixedBatcher with empty iterator") {
    val batcher = new FixedBatcher(Iterator[Int](), batchSize = 5)
    assert(!batcher.hasNext)
    assert(batcher.toList.isEmpty)
  }

  test("TimeIntervalBatcher respects maxBufferSize") {
    val input = (1 to 20).toList
    val batcher = new TimeIntervalBatcher(input.iterator, millis = 10000, maxBufferSize = 5)
    val batches = batcher.toList
    assert(batches.forall(_.size <= 5))
    assert(batches.flatMap(identity) == input)
  }

  test("DynamicBufferedBatcher yields all input elements") {
    val input = (1 to 20).toList
    val batcher = new DynamicBufferedBatcher(input.iterator)
    val result = batcher.flatMap(identity).toList.sorted
    assert(result == input)
  }

  test("FixedBufferedBatcher yields all input elements in correct batch sizes") {
    val input = (1 to 20).toList
    val batcher = new FixedBufferedBatcher(input.iterator, batchSize = 5)
    val batches = batcher.toList
    assert(batches.flatMap(identity).sorted == input)
    assert(batches.dropRight(1).forall(_.size == 5))
  }

}
