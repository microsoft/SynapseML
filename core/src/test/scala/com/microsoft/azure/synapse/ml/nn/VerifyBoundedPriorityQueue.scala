// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nn

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

class VerifyBoundedPriorityQueue extends TestBase {

  test("Adding elements up to maxSize stores all") {
    val queue = new BoundedPriorityQueue[Int](5)
    queue += 3 += 1 += 4 += 1 += 5
    assert(queue.size === 5)
    assert(queue.toSet === Set(3, 1, 4, 1, 5))
  }

  test("Adding beyond maxSize retains only top K by ordering") {
    val queue = new BoundedPriorityQueue[Int](3)
    queue += 1 += 2 += 3 += 4 += 5
    assert(queue.size === 3)
    assert(queue.toSet === Set(3, 4, 5))
  }

  test("Adding element smaller than all in full queue doesn't replace") {
    val queue = new BoundedPriorityQueue[Int](3)
    queue += 5 += 6 += 7
    queue += 1
    assert(queue.size === 3)
    assert(queue.toSet === Set(5, 6, 7))
  }

  test("Size never exceeds maxSize") {
    val queue = new BoundedPriorityQueue[Int](3)
    for (i <- 1 to 100) {
      queue += i
      assert(queue.size <= 3)
    }
    assert(queue.size === 3)
  }

  test("clear() empties the queue") {
    val queue = new BoundedPriorityQueue[Int](5)
    queue += 1 += 2 += 3
    assert(queue.size === 3)
    queue.clear()
    assert(queue.size === 0)
    assert(queue.isEmpty)
  }

  test("Bulk add with ++= works correctly") {
    val queue = new BoundedPriorityQueue[Int](3)
    queue ++= Seq(10, 20, 30, 40, 50)
    assert(queue.size === 3)
    assert(queue.toSet === Set(30, 40, 50))
  }

  test("Works with custom ordering (reverse)") {
    val queue = new BoundedPriorityQueue[Int](3)(Ordering[Int].reverse)
    queue += 1 += 2 += 3 += 4 += 5
    assert(queue.size === 3)
    assert(queue.toSet === Set(1, 2, 3))
  }

  test("Iterator returns all elements in queue") {
    val queue = new BoundedPriorityQueue[Int](4)
    queue += 10 += 20 += 30 += 40
    val fromIterator = queue.iterator.toSet
    assert(fromIterator === Set(10, 20, 30, 40))
  }

  test("Single-element queue (maxSize=1) keeps only the max") {
    val queue = new BoundedPriorityQueue[Int](1)
    queue += 3 += 1 += 4 += 1 += 5 += 9 += 2 += 6
    assert(queue.size === 1)
    assert(queue.toList === List(9))
  }

}
