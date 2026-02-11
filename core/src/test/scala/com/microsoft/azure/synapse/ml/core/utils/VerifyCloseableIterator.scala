// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.utils

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

class VerifyCloseableIterator extends TestBase {

  test("iterating through all elements calls cleanup") {
    var cleaned = false
    val iter = new CloseableIterator(List(1, 2, 3).iterator, { cleaned = true })
    while (iter.hasNext) iter.next()
    assert(cleaned)
  }

  test("cleanup is triggered when last element is consumed via next()") {
    var cleaned = false
    val iter = new CloseableIterator(List(1, 2, 3).iterator, { cleaned = true })
    iter.next()
    assert(!cleaned)
    iter.next()
    assert(!cleaned)
    iter.next()
    assert(cleaned)
  }

  test("hasNext delegates correctly") {
    val iter = new CloseableIterator(List(1).iterator, {})
    assert(iter.hasNext)
    iter.next()
    assert(!iter.hasNext)
  }

  test("next() returns correct values") {
    val iter = new CloseableIterator(List(1, 2, 3).iterator, {})
    assert(iter.next() === 1)
    assert(iter.next() === 2)
    assert(iter.next() === 3)
  }

  test("partial iteration does not trigger cleanup") {
    var cleaned = false
    val iter = new CloseableIterator(List(1, 2, 3).iterator, { cleaned = true })
    iter.next()
    assert(!cleaned)
  }

  test("cleanup that throws propagates from next() on last element") {
    val iter = new CloseableIterator(List(1).iterator, { throw new RuntimeException("boom") })
    assertThrows[RuntimeException] {
      iter.next()
    }
  }
}
