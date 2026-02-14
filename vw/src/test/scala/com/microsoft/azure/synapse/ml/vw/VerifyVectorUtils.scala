// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

// scalastyle:off magic.number
class VerifyVectorUtils extends TestBase {

  test("sortAndDistinct with empty arrays returns empty arrays") {
    val (indices, values) = VectorUtils.sortAndDistinct(Array[Int](), Array[Double]())
    assert(indices.isEmpty)
    assert(values.isEmpty)
  }

  test("sortAndDistinct sorts indices and values together") {
    val (indices, values) = VectorUtils.sortAndDistinct(
      Array(3, 1, 2), Array(30.0, 10.0, 20.0))
    assert(indices === Array(1, 2, 3))
    assert(values === Array(10.0, 20.0, 30.0))
  }

  test("sortAndDistinct deduplicates and sums collisions by default") {
    val (indices, values) = VectorUtils.sortAndDistinct(
      Array(1, 2, 1), Array(10.0, 20.0, 5.0))
    assert(indices === Array(1, 2))
    assert(values === Array(15.0, 20.0))
  }

  test("sortAndDistinct deduplicates without summing when sumCollisions is false") {
    val (indices, values) = VectorUtils.sortAndDistinct(
      Array(1, 2, 1), Array(10.0, 20.0, 5.0), sumCollisions = false)
    assert(indices === Array(1, 2))
    assert(values === Array(10.0, 20.0))
  }

  test("sortAndDistinct with single element") {
    val (indices, values) = VectorUtils.sortAndDistinct(
      Array(5), Array(42.0))
    assert(indices === Array(5))
    assert(values === Array(42.0))
  }

  test("sortAndDistinct with already sorted no-duplicate input") {
    val (indices, values) = VectorUtils.sortAndDistinct(
      Array(1, 2, 3, 4), Array(1.0, 2.0, 3.0, 4.0))
    assert(indices === Array(1, 2, 3, 4))
    assert(values === Array(1.0, 2.0, 3.0, 4.0))
  }

  test("sortAndDistinct with all duplicate indices sums to single element") {
    val (indices, values) = VectorUtils.sortAndDistinct(
      Array(5, 5, 5), Array(1.0, 2.0, 3.0))
    assert(indices === Array(5))
    assert(values === Array(6.0))
  }

  test("sortAndDistinct with multiple collision groups") {
    val (indices, values) = VectorUtils.sortAndDistinct(
      Array(3, 1, 3, 1, 2), Array(1.0, 2.0, 3.0, 4.0, 5.0))
    assert(indices === Array(1, 2, 3))
    assert(values === Array(6.0, 5.0, 4.0))
  }

  test("sortAndDistinct preserves negative values") {
    val (indices, values) = VectorUtils.sortAndDistinct(
      Array(2, 1), Array(-5.0, -3.0))
    assert(indices === Array(1, 2))
    assert(values === Array(-3.0, -5.0))
  }
}
// scalastyle:on magic.number
