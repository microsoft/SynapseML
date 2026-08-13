// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import breeze.linalg.{DenseVector => BDV}
import com.microsoft.azure.synapse.ml.core.test.base.TestBase

class VerifyCacheOps extends TestBase {

  test("CacheOps trait has default implementations that return input unchanged") {
    val ops = new CacheOps[String] {}
    val data = "test"
    assert(ops.cache(data) === data)
    assert(ops.checkpoint(data) === data)
  }

  test("BDVCacheOps.cache returns the same vector") {
    val vector = BDV(1.0, 2.0, 3.0)
    val result = BDVCacheOps.cache(vector)
    assert(result eq vector)
  }

  test("BDVCacheOps.checkpoint returns the same vector") {
    val vector = BDV(1.0, 2.0, 3.0)
    val result = BDVCacheOps.checkpoint(vector)
    assert(result eq vector)
  }

  test("BDVCacheOps is a no-op for dense vectors") {
    val vector = BDV(1.0, 2.0, 3.0, 4.0, 5.0)

    // Both operations should return the exact same instance
    val cached = BDVCacheOps.cache(vector)
    val checkpointed = BDVCacheOps.checkpoint(vector)

    assert(cached eq vector)
    assert(checkpointed eq vector)
    assert(cached.toArray === Array(1.0, 2.0, 3.0, 4.0, 5.0))
  }

  test("BDVCacheOps preserves vector data") {
    val vector = BDV(10.0, 20.0, 30.0)
    val cached = BDVCacheOps.cache(vector)

    assert(cached(0) === 10.0)
    assert(cached(1) === 20.0)
    assert(cached(2) === 30.0)
    assert(cached.length === 3)
  }

  test("CacheOps works with generic types") {
    case class TestData(value: Int)

    val ops = new CacheOps[TestData] {}
    val data = TestData(42)

    assert(ops.cache(data) === data)
    assert(ops.checkpoint(data) === data)
  }
}
