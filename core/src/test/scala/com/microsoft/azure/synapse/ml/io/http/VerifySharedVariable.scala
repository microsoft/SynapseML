// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.http

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

class VerifySharedVariable extends TestBase {

  test("SharedVariable.get returns the constructed value") {
    val sv = new SharedVariable[Int](42)
    assert(sv.get === 42)
  }

  test("SharedVariable.get returns the same instance on repeated calls") {
    val sv = new SharedVariable[Object](new Object())
    val first = sv.get
    val second = sv.get
    assert(first eq second)
  }

  test("SharedVariable companion object apply factory works") {
    val sv = SharedVariable[String]("hello")
    assert(sv.get === "hello")
  }

  test("SharedSingleton.get returns the constructed value") {
    SharedSingleton.poolClear()
    val ss = SharedSingleton[Int](99)
    assert(ss.get === 99)
    SharedSingleton.poolClear()
  }

  test("SharedSingleton adds to pool on first get") {
    SharedSingleton.poolClear()
    val sizeBefore = SharedSingleton.poolSize
    val ss = SharedSingleton[String]("pooled")
    ss.get
    val sizeAfter = SharedSingleton.poolSize
    assert(sizeBefore === 0)
    assert(sizeAfter === 1)
    SharedSingleton.poolClear()
  }

  test("SharedSingleton.poolClear clears the pool") {
    SharedSingleton.poolClear()
    val ss = SharedSingleton[Int](1)
    ss.get
    assert(SharedSingleton.poolSize === 1)
    SharedSingleton.poolClear()
    assert(SharedSingleton.poolSize === 0)
  }

  test("Two different SharedSingletons get unique UUIDs") {
    SharedSingleton.poolClear()
    val ss1 = SharedSingleton[Int](1)
    val ss2 = SharedSingleton[Int](2)
    assert(ss1.singletonUUID !== ss2.singletonUUID)
    SharedSingleton.poolClear()
  }

  test("SharedSingleton returns same value from pool on repeated get") {
    SharedSingleton.poolClear()
    val ss = SharedSingleton[Object](new Object())
    val first = ss.get
    val second = ss.get
    assert(first eq second)
    SharedSingleton.poolClear()
  }

}
