// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.lightgbm.GroupIdManager

class GroupIdManagerSuite extends TestBase {
  test("distinct string groups get distinct ids") {
    val manager = new GroupIdManager()
    val ids = Array("query_a", "query_b", "query_c").map(manager.getUniqueIdForGroup)

    assert(ids.distinct.length === 3,
      "Every distinct string group must map to its own id, otherwise LightGBM treats " +
        "the whole partition as a single query.")
    assert(ids.sorted.toSeq === Seq(0, 1, 2))
  }

  test("repeated string groups are stable") {
    val manager = new GroupIdManager()
    val first = manager.getUniqueIdForGroup("query_a")
    manager.getUniqueIdForGroup("query_b")

    assert(manager.getUniqueIdForGroup("query_a") === first)
  }

  test("distinct long groups get distinct ids") {
    val manager = new GroupIdManager()
    val ids = Array(100L, 200L, 300L).map(manager.getUniqueIdForGroup)

    assert(ids.distinct.length === 3)
    assert(ids.sorted.toSeq === Seq(0, 1, 2))
    assert(manager.getUniqueIdForGroup(200L) === ids(1))
  }

  test("int groups pass through unchanged") {
    val manager = new GroupIdManager()

    assert(manager.getUniqueIdForGroup(42) === 42)
    assert(manager.getUniqueIdForGroup(7) === 7)
  }

  test("unsupported group types are rejected") {
    val manager = new GroupIdManager()

    assertThrows[IllegalArgumentException](manager.getUniqueIdForGroup(1.5d))
  }
}
