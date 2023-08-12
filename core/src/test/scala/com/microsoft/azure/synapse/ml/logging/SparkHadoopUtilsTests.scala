// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.logging.common.SparkHadoopUtils.getHadoopConfig

class SparkHadoopUtilsTests extends TestBase {
  test("Hadoop Configuration Check (capacity id, and workspace id)"){
    sc.hadoopConfiguration.set("trident.capacity.id", "f32fae84-6ed0-4406-944c-01e26087aa9b")
    val capacityId = getHadoopConfig("trident.capacity.id", sc)
    val splittedCapacityId: Array[String] = capacityId.split("-")

    assert(splittedCapacityId.length == 4)
    assert(splittedCapacityId(0).length == 8)
    assert(splittedCapacityId(1).length == 4)
    assert(splittedCapacityId(2).length == 4)
    assert(splittedCapacityId(3).length == 4)
    assert(splittedCapacityId(4).length == 12)
  }
}
