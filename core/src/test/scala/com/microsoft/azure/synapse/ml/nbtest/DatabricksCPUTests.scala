// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import com.microsoft.azure.synapse.ml.nbtest.DatabricksUtilities._

import scala.language.existentials

class DatabricksCPUTests extends DatabricksTestHelper {

  val clusterId: String = createClusterInPool(ClusterName, AdbRuntime, NumWorkers, PoolId, memory = Some("7g"))

  databricksTestHelper(clusterId, Libraries, CPUNotebooks, 5)

  protected override def afterAll(): Unit = {
    afterAllHelper(clusterId, ClusterName)
    super.afterAll()
  }

  ignore("list running jobs for convenience") {
    val obj = databricksGet("jobs/runs/list?active_only=true&limit=1000")
    println(obj)
  }
}
