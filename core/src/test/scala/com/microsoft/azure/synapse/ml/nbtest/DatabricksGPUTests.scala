// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import com.microsoft.azure.synapse.ml.nbtest.DatabricksUtilities._
import com.microsoft.azure.synapse.ml.nbtest.DatabricksClusterStartup._

class DatabricksGPUTests extends DatabricksTestHelper {

  private val gpuTimeoutMs = 30 * 60 * 1000
  // Reuse the scarce GPU workers sequentially while the driver runs from the CPU pool.
  val clusterId: String = createActiveCluster(
    attempt => {
      val workerCount = gpuWorkerCount(attempt)
      println(s"Creating GPU cluster startup attempt $attempt with $workerCount worker(s)")
      createClusterInPool(
        GPUClusterName,
        AdbGpuRuntime,
        workerCount,
        GpuPoolId,
        driverInstancePoolId = Some(PoolId)
      )
    },
    clusterId => waitForClusterActive(clusterId, getClusterStatus),
    permanentDeleteCluster
  )

  databricksTestHelper(clusterId, GPULibraries, GPUNotebooks, 1, List(), gpuTimeoutMs)

  protected override def afterAll(): Unit = {
    afterAllHelper(clusterId, GPUClusterName)
    super.afterAll()
  }
}
