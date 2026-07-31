// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import com.microsoft.azure.synapse.ml.nbtest.DatabricksUtilities._
import com.microsoft.azure.synapse.ml.nbtest.DatabricksClusterStartup._

class DatabricksGPUTests extends DatabricksTestHelper {

  private val gpuTimeoutMs = 30 * 60 * 1000
  private val gpuCapacityWaitMs = 3L * 60 * 60 * 1000
  private val gpuSmokeTests = sys.env.get("SYNAPSEML_GPU_SMOKE_TESTS").exists(_.equalsIgnoreCase("true"))
  // Use one worker per run so concurrent builds can share the GPU pool.
  val clusterId: String = createActiveCluster(
    attempt => {
      println(s"Creating GPU cluster startup attempt $attempt with $GpuWorkersPerRun worker(s)")
      createClusterInPool(
        GPUClusterName,
        AdbGpuRuntime,
        GpuWorkersPerRun,
        GpuPoolId,
        driverInstancePoolId = Some(PoolId)
      )
    },
    clusterId => waitForClusterActive(clusterId, getClusterStatus),
    permanentDeleteCluster,
    maxAttempts = Int.MaxValue,
    maxRetryDurationMs = Some(gpuCapacityWaitMs)
  )

  databricksTestHelper(
    clusterId,
    GPULibraries,
    GPUNotebooks,
    1,
    retries = List(),
    timeoutMs = gpuTimeoutMs,
    baseParameters = Map("synapseml_ci_smoke" -> gpuSmokeTests.toString)
  )

  protected override def afterAll(): Unit = {
    afterAllHelper(clusterId, GPUClusterName)
    super.afterAll()
  }
}
