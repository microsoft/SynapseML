// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import com.microsoft.azure.synapse.ml.nbtest.DatabricksUtilities._

// Split GPU tests into separate classes so they run as parallel ADO matrix entries.
// Each creates its own cluster because Horovod fine-tuning uses all workers.

class DatabricksGPUTests1 extends DatabricksTestHelper {
  private val gpuTimeoutMs = 30 * 60 * 1000
  private val clusterName = s"mmlspark-build-gpu1-${java.time.LocalDateTime.now()}"
  val clusterId: String = createClusterInPool(clusterName, AdbGpuRuntime, 2, GpuPoolId)
  databricksTestHelper(clusterId, GPULibraries, gpuNotebook(0), 1, List(), gpuTimeoutMs)
  protected override def afterAll(): Unit = { afterAllHelper(clusterId, clusterName); super.afterAll() }
}

class DatabricksGPUTests2 extends DatabricksTestHelper {
  private val gpuTimeoutMs = 30 * 60 * 1000
  private val clusterName = s"mmlspark-build-gpu2-${java.time.LocalDateTime.now()}"
  val clusterId: String = createClusterInPool(clusterName, AdbGpuRuntime, 2, GpuPoolId)
  databricksTestHelper(clusterId, GPULibraries, gpuNotebook(1), 1, List(), gpuTimeoutMs)
  protected override def afterAll(): Unit = { afterAllHelper(clusterId, clusterName); super.afterAll() }
}

class DatabricksGPUTests3 extends DatabricksTestHelper {
  private val gpuTimeoutMs = 30 * 60 * 1000
  private val clusterName = s"mmlspark-build-gpu3-${java.time.LocalDateTime.now()}"
  val clusterId: String = createClusterInPool(clusterName, AdbGpuRuntime, 2, GpuPoolId)
  databricksTestHelper(clusterId, GPULibraries, gpuNotebook(2), 1, List(), gpuTimeoutMs)
  protected override def afterAll(): Unit = { afterAllHelper(clusterId, clusterName); super.afterAll() }
}
