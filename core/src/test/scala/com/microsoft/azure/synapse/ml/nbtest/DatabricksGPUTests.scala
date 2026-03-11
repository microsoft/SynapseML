// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import com.microsoft.azure.synapse.ml.nbtest.DatabricksUtilities._

class DatabricksGPUTests extends DatabricksTestHelper {

  // GPU fine-tuning notebooks can take up to 25 min; use 30 min timeout
  private val gpuTimeoutMs = 30 * 60 * 1000
  override val testTimeoutInSeconds: Int = gpuTimeoutMs / 1000

  val clusterId: String = createClusterInPool(GPUClusterName, AdbGpuRuntime, 3, GpuPoolId)

  databricksTestHelper(clusterId, GPULibraries, GPUNotebooks, 3, List(), gpuTimeoutMs)

  protected override def afterAll(): Unit = {
    afterAllHelper(clusterId, GPUClusterName)
    super.afterAll()
  }

}
