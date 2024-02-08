// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import com.microsoft.azure.synapse.ml.nbtest.DatabricksUtilities._

class DatabricksRapidsTests extends DatabricksTestHelper {

  val clusterId: String = createClusterInPool(GPUClusterName, AdbGpuRuntime, 2, GpuPoolId, RapidsInitScripts)
  val jobIdsToCancel: ListBuffer[Long] = databricksTestHelper(
    clusterId, GPULibraries, GPUNotebooks)

  protected override def afterAll(): Unit = {
    afterAllHelper(jobIdsToCancel, clusterId, GPUClusterName)
    super.afterAll()
  }

}
