// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import com.microsoft.azure.synapse.ml.nbtest.DatabricksUtilities._

class DatabricksRapidsTests extends DatabricksTestHelper {

  val clusterId: String = createClusterInPool(GPUClusterName, AdbGpuRuntime, 1, GpuPoolId, RapidsInitScripts)

  // We want to wait for 40 minutes for each test to complete
  databricksTestHelper(clusterId, GPULibraries, RapidsNotebooks)

  protected override def afterAll(): Unit = {
    afterAllHelper(clusterId, RapidsClusterName)
    super.afterAll()
  }

}
