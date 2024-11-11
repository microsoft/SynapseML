// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import com.microsoft.azure.synapse.ml.nbtest.DatabricksUtilities._
import org.scalatest.Ignore


@Ignore
class DatabricksRapidsTests extends DatabricksTestHelper {

  val clusterId: String = createClusterInPool(GPUClusterName, AdbGpuRuntime, 1, GpuPoolId, RapidsInitScripts)

  databricksTestHelper(clusterId, GPULibraries, RapidsNotebooks)

  protected override def afterAll(): Unit = {
    afterAllHelper(clusterId, RapidsClusterName)
    super.afterAll()
  }

}
