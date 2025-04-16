// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.core.env.FileUtilities
import com.microsoft.azure.synapse.ml.nbtest.DatabricksUtilities._

import java.io.File
import scala.collection.mutable.ListBuffer

class DatabricksRapidsTests extends DatabricksTestHelper {

  val clusterId: String = createClusterInPool(GPUClusterName, AdbGpuRuntime, 1, GpuPoolId, RapidsInitScripts)

  databricksTestHelper(clusterId, GPULibraries, RapidsNotebooks)

  protected override def afterAll(): Unit = {
    afterAllHelper(clusterId, RapidsClusterName)
    super.afterAll()
  }

}
