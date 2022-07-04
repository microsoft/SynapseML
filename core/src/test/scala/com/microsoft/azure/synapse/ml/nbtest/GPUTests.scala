// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import com.microsoft.azure.synapse.ml.nbtest.DatabricksUtilities._

import scala.collection.mutable.ListBuffer

class GPUTests extends DatabricksTestProcess {

  val clusterId: String = createClusterInPool(GPUClusterName, AdbGpuRuntime, PoolId)
  val jobIdsToCancel: ListBuffer[Int] = databricksTestProcess(clusterId, GPULibraries, GPUNotebooks)

  protected override def afterAll(): Unit = {
    afterAllHelper(jobIdsToCancel, clusterId)

    super.afterAll()
  }

}
