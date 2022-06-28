// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.nbtest.DatabricksUtilities._

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class GPUTests extends DatabricksTestProcess {

  val clusterId: String = createClusterInPool(GPUClusterName, AdbGpuRuntime, PoolId)
  val jobIdsToCancel: ListBuffer[Int] = databricksTestProcess(clusterId, GPULibraries, GPUNotebooks)

  protected override def afterAll(): Unit = {
    afterAllHelper(jobIdsToCancel, clusterId)

    super.afterAll()
  }

}
