// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import com.microsoft.azure.synapse.ml.nbtest.DatabricksUtilities._

import java.time.LocalDateTime
import scala.language.existentials

// Split CPU E2E tests into 3 partitions that run as separate ADO matrix entries.
// Each partition creates its own cluster and runs ~15 notebooks in parallel.
// All 3 partitions start simultaneously on different ADO agents.

class DatabricksCPUTests1 extends DatabricksTestHelper {
  private val clusterName = s"mmlspark-build-cpu1-${LocalDateTime.now()}"
  val clusterId: String = createClusterInPool(clusterName, AdbRuntime, NumWorkers, PoolId, memory = Some("7g"))
  databricksTestHelper(clusterId, Libraries, cpuNotebookPartition(0), NumWorkers)
  protected override def afterAll(): Unit = { afterAllHelper(clusterId, clusterName); super.afterAll() }
}

class DatabricksCPUTests2 extends DatabricksTestHelper {
  private val clusterName = s"mmlspark-build-cpu2-${LocalDateTime.now()}"
  val clusterId: String = createClusterInPool(clusterName, AdbRuntime, NumWorkers, PoolId, memory = Some("7g"))
  databricksTestHelper(clusterId, Libraries, cpuNotebookPartition(1), NumWorkers)
  protected override def afterAll(): Unit = { afterAllHelper(clusterId, clusterName); super.afterAll() }
}

class DatabricksCPUTests3 extends DatabricksTestHelper {
  private val clusterName = s"mmlspark-build-cpu3-${LocalDateTime.now()}"
  val clusterId: String = createClusterInPool(clusterName, AdbRuntime, NumWorkers, PoolId, memory = Some("7g"))
  databricksTestHelper(clusterId, Libraries, cpuNotebookPartition(2), NumWorkers)
  protected override def afterAll(): Unit = { afterAllHelper(clusterId, clusterName); super.afterAll() }
}
