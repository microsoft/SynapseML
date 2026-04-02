// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import com.microsoft.azure.synapse.ml.nbtest.DatabricksUtilities._

import java.time.LocalDateTime
import scala.language.existentials

// Split CPU E2E tests into 5 partitions that run as separate ADO matrix entries.
// Each partition creates its own cluster and contains ~9 notebooks,
// running up to the configured worker concurrency.
// All 5 partitions start simultaneously on different ADO agents.

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

class DatabricksCPUTests4 extends DatabricksTestHelper {
  private val clusterName = s"mmlspark-build-cpu4-${LocalDateTime.now()}"
  val clusterId: String = createClusterInPool(clusterName, AdbRuntime, NumWorkers, PoolId, memory = Some("7g"))
  databricksTestHelper(clusterId, Libraries, cpuNotebookPartition(3), NumWorkers)
  protected override def afterAll(): Unit = { afterAllHelper(clusterId, clusterName); super.afterAll() }
}

class DatabricksCPUTests5 extends DatabricksTestHelper {
  private val clusterName = s"mmlspark-build-cpu5-${LocalDateTime.now()}"
  val clusterId: String = createClusterInPool(clusterName, AdbRuntime, NumWorkers, PoolId, memory = Some("7g"))
  databricksTestHelper(clusterId, Libraries, cpuNotebookPartition(4), NumWorkers)
  protected override def afterAll(): Unit = { afterAllHelper(clusterId, clusterName); super.afterAll() }
}

// Streaming notebook (Deploying a Classifier) runs alone — its server.stop() cancels
// all SparkContext jobs on Spark 4.0, which would kill concurrent notebooks.
class DatabricksCPUStreamingTests extends DatabricksTestHelper {
  private val clusterName = s"mmlspark-build-cpu-streaming-${LocalDateTime.now()}"
  val clusterId: String = createClusterInPool(clusterName, AdbRuntime, NumWorkers, PoolId, memory = Some("7g"))
  databricksTestHelper(clusterId, Libraries, StreamingNotebooks, 1)
  protected override def afterAll(): Unit = { afterAllHelper(clusterId, clusterName); super.afterAll() }
}
