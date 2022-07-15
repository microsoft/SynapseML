// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import com.microsoft.azure.synapse.ml.nbtest.DatabricksUtilities._

import java.util.concurrent.TimeUnit
import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.language.existentials

class DatabricksTests extends DatabricksTestHelper {

  val clusterId: String = createClusterInPool(ClusterName, AdbRuntime, PoolId, "[]")
  val jobIdsToCancel: ListBuffer[Int] = databricksTestHelper(clusterId, Libraries, CPUNotebooks)

  println(s"Submitting nonparallelizable job...")
  NonParallelizableNotebooks.toIterator.foreach(notebook => {
    val run: DatabricksNotebookRun = uploadAndSubmitNotebook(clusterId, notebook)
    jobIdsToCancel.append(run.runId)

    test(run.notebookName) {
      val result = Await.ready(
        run.monitor(logLevel = 0),
        Duration(TimeoutInMillis.toLong, TimeUnit.MILLISECONDS)).value.get

      if (!result.isSuccess){
        throw result.failed.get
      }
    }
  })

  protected override def afterAll(): Unit = {
    afterAllHelper(jobIdsToCancel, clusterId)

    super.afterAll()
  }

  ignore("list running jobs for convenience") {
    val obj = databricksGet("jobs/runs/list?active_only=true&limit=1000")
    println(obj)
  }
}
