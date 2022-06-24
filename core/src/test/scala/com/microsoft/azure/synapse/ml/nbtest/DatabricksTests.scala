// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.nbtest.DatabricksUtilities._

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.language.existentials

class DatabricksTests extends TestBase {

  val clusterId: String = createClusterInPool(ClusterName, PoolId)
  val jobIdsToCancel: mutable.ListBuffer[Int] = mutable.ListBuffer[Int]()

  println("Checking if cluster is active")
  tryWithRetries(Seq.fill(60 * 15)(1000).toArray) { () =>
    assert(isClusterActive(clusterId))
  }
  println("Installing libraries")
  installLibraries(clusterId)
  tryWithRetries(Seq.fill(60 * 3)(1000).toArray) { () =>
    assert(areLibrariesInstalled(clusterId))
  }
  println(s"Creating folder $Folder")
  workspaceMkDir(Folder)

  println(s"Submitting jobs")
  val parNotebookRuns: Seq[DatabricksNotebookRun] = ParallelizableNotebooks.map(uploadAndSubmitNotebook(clusterId, _))
  parNotebookRuns.foreach(notebookRun => jobIdsToCancel.append(notebookRun.runId))

  println(s"Submitted ${parNotebookRuns.length} for execution: ${parNotebookRuns.map(_.runId).toList}")

  assert(parNotebookRuns.nonEmpty)

  parNotebookRuns.foreach(run => {
    println(s"Testing ${run.notebookName}")

    test(run.notebookName) {
      val result = Await.ready(
        run.monitor(logLevel = 0),
        Duration(TimeoutInMillis.toLong, TimeUnit.MILLISECONDS)).value.get

      if (!result.isSuccess){
        throw result.failed.get
      }
    }
  })

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
    println("Suite DatabricksTests finished. Running afterAll procedure...")
    jobIdsToCancel.foreach(cancelRun)

    deleteCluster(clusterId)
    println(s"Deleted cluster with Id $clusterId.")

    super.afterAll()
  }

  ignore("list running jobs for convenievce") {
    val obj = databricksGet("jobs/runs/list?active_only=true&limit=1000")
    println(obj)
  }
}
