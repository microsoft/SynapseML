// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.nbtest
//TODO temp hack because ij picks up on it test classes by mistake

import java.util.concurrent.TimeUnit

import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.nbtest.DatabricksUtilities._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.language.existentials

/** Tests to validate fuzzing of modules. */
class NotebookTests extends TestBase {

  test("Databricks Notebooks") {
    val clusterId = createClusterInPool(ClusterName, PoolId)
    try {
      println("Checking if cluster is active")
      tryWithRetries(Seq.fill(60*15)(1000).toArray){() =>
        assert(isClusterActive(clusterId))}
      println("Installing libraries")
      installLibraries(clusterId)
      println(s"Creating folder $Folder")
      workspaceMkDir(Folder)
      println(s"Submitting jobs")
      val jobIds = NotebookFiles.map(uploadAndSubmitNotebook(clusterId, _))
      println(s"Submitted ${jobIds.length} for execution: ${jobIds.toList}")
      try {
        val monitors = jobIds.map((runId: Int) => monitorJob(runId, TimeoutInMillis, logLevel = 2))
        println(s"Monitoring Jobs...")
        val failures = monitors
          .map(Await.ready(_, Duration(TimeoutInMillis.toLong, TimeUnit.MILLISECONDS)).value.get)
          .filter(_.isFailure)
        assert(failures.isEmpty)
      } catch {
        case t: Throwable =>
          jobIds.foreach { jid =>
            println(s"Cancelling job $jid")
            cancelRun(jid)
          }
          throw t
      }
    } finally {
      deleteCluster(clusterId)
    }
  }

  ignore("list running jobs for convenievce") {
    val obj = databricksGet("jobs/runs/list?active_only=true&limit=1000")
    println(obj)
  }

}
