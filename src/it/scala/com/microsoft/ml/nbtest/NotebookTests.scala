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

  ignore("Install libraries"){
    assert(listInstalledLibraries(clusterId).isEmpty, "Cluster already has libraries installed")
    println("Installing libraries")
    installLibraries(clusterId)
  }

  test("Databricks Notebooks") {
    tryWithRetries(Array.fill(500)(10000)) {() =>
      assert(listActiveJobs(clusterId).isEmpty,
        "Cluster already has running jobs cannot change libraries safely")
    }
    try {
      assert(listInstalledLibraries(clusterId).isEmpty, "Cluster already has libraries installed")
      println("Installing libraries")
      installLibraries(clusterId)
      println(s"Creating folder $folder")
      workspaceMkDir(folder)
      println(s"Submitting jobs")
      val jobIds = notebookFiles.map(uploadAndSubmitNotebook)
      println(s"Submitted ${jobIds.length} for execution: ${jobIds.toList}")
      try {
        val monitors = jobIds.map((runId: Int) => monitorJob(runId, timeoutInMillis, logLevel = 2))
        println(s"Monitoring Jobs...")
        val failures = monitors
          .map(Await.ready(_, Duration(timeoutInMillis.toLong, TimeUnit.MILLISECONDS)).value.get)
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
      uninstallAllLibraries(clusterId)
      restartCluster(clusterId)
    }
  }

  ignore("list running jobs for convenievce") {
    val obj = databricksGet("jobs/runs/list?active_only=true&limit=1000")
    println(obj)
  }

  ignore("Clean libraries") {
    uninstallAllLibraries(clusterId)
  }

  ignore("Restart cluster") {
    restartCluster(clusterId)
  }

  ignore("Refresh cluster") {
    restartCluster(clusterId)
    uninstallAllLibraries(clusterId)
    restartCluster(clusterId)
  }

}
