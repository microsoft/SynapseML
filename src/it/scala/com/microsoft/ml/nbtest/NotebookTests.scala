// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.nbtest
//TODO temp hack because ij picks up on it test classes by mistake

import java.util.concurrent.TimeUnit

import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.nbtest.DatabricksUtilities

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.language.existentials

/** Tests to validate fuzzing of modules. */
class NotebookTests extends TestBase {

  test("Databricks Notebooks") {
    val clusterId = DatabricksUtilities.createClusterInPool(DatabricksUtilities.ClusterName, DatabricksUtilities.PoolId)
    try {
      println("Checking if cluster is active")
      tryWithRetries(Seq.fill(60*15)(1000).toArray){() =>
        assert(DatabricksUtilities.isClusterActive(clusterId))}
      println("Installing libraries")
      DatabricksUtilities.installLibraries(clusterId)
      tryWithRetries(Seq.fill(60*3)(1000).toArray){() =>
        assert(DatabricksUtilities.isClusterActive(clusterId))}
      val folder = DatabricksUtilities.Folder
      println(s"Creating folder $folder")
      DatabricksUtilities.workspaceMkDir(folder)
      println(s"Submitting jobs")
      val jobIds = DatabricksUtilities.NotebookFiles.map(DatabricksUtilities.uploadAndSubmitNotebook(clusterId, _))
      println(s"Submitted ${jobIds.length} for execution: ${jobIds.toList}")
      try {
        val monitors = jobIds.map((runId: Int) => DatabricksUtilities.monitorJob(
          runId,
          DatabricksUtilities.TimeoutInMillis,
          logLevel = 2))
        println(s"Monitoring Jobs...")
        val failures = monitors
          .map(Await.ready(_, Duration(DatabricksUtilities.TimeoutInMillis.toLong, TimeUnit.MILLISECONDS)).value.get)
          .filter(_.isFailure)
        assert(failures.isEmpty)
      } catch {
        case t: Throwable =>
          jobIds.foreach { jid =>
            println(s"Cancelling job $jid")
            DatabricksUtilities.cancelRun(jid)
          }
          throw t
      }
    } finally {
      DatabricksUtilities.deleteCluster(clusterId)
    }
  }

  test("Synapse Notebooks PROD") {
    val livyPort = "8998"
    val livyIP = sys.env("LIVY_IP")
    val livyURL = s"http://${livyIP.trim.replace("'","")}:$livyPort"

    val clusterId = DatabricksUtilities.createClusterInPool(DatabricksUtilities.ClusterName, DatabricksUtilities.PoolId)
    try {
      val jobIds = DatabricksUtilities.NotebookFiles.map(DatabricksUtilities.uploadAndSubmitNotebook(clusterId, _))
      println(s"Submitted ${jobIds.length} for execution: ${jobIds.toList}")
      try {
        val monitors = jobIds.map((runId: Int) => DatabricksUtilities.monitorJob(
          runId,
          DatabricksUtilities.TimeoutInMillis,
          logLevel = 2))
        println(s"Monitoring Jobs...")
        val failures = monitors
          .map(Await.ready(_, Duration(DatabricksUtilities.TimeoutInMillis.toLong, TimeUnit.MILLISECONDS)).value.get)
          .filter(_.isFailure)
        assert(failures.isEmpty)
      } catch {
        case t: Throwable =>
          jobIds.foreach { jid =>
            println(s"Cancelling job $jid")
            DatabricksUtilities.cancelRun(jid)
          }
          throw t
      }
    } finally {
      DatabricksUtilities.deleteCluster(clusterId)
    }
  }

  ignore("list running jobs for convenievce") {
    val obj = DatabricksUtilities.databricksGet("jobs/runs/list?active_only=true&limit=1000")
    println(obj)
  }

}
