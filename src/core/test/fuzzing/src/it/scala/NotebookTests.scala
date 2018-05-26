// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.util.concurrent.TimeUnit

import com.microsoft.ml.spark.DatabricksUtilities._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.language.existentials

/** Tests to validate fuzzing of modules. */
class NotebookTests extends TestBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
    assert(listActiveJobs(clusterId).isEmpty,
      "Cluster already has running jobs cannot change libraries safely")
    installLibraries(clusterId)
  }

  test("Databricks Notebooks") {
    workspaceMkDir(folder)
    println(s"Submitting jobs")
    val jobIds = notebookFiles.map(uploadAndSubmitNotebook)
    println(s"Submitted ${jobIds.length} for execution: ${jobIds.toList}")
    try {
      val monitors = jobIds.map(monitorJob(_, logLevel=2, timeout = timeoutInMillis))
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
  }

  ignore("list running jobs for convenievce"){
    val obj = databricksGet("jobs/runs/list?active_only=true&limit=1000")
    println(obj)
  }

  override def afterAll(): Unit = {
    uninstallLibraries(clusterId)
    restartCluster(clusterId)
    super.afterAll()
  }

}
