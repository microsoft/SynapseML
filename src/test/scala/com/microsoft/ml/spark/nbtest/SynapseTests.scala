// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.nbtest

import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.nbtest.DatabricksUtilities._

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.existentials

/** Tests to validate fuzzing of modules. */
class SynapseTests extends TestBase {

  test("SynapsePROD") {
    val workspaceName = "wenqxsynapse"
    val poolName = "wenqxpool"
    val livyUrl = "https://" +
      workspaceName +
      ".dev.azuresynapse.net/livyApi/versions/2019-11-01-preview/sparkPools/" +
      poolName +
      "/batches"
    val livyBatches = LivyUtilities.NotebookFiles.map(LivyUtilities.uploadAndSubmitNotebook(livyUrl, _))
    println(s"Submitted ${livyBatches.length} jobs for execution: " +
      s"${livyBatches.map(batch => s"${batch.id} : ${batch.state}").mkString("Array(", ", ", ")")}")
    try {
      val batchFutures = livyBatches.map((batch: LivyBatch) => {
        Future {
          if (batch.state != "success") {
            if (batch.state == "error") {
              LivyUtilities.postMortem(batch, livyUrl)
              throw new RuntimeException(s"${batch.id} returned with state ${batch.state}")
            }
            else {
              LivyUtilities.retry(batch.id, livyUrl, LivyUtilities.TimeoutInMillis, System.currentTimeMillis())
            }
          }
        }(ExecutionContext.global)
      })

      val failures = batchFutures
        .map(Await.ready(_, Duration(LivyUtilities.TimeoutInMillis.toLong, TimeUnit.MILLISECONDS)).value.get)
        .filter(_.isFailure)
      assert(failures.isEmpty)
    }
    catch {
      case t: Throwable =>
        livyBatches.foreach { batch =>
          println(s"Cancelling job ${batch.id}")
          LivyUtilities.cancelRun(livyUrl, batch.id)
        }
        throw t
    }
  }

  ignore("list running jobs for convenievce") {
    val obj = databricksGet("jobs/runs/list?active_only=true&limit=1000")
    println(obj)
  }

}
