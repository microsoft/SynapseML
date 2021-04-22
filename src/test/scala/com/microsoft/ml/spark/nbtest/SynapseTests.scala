// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.nbtest

import com.microsoft.ml.spark.core.test.base.TestBase

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.existentials

/** Tests to validate fuzzing of modules. */
class SynapseTests extends TestBase {

  test("convert") {
    SynapseUtilities.convertNotebook()
  }

  test("SynapsePROD") {
    val workspaceName = "wenqxsynapse"
    val poolName = "wenqxpool3"
    val livyUrl = "https://" +
      workspaceName +
      ".dev.azuresynapse.net/livyApi/versions/2019-11-01-preview/sparkPools/" +
      poolName +
      "/batches"

    val livyBatches = SynapseUtilities.NotebookPythonFiles.map(SynapseUtilities.uploadAndSubmitNotebook(livyUrl, _))
    println(s"Submitted ${livyBatches.length} jobs for execution: " +
      s"${livyBatches.map(batch => s"${batch.id} : ${batch.state}").mkString("Array(", ", ", ")")}")
    try {
      val batchFutures = livyBatches.map((batch: LivyBatch) => {
        Future {
          if (batch.state != "success") {
            if (batch.state == "error") {
              SynapseUtilities.postMortem(batch, livyUrl)
              throw new RuntimeException(s"${batch.id} returned with state ${batch.state}")
            }
            else {
              SynapseUtilities.retry(batch.id, livyUrl, SynapseUtilities.TimeoutInMillis, System.currentTimeMillis())
            }
          }
        }(ExecutionContext.global)
      })

      val failures = batchFutures
        .map(Await.ready(_, Duration(SynapseUtilities.TimeoutInMillis.toLong, TimeUnit.MILLISECONDS)).value.get)
        .filter(_.isFailure)
      assert(failures.isEmpty)
    }
    catch {
      case t: Throwable =>
        livyBatches.foreach { batch =>
          println(s"Cancelling job ${batch.id}")
          SynapseUtilities.cancelRun(livyUrl, batch.id)
        }
        throw t
    }
  }
}
