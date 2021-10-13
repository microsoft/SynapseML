// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import SynapseUtilities.exec

import java.io.File
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.existentials
import scala.sys.process.Process

/** Tests to validate fuzzing of modules. */
class SynapseTests extends TestBase {

  ignore("Synapse") {

    val os = sys.props("os.name").toLowerCase
    os match {
      case x if x contains "windows" =>
        exec("conda activate synapseml && jupyter nbconvert --to script .\\notebooks\\*.ipynb")
      case _ =>
        Process(s"conda init bash; conda activate synapseml; jupyter nbconvert --to script ./notebooks/*.ipynb")
    }

    SynapseUtilities.listPythonFiles().map(f => {
      val newPath = f
        .replace(" ", "")
        .replace("-", "")
      new File(f).renameTo(new File(newPath))
    })

    val workspaceName = "mmlspark"
    val sparkPools = Array("buildpool", "buildpool2", "buildpool3")

    val livyBatchJobs = SynapseUtilities.listPythonJobFiles()
      .filterNot(_.contains(" "))
      .filterNot(_.contains("-"))
      .map(f => {
        val poolName = SynapseUtilities.monitorPool(workspaceName, sparkPools)
        val livyUrl = "https://" +
          workspaceName +
          ".dev.azuresynapse.net/livyApi/versions/2019-11-01-preview/sparkPools/" +
          poolName +
          "/batches"
        val livyBatch: LivyBatch = SynapseUtilities.uploadAndSubmitNotebook(livyUrl, f)
        println(s"submitted livy job: ${livyBatch.id} to sparkPool: $poolName")
        LivyBatchJob(livyBatch, poolName, livyUrl)
      })
      .filterNot(_.livyBatch.state == "none")

    try {
      val batchFutures: Array[Future[Any]] = livyBatchJobs.map((batchJob: LivyBatchJob) => {
        Future {
          val batch = batchJob.livyBatch
          val livyUrl = batchJob.livyUrl

          if (batch.state != "success") {
            SynapseUtilities.retry(batch.id, livyUrl, SynapseUtilities.TimeoutInMillis, System.currentTimeMillis())
          }
        }(ExecutionContext.global)
      })

      val failures = batchFutures
        .map(f => Await.ready(f, Duration(SynapseUtilities.TimeoutInMillis.toLong, TimeUnit.MILLISECONDS)).value.get)
        .filter(f => f.isFailure)
      assert(failures.isEmpty)
    }
    catch {
      case t: Throwable =>
        livyBatchJobs.foreach { batchJob =>
          println(s"Cancelling job ${batchJob.livyBatch.id}")
          SynapseUtilities.cancelRun(batchJob.livyUrl, batchJob.livyBatch.id)
        }
        throw t
    }
  }
}
