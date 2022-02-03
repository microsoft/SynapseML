// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import SynapseUtilities.exec

import java.io.File
import java.util
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.existentials
import scala.sys.process.Process

/** Tests to validate fuzzing of modules. */
class SynapseTests extends TestBase {
  val os = sys.props("os.name").toLowerCase
  os match {
    case x if x contains "windows" =>
      exec("conda activate synapseml " +
        "&& jupyter nbconvert --to script .\\notebooks\\features\\**\\*.ipynb")
    case _ =>
      Process(s"conda init bash; conda activate synapseml; " +
        "jupyter nbconvert --to script ./notebooks/features/**/*.ipynb")
  }

  SynapseUtilities.listPythonFiles().map(f => {
    val newPath = f
      .replace(" ", "")
      .replace("-", "")
    new File(f).renameTo(new File(newPath))
  })

  val workspaceName = "mmlsparkppe"
  val sparkPools = Array(
    "e2etstspark32i1",
    "e2etstspark32i2",
    "e2etstspark32i3",
    "e2etstspark32i4",
    "e2etstspark32i5")

  val livyBatchJobForEachFile: util.HashMap[String, LivyBatchJob] = new util.HashMap[String, LivyBatchJob]

  SynapseUtilities.listPythonJobFiles()
    .filterNot(_.contains(" "))
    .filterNot(_.contains("-"))
    .foreach(file => {
      val poolName = SynapseUtilities.monitorPool(workspaceName, sparkPools)
      val livyUrl = "https://" +
        workspaceName +
        ".dev.azuresynapse-dogfood.net/livyApi/versions/2019-11-01-preview/sparkPools/" +
        poolName +
        "/batches"
      val livyBatch: LivyBatch = SynapseUtilities.uploadAndSubmitNotebook(livyUrl, file)
      println(s"submitted livy job: ${livyBatch.id} for file ${file} to sparkPool: $poolName")

      livyBatchJobForEachFile.put(file, LivyBatchJob(livyBatch, poolName, livyUrl))
    })

  try {
    val batchFuturesForEachFile: util.HashMap[String, Future[Any]] = new util.HashMap[String, Future[Any]]

    livyBatchJobForEachFile.forEach((file: String, batchJob: LivyBatchJob) => {
      batchFuturesForEachFile.put(file, Future {
        val batch = batchJob.livyBatch
        val livyUrl = batchJob.livyUrl

        if(batch.state != "success") {
          SynapseUtilities.retry(batch.id, livyUrl, SynapseUtilities.TimeoutInMillis, System.currentTimeMillis())
        }
      }(ExecutionContext.global))
    })

    batchFuturesForEachFile.forEach((file: String, future: Future[Any]) => {
      val result = Await.ready(
        future,
        Duration(SynapseUtilities.TimeoutInMillis.toLong, TimeUnit.MILLISECONDS)).value.get

      test(file) {
        assert(result.isSuccess)
      }
    })
  }
  catch {
    case t: Throwable =>
      livyBatchJobForEachFile.forEach((file:String, batchJob:LivyBatchJob) => {
        println(s"Cancelling job ${batchJob.livyBatch.id} for file ${file}")
        SynapseUtilities.cancelRun(batchJob.livyUrl, batchJob.livyBatch.id)
      })

      throw t
  }
}
