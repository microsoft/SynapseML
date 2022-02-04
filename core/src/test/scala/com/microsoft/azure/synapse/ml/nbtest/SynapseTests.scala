// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.nbtest.SynapseUtilities.exec

import java.io.File
import java.nio.file.{Path, Paths}
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.language.existentials
import scala.sys.process.Process

/** Tests to validate fuzzing of modules. */
class SynapseTests extends TestBase {
  val os: String = sys.props("os.name").toLowerCase
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
  val sparkPools: Array[String] = Array(
    "e2etstspark32i1",
    "e2etstspark32i2",
    "e2etstspark32i3",
    "e2etstspark32i4",
    "e2etstspark32i5")

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
      val path: Path = Paths.get(file)
      val fileName: String = path.getFileName.toString

      println(s"submitted livy job: ${livyBatch.id} for file $fileName to sparkPool: $poolName")

      val livyBatchJob: LivyBatchJob = LivyBatchJob(livyBatch, poolName, livyUrl)

      test(fileName) {
        try {
          val result = Await.ready(
            livyBatchJob.monitor(),
            Duration(SynapseUtilities.TimeoutInMillis.toLong, TimeUnit.MILLISECONDS)).value.get

          assert(result.isSuccess)
        } catch {
          case t: Throwable =>
            println(s"Cancelling job ${livyBatchJob.livyBatch.id} for file $fileName")
            SynapseUtilities.cancelRun(livyBatchJob.livyUrl, livyBatchJob.livyBatch.id)

            throw t
        }
      }
    })
}
