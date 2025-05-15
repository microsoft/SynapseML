// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import com.microsoft.azure.synapse.ml.core.env.FileUtilities
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.http.RESTHelpers.sendAndParseJson
import com.microsoft.azure.synapse.ml.nbtest.SynapseUtilities._
import org.apache.http.client.methods.HttpGet

import java.io.File
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.language.existentials
import scala.util.Try

class SynapseTestCleanup extends TestBase {

  import SynapseJsonProtocol._

  ignore("Clean up all pools") {
    println("Deleting stray old Apache Spark Pools...")
    val getBigDataPoolsUri =
      s"""
         |$ManagementUrlRoot/resources?api-version=2021-04-01&
         |$$filter=substringof(name, \'$WorkspaceName\') and
         | resourceType eq \'Microsoft.Synapse/workspaces/bigDataPools\'
         |""".stripMargin.replaceAll(LineSeparator, "")


    val getBigDataPoolRequest = new HttpGet(getBigDataPoolsUri)
    getBigDataPoolRequest.setHeader("Authorization", s"Bearer $ArmToken")
    val sparkPools = sendAndParseJson(getBigDataPoolRequest).convertTo[SynapseResourceResponse].value
    sparkPools.foreach(sparkPool => {
      val name = sparkPool.name.stripPrefix(s"$WorkspaceName/")
      deleteSparkPool(name)
    })
  }

}

class SynapseTests extends TestBase {
  final val excludedNotebooks: Set[String] = Set(
    "Finetune", // Excluded by design task 1829306
    "GPU",
    "PhiModel",
    "VWnativeFormat",
    "VowpalWabbitMulticlassclassification", // Wait for Synapse fix
    "Langchain", // Wait for Synapse fix
    "DocumentQuestionandAnsweringwithPDFs", // Wait for Synapse fix
    "SetupCognitive", // No code to run
    "CreateaSparkCluster", // No code to run
    "Deploying", // New issue
    "MultivariateAnomaly", // New issue
    "TuningHyperOpt", // New issue
    "IsolationForests", // New issue
    "CreateAudiobooks", // New issue
    "ExplanationDashboard" // New issue
  )

  val selectedPythonFiles: Array[File] = SharedNotebookE2ETestUtilities.generateNotebooks()
    .filterNot(file => excludedNotebooks.exists(excluded => file.getAbsolutePath.contains(excluded)))
    .sortBy(_.getAbsolutePath)
    .take(1)

  val expectedPoolCount: Int = selectedPythonFiles.length

  assert(expectedPoolCount >= 1)
  println(s"SynapseTests E2E Test Suite starting on ${expectedPoolCount} notebook(s)...")
  selectedPythonFiles.foreach(println)

  // Cleanup old stray spark pools lying around due to ungraceful test shutdown
  tryDeleteOldSparkPools()

  println(s"Creating $expectedPoolCount Spark Pools...")
  val sparkPools: Seq[String] = createSparkPools(expectedPoolCount)


  val livyBatches: Array[LivyBatch] = selectedPythonFiles.zip(sparkPools).map { case (file, poolName) =>
    SynapseUtilities.uploadAndSubmitNotebook(poolName, file)
  }

  livyBatches.foreach(testNotebook)

  private def testNotebook(livyBatch: LivyBatch) {
    println(s"submitted livy job: ${livyBatch.id} for ${livyBatch.runName} to sparkPool: ${livyBatch.sparkPool}")
    test(livyBatch.runName) {
      try {
        val result = Await.ready(
          livyBatch.monitor(),
          Duration(SynapseUtilities.TimeoutInMillis.toLong, TimeUnit.MILLISECONDS)).value.get
        assert(result.isSuccess)
      } catch {
        case t: Throwable =>
          livyBatch.cancelRun()
          try {
            val getStatusRequest = new HttpGet(s"${SynapseUtilities.livyUrl(livyBatch.sparkPool)}/${livyBatch.id}")
            getStatusRequest.setHeader("Authorization", s"Bearer ${SynapseUtilities.SynapseToken}")
            val batchData = sendAndParseJson(getStatusRequest).convertTo[LivyBatchData]
            println(s"--- Livy Logs for job ${livyBatch.id} ---")
            batchData.log.foreach(_.foreach(println))
            println(s"--- End of Livy Logs ---")
          } catch {
            case logEx: Throwable =>
              println(s"Failed to fetch Livy logs: ${logEx.getMessage}")
          }
          throw new RuntimeException(
            s"Job failed see ${livyBatch.jobStatusPage} for details",
            t
          )
      }
    }
  }

  protected override def afterAll(): Unit = {
    println("Synapse E2E Test Suite finished. Deleting Spark Pools...")
    val failures = sparkPools.map(pool => Try(deleteSparkPool(pool)))
      .filter(_.isFailure)
    if (failures.isEmpty) {
      println("All Spark Pools deleted successfully.")
    } else {
      println("Failed to delete all spark pools cleanly:")
      failures.foreach(failure =>
        println(failure.failed.get.getMessage))
    }
    super.afterAll()
  }
}
