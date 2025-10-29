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
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.language.existentials
import scala.util.Try
import scala.annotation.tailrec
import scala.util.Success
import scala.util.Failure

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
    "ExplanationDashboard", // New issue
    "ExploreAlgorithmsDeepLearningQuickstartONNXModelInference" // ONNX package issues
  )

  val generatedNotebooks = SharedNotebookE2ETestUtilities.generateNotebooks()
  println(s"Found ${generatedNotebooks.length} notebooks in ${SharedNotebookE2ETestUtilities.NotebooksDir}")

  val selectedPythonFiles: Array[File] = generatedNotebooks
    .filterNot(file => excludedNotebooks.exists(excluded => file.getAbsolutePath.contains(excluded)))
    // .filter(file => file.getName().contains("OnePlusOne"))
    .sortBy(_.getAbsolutePath)

  val expectedPoolCount: Int = selectedPythonFiles.length

  assert(expectedPoolCount >= 1)
  println(s"SynapseTests E2E Test Suite starting on ${expectedPoolCount} notebook(s)...")
  selectedPythonFiles.foreach(println)

  // Cleanup old stray spark pools lying around due to ungraceful test shutdown
  tryDeleteOldSparkPools()

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  private val existingPoolNameOpt: Option[String] =
    Some("test35")

  private val sparkPools: Seq[String] = existingPoolNameOpt match {
    case Some(pool) =>
      println(s"Using existing Spark pool '$pool' for all ${expectedPoolCount} notebook(s).")
      Seq.fill(expectedPoolCount)(pool)
    case None =>
      createSparkPools(expectedPoolCount)
  }

  testNotebooks(selectedPythonFiles, sparkPools)

  private def monitorNotebook(livyBatch: LivyBatch): LivyBatch = {
    val tryResult = Await
      .ready(
        livyBatch.monitor(),
        Duration(SynapseUtilities.TimeoutInMillis.toLong,TimeUnit.MILLISECONDS)
      ).value.get
    assert(tryResult.isSuccess)
    val result = tryResult.get

    if (result.isSuccess) {
      println(s"Job ${livyBatch.id} for ${livyBatch.runName} completed successfully.")
    } else {
      println(s"Job failed see ${livyBatch.jobStatusPage} for details")
      livyBatch.cancelRun()
      // Fetch and print Livy logs for debugging
      try {
        val getStatusRequest = new HttpGet(
          s"${SynapseUtilities.livyUrl(livyBatch.sparkPool)}/${livyBatch.id}"
        )
        getStatusRequest.setHeader(
          "Authorization",
          s"Bearer ${SynapseUtilities.SynapseToken}"
        )
        val batchData =
          sendAndParseJson(getStatusRequest).convertTo[LivyBatchData]
        println(s"--- Livy Logs for job ${livyBatch.id} ---")
        batchData.log.foreach(_.foreach(println))
        println(s"--- End of Livy Logs ---")
      } catch {
        case logEx: Throwable =>
          println(s"Failed to fetch Livy logs: ${logEx.getMessage}")
      }
    }

    result
  }

  @tailrec
  private def retryBatch(maxRetries: Int, delayMillis: Long, attempt: Int = 1)(block: => LivyBatch): LivyBatch = {
    val livyBatch = block
    livyBatch.isSuccess match {
      case true => livyBatch
      case false if attempt < maxRetries =>
        println(s"Retrying after failure of job ${livyBatch.id} for ${livyBatch.runName}. $attempt of $maxRetries.")
        val jitter = scala.util.Random.nextInt(5000)
        Thread.sleep(delayMillis + jitter)
        retryBatch(maxRetries, delayMillis, attempt + 1)(block)
      case false => livyBatch
    }
  }

  private def testNotebooks(selectedPythonFiles: Array[File], sparkPools: Seq[String]): Unit = {
    println(s"Submitting ${selectedPythonFiles.length} notebook(s) as Livy batches in workspace: $WorkspaceName...")
    val batchFutures: Seq[(String, Future[LivyBatch])] = selectedPythonFiles.zip(sparkPools).zipWithIndex.map {
      case ((file, pool), index) => {
        (
          file.getName(),
          Future {
            val jitter = scala.util.Random.nextInt(400)
            Thread.sleep(3000L * index + jitter)

            retryBatch(maxRetries = 5, delayMillis = 60000) {
              val livyBatch = SynapseUtilities.uploadAndSubmitNotebook(pool, file)
              println(s"- Job ${livyBatch.id}: ${livyBatch.runName} on pool ${livyBatch.sparkPool}")
              monitorNotebook(livyBatch)
            }
          }
        )
      }
    }

    // Register a test block for each notebook, blocking on its Future
    batchFutures.foreach { case (name, fut) =>
      test(name.substring(0, name.length - 3)) {
        val result = Await.result(fut, Duration.Inf)
        assert(result.isSuccess)
      }
    }
    try {
      val results = batchFutures.map { case (name, fut) =>
        Await.result(fut, Duration.Inf)
      }.map(b => (b.runName, b.isSuccess, b.elapsedSeconds))

      // Dynamically calculate column widths
      val nbWidth = (results.map(_._1.length).max max "Notebook".length) + 2
      val succeededWidth = (results.map(_._2.toString.length).max max "Succeeded".length) + 2
      val timeWidth = (results.map(r => r._3.toDouble.formatted("%.2f").length).max max "Time(s)".length) + 2

      def pad(s: String, width: Int) = s.padTo(width, ' ')

      println(pad("Notebook", nbWidth) + pad("Succeeded", succeededWidth) + pad("Time(s)", timeWidth))
      for ((name, status, time) <- results) {
        println(pad(name, nbWidth) + pad(status.toString, succeededWidth) + pad(f"${time.toDouble}%.2f", timeWidth))
      }
    } catch {
      case e: Throwable =>
        println(s"Failed to print summary table: ${e.getMessage}")
    }
  }

  protected override def afterAll(): Unit = {
    println("Synapse E2E Test Suite finished. Deleting Spark Pools...")
    // Only delete pools that were created by this test run. If an existing pool was
    // provided, skip deletion entirely. Additionally, guard by prefix to be safe.
    existingPoolNameOpt match {
      case Some(pool) =>
        println(s"Existing pool '$pool' was used; skipping deletion.")
      case None =>
        val poolsToDelete = sparkPools.distinct.filter(_.startsWith(ClusterPrefix))
        val failures = poolsToDelete.map(pool => Try(deleteSparkPool(pool)))
          .filter(_.isFailure)
        if (failures.isEmpty) {
          println("All Spark Pools deleted successfully.")
        } else {
          println("Failed to delete all spark pools cleanly:")
          failures.foreach(failure => println(failure.failed.get.getMessage))
        }
    }
    super.afterAll()
  }
}
