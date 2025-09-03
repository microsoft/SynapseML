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

  val generatedNotebooks = SharedNotebookE2ETestUtilities.generateNotebooks()
  println(s"Found ${generatedNotebooks.length} notebooks in ${SharedNotebookE2ETestUtilities.NotebooksDir}")
  
  val selectedPythonFiles: Array[File] = generatedNotebooks
    .filter(file => file.getName.contains("OnePlusOne"))
    .filterNot(file => excludedNotebooks.exists(excluded => file.getAbsolutePath.contains(excluded)))
    .sortBy(_.getAbsolutePath)
    .take(1)

  val expectedPoolCount: Int = selectedPythonFiles.length

  assert(expectedPoolCount >= 1)
  println(s"SynapseTests E2E Test Suite starting on ${expectedPoolCount} notebook(s)...")
  selectedPythonFiles.foreach(println)

  // Cleanup old stray spark pools lying around due to ungraceful test shutdown
  tryDeleteOldSparkPools()

  val pkgs = Array(
    // "com.github.vowpalwabbit:vw-jni:9.3.0",
    // "com.globalmentor:hadoop-bare-naked-local-fs:0.1.0",
    // "com.jcraft:jsch:0.1.54",
    // "com.linkedin.isolation-forest:isolation-forest_3.5.0_2.12:3.0.5",
    // "com.microsoft.azure:onnx-protobuf_2.12:0.9.3",
    // "com.microsoft.azure:synapseml-cognitive_2.12:1.0.11-spark3.5",
    // "com.microsoft.azure:synapseml-core_2.12:1.0.11-spark3.5",
    // "com.microsoft.azure:synapseml-deep-learning_2.12:1.0.11-spark3.5",
    // "com.microsoft.azure:synapseml-lightgbm_2.12:1.0.11-spark3.5",
    // "com.microsoft.azure:synapseml-opencv_2.12:1.0.11-spark3.5",
    // "com.microsoft.azure:synapseml-vw_2.12:1.0.11-spark3.5",
    // "com.microsoft.cognitiveservices.speech:client-sdk:1.24.1",
    // "com.microsoft.ml.lightgbm:lightgbmlib:3.3.510",
    // "com.microsoft.onnxruntime:onnxruntime_gpu:1.8.1",
    // "commons-lang:commons-lang:2.6",
    // "io.spray:spray-json_2.12:1.3.5",
    // "org.apache.hadoop:hadoop-azure:3.3.4",
    // "org.apache.hadoop:hadoop-common:3.3.4",
    // "org.apache.httpcomponents.client5:httpclient5:5.1.3",
    // "org.apache.httpcomponents:httpmime:4.5.13",
    // "org.apache.spark:spark-avro_2.12:3.5.0",
    // "org.apache.spark:spark-core_2.12:3.5.0",
    // "org.apache.spark:spark-mllib_2.12:3.5.0",
    // "org.apache.spark:spark-tags_2.12:3.5.0",
    // "org.openpnp:opencv:3.2.0-1",
    // "org.scala-lang:scala-compiler:2.12.17",
    // "org.scala-lang:scala-library:2.12.17",
    // "org.scalactic:scalactic_2.12:3.2.14",
    // "org.scalanlp:breeze_2.12:2.1.0",
    // "org.scalatest:scalatest_2.12:3.2.14",
  )

  // val sparkPools: Seq[String] = Array[String]()
  // pkgs.foreach { pkg =>
  //   println(s"$pkg: Creating $expectedPoolCount Spark Pools...")
  //   val curSparkPools: Seq[String] = createSparkPools(expectedPoolCount)
  //   curSparkPools.foreach { pool =>
  //     sparkPools :+ pool
  //   } 
  //   testNotebooks(selectedPythonFiles, curSparkPools, pkg)
  // }

  // Allow using a pre-existing Spark pool by env var or system property
  // Env: SYNAPSE_SPARK_POOL    or    JVM prop: -Dsynapse.sparkPool=<poolName>
  private val existingPoolNameOpt: Option[String] =
    Some("test35")
    // sys.env.get("SYNAPSE_SPARK_POOL")
    //   .orElse(sys.props.get("synapse.sparkPool"))
    //   .map(_.trim)
    //   .filter(_.nonEmpty)

  private val sparkPools: Seq[String] = existingPoolNameOpt match {
    case Some(pool) =>
      println(s"Using existing Spark pool '$pool' for all ${expectedPoolCount} notebook(s); no new pools will be created.")
      Seq.fill(expectedPoolCount)(pool)
    case None =>
      createSparkPools(expectedPoolCount)
  }

  testNotebooks(selectedPythonFiles, sparkPools)

  private def testNotebooks(selectedPythonFiles: Array[File], sparkPools: Seq[String]): Unit = {
    val livyBatches: Array[LivyBatch] =
      selectedPythonFiles.zip(sparkPools).map { case (file, poolName) =>
        SynapseUtilities.uploadAndSubmitNotebook(poolName, file)
      }

    livyBatches.foreach { livyBatch =>
      println(
        s"submitted livy job: ${livyBatch.id} for ${livyBatch.runName} to sparkPool: ${livyBatch.sparkPool}"
      )
      test(livyBatch.runName) {
        try {
          val result = Await
            .ready(
              livyBatch.monitor(),
              Duration(
                SynapseUtilities.TimeoutInMillis.toLong,
                TimeUnit.MILLISECONDS
              )
            )
            .value
            .get
          assert(result.isSuccess)
        } catch {
          case t: Throwable =>
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
            throw new RuntimeException(
              s"Job failed see ${livyBatch.jobStatusPage} for details",
              t
            )
        }
      }
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
