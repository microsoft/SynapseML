// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import com.microsoft.azure.synapse.ml.core.env.FileUtilities
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.fabric.{FabricTestConstants, HasFabricOperationsConnection}

import java.io.{File, PrintWriter}
import java.time.LocalDateTime
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, blocking}

trait HasFabricNotebookTestConnection extends HasFabricOperationsConnection {
  fabricClientId = Some(FabricTestConstants.INTEGRATION_APP_ID)
  fabricRedirectUri = Some(FabricTestConstants.INTEGRATION_REDIRECT_URI)
  fabricWorkspaceId = Some(FabricTestConstants.INTEGRATION_WORKSPACE_ID)
}

class FabricTestCleanup extends TestBase with HasFabricNotebookTestConnection {
  test("Clean up old artifacts") {
    fabric.listArtifacts()
      .foreach(artifact => {
        if (artifact.lastUpdatedDate.isBefore(LocalDateTime.now().minusDays(3))) {
          println(s"Artifact cleanup: deleting artifact ${artifact.displayName}.")
          println(s"Last Update Date: ${artifact.lastUpdatedDate.toString()}")
          try {
            fabric.deleteArtifact(artifact.objectId)
          } catch {
            case e: RuntimeException if e.getMessage.contains("PowerBIEntityNotFound") =>
              println(s"Artifact ${artifact.displayName} not found. It may have already been deleted.")
            case t: Throwable =>
              throw t
          }
        }
      })
  }
}

class FabricSmokeTests extends TestBase with HasFabricNotebookTestConnection {

  val trivialScript: String =
    """
      |from pyspark.sql import SparkSession
      |
      |spark = SparkSession.builder.getOrCreate()
      |
      |# Trivial 1+1 test
      |result = 1 + 1
      |assert result == 2, f"Expected 2, got {result}"
      |print(f"SUCCESS: 1 + 1 = {result}")
      |
      |spark.stop()
      |""".stripMargin

  lazy val notebookFile: File = {
    val dir = new File(System.getProperty("java.io.tmpdir"), "fabric-e2e-test")
    dir.mkdirs()
    val f = new File(dir, "OnePlusOne.py")
    val pw = new PrintWriter(f)
    try { pw.write(trivialScript) } finally { pw.close() }
    f
  }

  val storeArtifactId: String = fabric.createStoreArtifact()

  test("OnePlusOne") {
    val notebookName = fabric.getBlobNameFromFilepath(notebookFile.getPath)
    val artifactId = fabric.createSJDArtifact(notebookFile.getPath)
    val notebookBlobPath = fabric.uploadNotebookToAzure(notebookFile)
    fabric.updateSJDArtifact(notebookBlobPath, artifactId, storeArtifactId, includePackages = false)
    blocking {
      Thread.sleep(3000) //scalastyle:ignore
    }
    val jobInstanceId = fabric.submitJob(artifactId)
    blocking {
      Thread.sleep(10000) //scalastyle:ignore
    }
    try {
      val result = Await.ready(
        fabric.monitorJob(artifactId, jobInstanceId),
        Duration(fabric.timeoutInMillis.toLong, TimeUnit.MILLISECONDS)).value.get
      assert(result.isSuccess)
    } catch {
      case t: Throwable =>
        throw new RuntimeException(s"Job failed for $notebookName", t)
    }
  }
}

class FabricNotebookTests extends TestBase with HasFabricNotebookTestConnection {
  SharedNotebookE2ETestUtilities.generateNotebooks()

  val selectedPythonFiles: Array[File] = FileUtilities
    .recursiveListFiles(SharedNotebookE2ETestUtilities.NotebooksDir)
    .filter(_.getAbsolutePath.endsWith(".py"))
    // Only include notebooks that don't require external API keys
    .filter(f => FabricNotebookTests.IncludedNotebooks.exists(f.getAbsolutePath.contains))
    .filterNot(f => FabricNotebookTests.ExcludedNotebooks.exists(f.getAbsolutePath.contains))
    .sortBy(_.getAbsolutePath)

  selectedPythonFiles.foreach(x => println(s"Fabric notebook to be tested: $x"))
  assert(selectedPythonFiles.nonEmpty, "No notebooks found to test")

  val storeArtifactId: String = fabric.createStoreArtifact()

  selectedPythonFiles.foreach(createAndExecuteSJD)

  def createAndExecuteSJD(notebookFile: File): Unit = {
    val notebookName = fabric.getBlobNameFromFilepath(notebookFile.getPath)
    val artifactId = fabric.createSJDArtifact(notebookFile.getPath)
    val notebookBlobPath = fabric.uploadNotebookToAzure(notebookFile)
    fabric.updateSJDArtifact(notebookBlobPath, artifactId, storeArtifactId)
    blocking {
      Thread.sleep(3000) //scalastyle:ignore
    }
    val jobInstanceId = fabric.submitJob(artifactId)
    blocking {
      Thread.sleep(10000) //scalastyle:ignore
    }
    test(notebookName) {
      try {
        val result = Await.ready(
          fabric.monitorJob(artifactId, jobInstanceId),
          Duration(fabric.timeoutInMillis.toLong, TimeUnit.MILLISECONDS)).value.get
        assert(result.isSuccess)
      } catch {
        case t: Throwable =>
          throw new RuntimeException(s"Job failed for $notebookName", t)
      }
    }
  }
}

object FabricNotebookTests {
  // Notebooks that work on Fabric without external API keys.
  // These exercise core SynapseML Spark/ML functionality.
  val IncludedNotebooks: Seq[String] = Seq(
    "Classification",
    "Regression",
    "LightGBM",
    "VowpalWabbit",
    "CausalInference",
    "HyperOpt",
    "RandomSearch",
    "DataBalance",
    "ImageTransformations",
    "ONNXModelInference",
    "TrainClassifier",
    "TrainRegressor",
    "DataCleaning",
    "SparkMLvsSynapseML",
    "DeployingaClassifier",
    "ContextualBandits",
    "AnomalousAccessDetection",
    "Explainers",
    "OnePlusOne"
  )

  // Notebooks to exclude even if they match includes above
  val ExcludedNotebooks: Seq[String] = Seq(
    "GPU",
    "FineTune",
    "Finetune",
    "PhiModel",
    "LanguageModel",
    "TransferLearn",      // Needs image download that may timeout
    "ChatCompletion"      // Needs Azure AI Foundry key
  )
}
