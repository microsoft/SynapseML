// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest.SynapseExtension

import com.microsoft.azure.synapse.ml.core.env.FileUtilities
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.nbtest.SharedNotebookE2ETestUtilities

import java.io.File
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, blocking}
import scala.language.existentials

class SynapseExtensionTestCleanup extends TestBase {
  ignore("Clean up all artifacts") {
    SynapseExtensionUtilities.listArtifacts()
      .foreach(artifact =>
      {
        println(s"Artifact cleanup: deleting artifact ${artifact.displayName}")
        //TODO: This will delete all artifacts. Us creation timestamp in sjd response
        //  To only delete artifacts > some time ago
        SynapseExtensionUtilities.deleteArtifact(artifact.objectId)
      })
  }
}

class SynapseExtensionsTests extends TestBase {
  SharedNotebookE2ETestUtilities.generateNotebooks()

  val selectedPythonFiles: Array[File] = FileUtilities.recursiveListFiles(SharedNotebookE2ETestUtilities.NotebooksDir)
    .filter(_.getAbsolutePath.endsWith(".py"))
    .filterNot(_.getAbsolutePath.contains("HyperParameterTuning"))
    .filterNot(_.getAbsolutePath.contains("CyberML"))
    .filterNot(_.getAbsolutePath.contains("VowpalWabbitOverview"))
    .filterNot(_.getAbsolutePath.contains("IsolationForest"))
    .filterNot(_.getAbsolutePath.contains("ExplanationDashboard"))
    .filterNot(_.getAbsolutePath.contains("DeepLearning"))
    .filterNot(_.getAbsolutePath.contains("Interpretability")) //TODO: Remove when fixed
    .filterNot(_.getAbsolutePath.contains("Cognitive")) // Excluding CogServices notebooks until GetSecret API is avail
    .filterNot(_.getAbsolutePath.contains("Geospatial"))
    .filterNot(_.getAbsolutePath.contains("SentimentAnalysis"))
    .filterNot(_.getAbsolutePath.contains("SparkServing")) // Not testing this functionality
    .filterNot(_.getAbsolutePath.contains("OpenCVPipelineImage")) // Reenable with spark streaming fix
    .sortBy(_.getAbsolutePath)

  selectedPythonFiles.foreach(println)
  assert(selectedPythonFiles.length > 0)

  val storeArtifactId = SynapseExtensionUtilities.createStoreArtifact()

  selectedPythonFiles.seq.map(createAndExecuteSJD)

  def createAndExecuteSJD(notebookFile: File): Future[String] =
  {
    val notebookName = SynapseExtensionUtilities.getBlobNameFromFilepath(notebookFile.getPath)
    val artifactId = SynapseExtensionUtilities.createSJDArtifact(notebookFile.getPath)
    val notebookBlobPath = SynapseExtensionUtilities.uploadNotebookToAzure(notebookFile)
    SynapseExtensionUtilities.updateSJDArtifact(notebookBlobPath, artifactId, storeArtifactId)
    blocking {
      Thread.sleep(3000)
    }
    val jobInstanceId = SynapseExtensionUtilities.submitJob(artifactId)
    blocking {
      Thread.sleep(10000)
    }
    test(notebookName) {
      try {
        val result = Await.ready(
          SynapseExtensionUtilities.monitorJob(artifactId, jobInstanceId),
          Duration(SynapseExtensionUtilities.TimeoutInMillis.toLong, TimeUnit.MILLISECONDS)).value.get
        assert(result.isSuccess)
      } catch {
        case t: Throwable =>
          throw new RuntimeException(s"Job failed for $notebookName", t)
      }
    }
    SynapseExtensionUtilities.monitorJob(artifactId, jobInstanceId)
  }
}
