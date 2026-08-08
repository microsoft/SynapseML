// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ArrayBuffer

class FabricTestArtifactTrackerSuite extends AnyFunSuite {

  test("Delete tracked artifacts in reverse creation order") {
    val deleted = ArrayBuffer.empty[String]
    val tracker = new FabricTestArtifactTracker(artifactId => {
      deleted += artifactId
      ()
    })

    tracker.track("store")
    tracker.track("job-1")
    tracker.track("job-2")
    tracker.cleanup()

    assert(deleted == Seq("job-2", "job-1", "store"))
  }

  test("Ignore artifacts that were already deleted") {
    val attempted = ArrayBuffer.empty[String]
    val tracker = new FabricTestArtifactTracker(artifactId => {
      attempted += artifactId
      if (artifactId == "missing") {
        throw new RuntimeException("PowerBIEntityNotFound")
      }
    })

    tracker.track("remaining")
    tracker.track("missing")
    tracker.cleanup()

    assert(attempted == Seq("missing", "remaining"))
  }

  test("Attempt all deletions and preserve cleanup failures") {
    val attempted = ArrayBuffer.empty[String]
    val firstFailure = new RuntimeException("first failure")
    val secondFailure = new RuntimeException("second failure")
    val tracker = new FabricTestArtifactTracker(artifactId => {
      attempted += artifactId
      throw Map("first" -> firstFailure, "second" -> secondFailure)(artifactId)
    })

    tracker.track("first")
    tracker.track("second")

    val thrown = intercept[RuntimeException](tracker.cleanup())
    assert(thrown eq secondFailure)
    assert(thrown.getSuppressed.toSeq == Seq(firstFailure))
    assert(attempted == Seq("second", "first"))
  }

  test("Recognize only SynapseML Fabric test artifact names") {
    assert(FabricNotebookTests.isTestArtifactName("Lakehouse20260808010917"))
    assert(FabricNotebookTests.isTestArtifactName(
      "ExploreAlgorithmsRegressionQuickstartTrainRegressor-20260808-01-09-17"))
    assert(FabricNotebookTests.isTestArtifactName("OnePlusOne-20260808-01-09-17"))
    assert(!FabricNotebookTests.isTestArtifactName("LakehouseForManualTesting"))
    assert(!FabricNotebookTests.isTestArtifactName("CustomerNotebook-20260808-01-09-17"))
  }

  test("Wait for notebook tasks before artifact cleanup") {
    val executor = Executors.newSingleThreadExecutor()
    val completed = new CountDownLatch(1)
    executor.submit(new Runnable {
      override def run(): Unit = completed.countDown()
    })

    FabricNotebookTests.shutdownExecutor(executor)

    assert(completed.await(0, TimeUnit.SECONDS))
    assert(executor.isTerminated)
  }
}
