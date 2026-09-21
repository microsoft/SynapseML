// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ArrayBuffer

private[nbtest] trait FabricTestArtifactTrackerFailureTests extends AnyFunSuite {
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

  test("Preserve a repeated cleanup throwable without self-suppression") {
    val failure = new IllegalStateException("delete denied")
    val attempted = ArrayBuffer.empty[String]
    val tracker = new FabricTestArtifactTracker(id => {
      attempted += id
      throw failure
    })
    Seq("store", "job").foreach(tracker.track)
    val thrown = intercept[IllegalStateException](tracker.cleanup())
    assert(thrown eq failure)
    assert(thrown.getSuppressed.isEmpty)
    tracker.cleanup()
    assert(attempted == Seq("job", "store"))
  }
}
