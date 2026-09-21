// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

import scala.collection.mutable.ArrayBuffer

class FabricTestArtifactTrackerFailureSuite extends TestBase {
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
