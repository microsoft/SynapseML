// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.dataset

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.lightgbm.StreamingPartitionTask

class ReferenceDatasetUtilsSuite extends TestBase {
  private class TrackingDataset(closeFailure: Option[RuntimeException] = None)
    extends LightGBMDataset(null) { // scalastyle:ignore null
    var closeCount: Int = 0

    override def close(): Unit = {
      closeCount += 1
      closeFailure.foreach(throw _)
    }
  }

  test("initializeOwnedDataset leaves a successfully initialized Dataset open") {
    val dataset = new TrackingDataset()

    val result = ReferenceDatasetUtils.initializeOwnedDataset(dataset) {}

    assert(result eq dataset)
    assert(dataset.closeCount == 0)
  }

  test("initializeOwnedDataset closes the Dataset when initialization fails") {
    val dataset = new TrackingDataset()
    val initializationFailure = new IllegalStateException("initialization failed")

    val thrown = intercept[IllegalStateException] {
      ReferenceDatasetUtils.initializeOwnedDataset(dataset) {
        throw initializationFailure
      }
    }

    assert(thrown eq initializationFailure)
    assert(dataset.closeCount == 1)
  }

  test("initializeOwnedDataset preserves initialization failure when close also fails") {
    val cleanupFailure = new RuntimeException("cleanup failed")
    val dataset = new TrackingDataset(Option(cleanupFailure))
    val initializationFailure = new IllegalStateException("initialization failed")

    val thrown = intercept[IllegalStateException] {
      ReferenceDatasetUtils.initializeOwnedDataset(dataset) {
        throw initializationFailure
      }
    }

    assert(thrown eq initializationFailure)
    assert(dataset.closeCount == 1)
    assert(thrown.getSuppressed.toSeq == Seq(cleanupFailure))
  }

  test("validation Dataset cleanup runs when row insertion fails before ownership transfer") {
    val cleanupFailure = new RuntimeException("cleanup failed")
    val dataset = new TrackingDataset(Option(cleanupFailure))
    val insertionFailure = new IllegalStateException("row insertion failed")
    var markFinishedCalled = false
    var ownershipTransferred = false

    val thrown = intercept[IllegalStateException] {
      StreamingPartitionTask.initializeValidationDataset(dataset)(
        throw insertionFailure)(
        markFinishedCalled = true)(
        ownershipTransferred = true)
    }

    assert(thrown eq insertionFailure)
    assert(dataset.closeCount == 1)
    assert(thrown.getSuppressed.toSeq == Seq(cleanupFailure))
    assert(!markFinishedCalled)
    assert(!ownershipTransferred)
  }

  test("validation Dataset cleanup runs when MarkFinished fails before ownership transfer") {
    val dataset = new TrackingDataset()
    val markFinishedFailure = new IllegalStateException("MarkFinished failed")
    var insertionCalled = false
    var ownershipTransferred = false

    val thrown = intercept[IllegalStateException] {
      StreamingPartitionTask.initializeValidationDataset(dataset)(
        insertionCalled = true)(
        throw markFinishedFailure)(
        ownershipTransferred = true)
    }

    assert(thrown eq markFinishedFailure)
    assert(dataset.closeCount == 1)
    assert(insertionCalled)
    assert(!ownershipTransferred)
  }
}
