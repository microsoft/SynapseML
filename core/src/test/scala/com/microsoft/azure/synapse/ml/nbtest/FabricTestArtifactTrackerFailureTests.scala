// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ArrayBuffer
import scala.util.control.ControlThrowable

private[nbtest] trait FabricTestArtifactTrackerFailureTests extends AnyFunSuite {
  test("Release failed jobs and preserve the original failure") {
    val deleted = ArrayBuffer.empty[String]
    val tracker = new FabricTestArtifactTracker(id => {
      deleted += id
      ()
    })
    val failure = new IllegalStateException("job failed")
    val thrown = intercept[IllegalStateException] {
      tracker.withArtifact("job") { _ => throw failure }
    }
    assert(thrown eq failure)
    assert(deleted == Seq("job"))
    tracker.cleanup()
    assert(deleted == Seq("job"))
  }

  test("Retain unsuccessful deletions for final cleanup without masking job failure") {
    val jobFailure = new IllegalStateException("job failed")
    val cleanupFailure = new IllegalStateException("delete failed")
    var attempts = 0
    val tracker = new FabricTestArtifactTracker(_ => {
      attempts += 1
      if (attempts == 1) throw cleanupFailure
    })

    val thrown = intercept[IllegalStateException] {
      tracker.withArtifact("job") { _ => throw jobFailure }
    }
    assert(thrown eq jobFailure)
    assert(thrown.getSuppressed.toSeq == Seq(cleanupFailure))
    tracker.cleanup()
    assert(attempts == 2)
  }

  test("Fail successful jobs when artifact cleanup fails") {
    val failure = new IllegalStateException("delete failed")
    val tracker = new FabricTestArtifactTracker(_ => throw failure)
    val thrown = intercept[IllegalStateException] {
      tracker.withArtifact("job") { _ => "completed" }
    }
    assert(thrown eq failure)
  }

  test("Attempt artifact cleanup after executor shutdown fails") {
    val shutdownFailure = new RuntimeException("shutdown failed")
    val cleanupFailure = new RuntimeException("cleanup failed")
    var cleanupAttempted = false

    val thrown = intercept[RuntimeException] {
      FabricNotebookTests.shutdownAndCleanup(
        throw shutdownFailure,
        {
          cleanupAttempted = true
          throw cleanupFailure
        })
    }

    assert(cleanupAttempted)
    assert(thrown eq shutdownFailure)
    assert(thrown.getSuppressed.toSeq == Seq(cleanupFailure))
  }

  test("Attempt artifact cleanup after executor shutdown is interrupted") {
    var cleanupAttempted = false
    try {
      val thrown = intercept[InterruptedException] {
        FabricNotebookTests.shutdownAndCleanup(
          throw new InterruptedException("shutdown interrupted"),
          {
            cleanupAttempted = true
          })
      }

      assert(cleanupAttempted)
      assert(thrown.getMessage == "shutdown interrupted")
      assert(Thread.currentThread().isInterrupted)
    } finally {
      Thread.interrupted()
    }
  }

  test("Propagate fatal per-artifact cleanup errors after successful or failed work") {
    val cleanupFailures = Seq[() => Throwable](
      () => new InterruptedException("cleanup interrupted"),
      () => new InternalError("cleanup VM failure"),
      () => new ThreadDeath(),
      () => new LinkageError("cleanup linkage failure"),
      () => new ControlThrowable {})
    cleanupFailures.foreach { newCleanupFailure =>
      Seq[Option[Throwable]](None, Some(new IllegalStateException("job failed")),
        Some(new InternalError("job VM failure"))).foreach { jobFailure =>
        val cleanupFailure = newCleanupFailure()
        var attempts = 0
        val tracker = new FabricTestArtifactTracker(_ => {
          attempts += 1
          if (attempts == 1) throw cleanupFailure
        })
        val thrown = intercept[Throwable] {
          tracker.withArtifact("job") { _ =>
            jobFailure.foreach(throw _)
            "completed"
          }
        }
        assert(thrown eq cleanupFailure)
        val suppressionProbe = newCleanupFailure()
        suppressionProbe.addSuppressed(new RuntimeException("suppression probe"))
        val expectedSuppressed = if (suppressionProbe.getSuppressed.isEmpty) Seq.empty else jobFailure.toSeq
        assert(thrown.getSuppressed.toSeq == expectedSuppressed)
        assert(attempts == 1)
        tracker.cleanup()
        assert(attempts == 2)
        tracker.cleanup()
        assert(attempts == 2)
      }
    }
  }

  test("Preserve a repeated fatal cleanup throwable without self-suppression") {
    val failure = new InterruptedException("cleanup interrupted")
    val tracker = new FabricTestArtifactTracker(_ => throw failure)
    val thrown = intercept[InterruptedException] {
      tracker.withArtifact("job") { _ => throw failure }
    }
    assert(thrown eq failure)
    assert(thrown.getSuppressed.isEmpty)
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
