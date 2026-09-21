// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import java.util.concurrent.ConcurrentLinkedDeque
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

private[nbtest] final class FabricTestArtifactTracker(deleteArtifact: String => Unit) {
  private val artifactIds = new ConcurrentLinkedDeque[String]()

  def track(artifactId: String): String = {
    artifactIds.push(artifactId)
    artifactId
  }

  def withArtifact[T](artifactId: String)(use: String => T): T = {
    track(artifactId)
    var failure = Option.empty[Throwable]
    try {
      use(artifactId)
    } catch {
      case error: Throwable =>
        failure = Some(error)
        throw error
    } finally {
      try {
        deleteTrackedArtifact(artifactId)
        artifactIds.remove(artifactId)
      } catch {
        case cleanupError: Throwable =>
          failure match {
            case Some(original) =>
              if (original ne cleanupError) original.addSuppressed(cleanupError)
            case None => throw cleanupError
          }
      }
    }
  }

  private def deleteTrackedArtifact(artifactId: String): Unit = {
    try {
      deleteArtifact(artifactId)
      println(s"Artifact cleanup: deleted artifact $artifactId.")
    } catch {
      case e: RuntimeException if Option(e.getMessage).exists(_.contains("PowerBIEntityNotFound")) =>
        println(s"Artifact $artifactId was already deleted.")
    }
  }

  def cleanup(): Unit = {
    val failures = ArrayBuffer.empty[Throwable]
    Iterator.continually(artifactIds.poll()).takeWhile(_ != null).foreach { artifactId =>
      try {
        deleteTrackedArtifact(artifactId)
      } catch {
        case NonFatal(e) =>
          println(s"Artifact cleanup failed for artifact $artifactId: $e")
          failures += e
      }
    }

    failures.headOption.foreach { failure =>
      failures.tail.filterNot(_ eq failure).foreach(failure.addSuppressed)
      throw failure
    }
  }
}
