// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.fabric

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

import java.nio.file.Files
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}

object BrokenReflectionCandidate {
  val Initialize: Unit = throw new RuntimeException("simulated initialization failure")

  def clear(): Unit = ()
}

object WorkingReflectionCandidate {
  def clear(): Unit = ()
}

class VerifyTokenInvalidation extends TestBase {

  test("Spark MWC invalidation resolves the encoded NFS cache key") {
    val tokenPath = Files.createTempFile("synapseml-mwc-token", ".cache")
    val events = scala.collection.mutable.ArrayBuffer.empty[(String, String)]
    val logicalCacheKey = "WorkspaceArtifact2SparkCore"

    try {
      TokenLibrary.invalidateSparkMwcTokenCaches(
        logicalCacheKey,
        cacheKey => {
          events += ("encode" -> cacheKey)
          "encoded-cache-key"
        },
        cacheKey => {
          events += ("delete" -> cacheKey)
          Files.deleteIfExists(tokenPath)
        },
        () => {
          events += ("clear" -> "")
          true
        })

      assert(events === Seq(
        "encode" -> logicalCacheKey,
        "delete" -> "encoded-cache-key",
        "clear" -> ""))
      assert(!Files.exists(tokenPath))
    } finally {
      Files.deleteIfExists(tokenPath)
    }
  }

  test("Spark MWC invalidation fails when the runtime exposes no supported cache API") {
    val error = intercept[NoSuchMethodException] {
      TokenLibrary.invalidateSparkMwcTokenCaches(
        "cache-key",
        identity,
        _ => false,
        () => false)
    }

    assert(error.getMessage.contains("does not expose MWC token cache invalidation"))
  }

  test("runtime reflection skips a broken candidate and uses the next compatible class") {
    val method = TokenLibrary.objectMethod(
      Seq(
        "com.microsoft.azure.synapse.ml.fabric.BrokenReflectionCandidate$",
        "com.microsoft.azure.synapse.ml.fabric.WorkingReflectionCandidate$"),
      "clear",
      0)

    assert(method.exists(_._1.getClass.getName.endsWith("WorkingReflectionCandidate$")))
  }

  test("NFS token deletion reports filesystem failures without throwing") {
    val directory = Files.createTempDirectory("synapseml-mwc-cache")
    val child = Files.createFile(directory.resolve("token"))

    try {
      assert(!TokenLibrary.deleteTokenPath(directory))
      assert(Files.exists(directory))
      assert(Files.exists(child))
    } finally {
      Files.deleteIfExists(child)
      Files.deleteIfExists(directory)
    }
  }

  test("concurrent refreshes invalidate a rejected MWC token once") {
    val workers = 8
    val executor = Executors.newFixedThreadPool(workers)
    val start = new CountDownLatch(1)
    val currentAuth = new AtomicReference("MwcToken stale")
    val invalidationCount = new AtomicInteger(0)
    val refreshLock = new Object()

    try {
      val futures = (1 to workers).map { _ =>
        executor.submit(new Callable[String] {
          override def call(): String = {
            start.await()
            FabricClient.refreshAuthHeader(
              "MwcToken stale",
              refreshLock,
              () => currentAuth.get(),
              () => {
                invalidationCount.incrementAndGet()
                currentAuth.set("MwcToken fresh")
              })
          }
        })
      }
      start.countDown()

      assert(futures.map(_.get(10, TimeUnit.SECONDS)) === Seq.fill(workers)("MwcToken fresh"))
      assert(invalidationCount.get() === 1)
    } finally {
      executor.shutdownNow()
    }
  }
}
