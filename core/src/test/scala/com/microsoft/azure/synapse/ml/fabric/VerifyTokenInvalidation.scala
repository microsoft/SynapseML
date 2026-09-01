// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.fabric

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

import java.nio.file.Files

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
}
