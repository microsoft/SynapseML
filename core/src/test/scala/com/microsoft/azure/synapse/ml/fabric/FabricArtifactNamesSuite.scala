// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.fabric

import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDateTime
import java.util.UUID

class FabricArtifactNamesSuite extends AnyFunSuite {
  private val testTime = LocalDateTime.of(2026, 8, 9, 2, 18, 35)
  private val testId = UUID.fromString("01234567-89ab-cdef-0123-456789abcdef")
  private val otherTestId = UUID.fromString("fedcba98-7654-3210-fedc-ba9876543210")

  test("Add a unique suffix to Spark Job Definition names") {
    assert(
      FabricArtifactNames.sjd("TestNotebook", testTime, testId) ==
        "TestNotebook-20260809-02-18-35-0123456789abcdef0123456789abcdef")
  }

  test("Add a unique suffix to store artifact names") {
    assert(
      FabricArtifactNames.store("Lakehouse", testTime, testId) ==
        "Lakehouse202608090218350123456789abcdef0123456789abcdef")
  }

  test("Distinguish artifacts created with the same timestamp") {
    assert(
      FabricArtifactNames.sjd("TestNotebook", testTime, testId) !=
        FabricArtifactNames.sjd("TestNotebook", testTime, otherTestId))
    assert(
      FabricArtifactNames.store("Lakehouse", testTime, testId) !=
        FabricArtifactNames.store("Lakehouse", testTime, otherTestId))
  }
}
