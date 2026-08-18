// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.common

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

class VerifyPlatformDetails extends TestBase {

  test("Platform constants have expected values") {
    assert(PlatformDetails.PlatformSynapseInternal === "synapse_internal")
    assert(PlatformDetails.PlatformSynapse === "synapse")
    assert(PlatformDetails.PlatformBinder === "binder")
    assert(PlatformDetails.PlatformDatabricks === "databricks")
    assert(PlatformDetails.PlatformUnknown === "unknown")
    assert(PlatformDetails.SynapseProjectName === "Microsoft.ProjectArcadia")
  }

  test("CurrentPlatform returns a string") {
    val platform = PlatformDetails.CurrentPlatform
    assert(platform.nonEmpty)
  }

  test("FabricRuntime returns a stable runtime value") {
    val runtime = PlatformDetails.FabricRuntime
    assert(runtime.nonEmpty)
    assert(runtime === PlatformDetails.FabricRuntime)
  }

  test("sparkVersion safely reports the packaged Spark runtime") {
    assert(PlatformDetails.sparkVersion.contains(org.apache.spark.SPARK_VERSION))
  }

  test("resolveFabricRuntime prefers Spark and falls back to Python or Fabric") {
    assert(
      PlatformDetails.resolveFabricRuntime(
        isFabric = true,
        sparkVersion = Some("3.5.4"),
        pythonVersion = Some("3.11.9")) ===
        "fabric_spark_3.5.4")
    assert(
      PlatformDetails.resolveFabricRuntime(
        isFabric = true,
        sparkVersion = None,
        pythonVersion = Some("3.11.9")) ===
        "fabric_python_3.11.9")
    assert(
      PlatformDetails.resolveFabricRuntime(
        isFabric = true,
        sparkVersion = None,
        pythonVersion = None) ===
        "fabric")
    assert(
      PlatformDetails.resolveFabricRuntime(
        isFabric = false,
        sparkVersion = Some("3.5.4"),
        pythonVersion = Some("3.11.9")) ===
        PlatformDetails.PlatformUnknown)
  }

  test("currentPlatform returns a valid platform string") {
    val platform = PlatformDetails.currentPlatform()
    val validPlatforms = Set(
      PlatformDetails.PlatformSynapseInternal,
      PlatformDetails.PlatformSynapse,
      PlatformDetails.PlatformBinder,
      PlatformDetails.PlatformDatabricks,
      PlatformDetails.PlatformUnknown
    )
    assert(validPlatforms.contains(platform))
  }

  test("runningOnSynapseInternal agrees with CurrentPlatform") {
    assert(PlatformDetails.runningOnSynapseInternal() ===
      (PlatformDetails.CurrentPlatform == PlatformDetails.PlatformSynapseInternal))
  }

  test("runningOnSynapse agrees with CurrentPlatform") {
    assert(PlatformDetails.runningOnSynapse() ===
      (PlatformDetails.CurrentPlatform == PlatformDetails.PlatformSynapse))
  }

  test("runningOnSynapse and runningOnSynapseInternal are mutually exclusive") {
    assert(!(PlatformDetails.runningOnSynapse() && PlatformDetails.runningOnSynapseInternal()))
  }

  test("runningOnFabric returns same as runningOnSynapseInternal") {
    assert(PlatformDetails.runningOnFabric() === PlatformDetails.runningOnSynapseInternal())
  }

  test("CurrentPlatform returns a known platform value") {
    val platform = PlatformDetails.CurrentPlatform
    // Expected platforms when running tests on a local/dev environment
    val expectedOnDev = Set(PlatformDetails.PlatformUnknown, PlatformDetails.PlatformBinder)
    // Allow-list of platforms that may legitimately appear in CI (e.g., Synapse or Databricks)
    val ciPlatforms = Set(
      PlatformDetails.PlatformSynapseInternal,
      PlatformDetails.PlatformSynapse,
      PlatformDetails.PlatformDatabricks
    )
    // Verify that the platform is either a dev-expected value or a known CI platform
    assert(expectedOnDev.contains(platform) || ciPlatforms.contains(platform))
  }
}
