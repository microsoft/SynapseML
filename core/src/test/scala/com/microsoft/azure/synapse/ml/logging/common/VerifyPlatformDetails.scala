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

  test("runningOnSynapseInternal returns boolean") {
    val result = PlatformDetails.runningOnSynapseInternal()
    assert(result.isInstanceOf[Boolean])
  }

  test("runningOnSynapse returns boolean") {
    val result = PlatformDetails.runningOnSynapse()
    assert(result.isInstanceOf[Boolean])
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
