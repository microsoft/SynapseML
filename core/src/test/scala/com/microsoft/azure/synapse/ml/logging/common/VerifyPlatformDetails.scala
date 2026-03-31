// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.common

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

class VerifyPlatformDetails extends TestBase {

  test("currentPlatform returns a non-null non-empty string") {
    val platform = PlatformDetails.currentPlatform()
    assert(platform != null)
    assert(platform.nonEmpty)
  }

  test("currentPlatform returns one of the known platform values") {
    val known = Set(
      PlatformDetails.PlatformSynapseInternal,
      PlatformDetails.PlatformSynapse,
      PlatformDetails.PlatformDatabricks,
      PlatformDetails.PlatformBinder,
      PlatformDetails.PlatformUnknown
    )
    assert(known.contains(PlatformDetails.currentPlatform()))
  }

  test("runningOnFabric is consistent with runningOnSynapseInternal") {
    assert(PlatformDetails.runningOnFabric() === PlatformDetails.runningOnSynapseInternal())
  }
}
