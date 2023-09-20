// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.logging.Usage.{FeatureSynapseML, FeatureActivityTransform,
  FeatureActivityTrain, FeatureActivityFit}

class FeatureUsagePayloadTest extends TestBase {
  test("FeatureName"){
    assert(new FeatureSynapseML().getFeatureName == "SynapseML")
  }
  test("FeatureActivityName") {
    assert(new FeatureActivityTransform().getFeatureActivityName == "Transform")
    assert(new FeatureActivityTrain().getFeatureActivityName == "Train")
    assert(new FeatureActivityFit().getFeatureActivityName == "Fit")
  }
}
