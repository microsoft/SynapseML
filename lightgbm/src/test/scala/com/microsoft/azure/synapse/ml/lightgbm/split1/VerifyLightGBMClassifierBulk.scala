// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.lightgbm._

/** Tests to validate the functionality of LightGBM module. */
class VerifyLightGBMClassifierBulk extends VerifyLightGBMClassifierStream {
  override lazy val executionMode: String = LightGBMConstants.BulkExecutionMode
  override def ignoreSerializationFuzzing: Boolean = true
  override def ignoreExperimentFuzzing: Boolean = true
}
