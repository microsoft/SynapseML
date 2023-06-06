// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split3

import com.microsoft.azure.synapse.ml.lightgbm._
import com.microsoft.azure.synapse.ml.lightgbm.split4.VerifyLightGBMClassifierStream

/** Tests to validate the functionality of LightGBM module. */
class VerifyLightGBMClassifierBulkBasic extends VerifyLightGBMClassifierStream {
  override lazy val executionMode: String = LightGBMConstants.BulkDataTransferMode
  override def ignoreSerializationFuzzing: Boolean = true
  override def ignoreExperimentFuzzing: Boolean = true
}
