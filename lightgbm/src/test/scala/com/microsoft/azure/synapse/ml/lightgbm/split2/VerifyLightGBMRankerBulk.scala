// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split2

import com.microsoft.azure.synapse.ml.lightgbm.LightGBMConstants

/** Tests to validate the functionality of LightGBM Ranker module in bulk mode. */
class VerifyLightGBMRankerBulk extends VerifyLightGBMRankerStream {
  override lazy val dataTransferMode: String = LightGBMConstants.BulkDataTransferMode
  override def ignoreSerializationFuzzing: Boolean = true
  override def ignoreExperimentFuzzing: Boolean = true
}
