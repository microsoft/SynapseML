// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split6

import com.microsoft.azure.synapse.ml.lightgbm._
import com.microsoft.azure.synapse.ml.lightgbm.split5.VerifyLightGBMClassifierStream

/** Tests to validate the functionality of LightGBM module. */
class VerifyLightGBMClassifierBulk extends VerifyLightGBMClassifierStream {
  override lazy val dataTransferMode: String = LightGBMConstants.BulkDataTransferMode
}
