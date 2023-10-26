// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import org.apache.spark.ml.param.Params

trait DiffInDiffEstimatorParams extends Params
  with HasTreatmentCol
  with HasOutcomeCol
  with HasPostTreatmentCol
