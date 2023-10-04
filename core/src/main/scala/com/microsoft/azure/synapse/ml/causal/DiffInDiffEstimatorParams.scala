package com.microsoft.azure.synapse.ml.causal

import org.apache.spark.ml.param.Params

trait DiffInDiffEstimatorParams extends Params
  with HasTreatmentCol
  with HasOutcomeCol
  with HasPostTreatmentCol