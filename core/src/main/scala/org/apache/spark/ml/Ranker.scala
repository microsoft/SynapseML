// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml

import com.microsoft.azure.synapse.ml.core.contracts.HasGroupCol

// Note: a bit strange to have the synapsemlimport here, but it works

/**
  * Ranker base class
  *
  * @tparam FeaturesType Type of input features.  E.g., org.apache.spark.mllib.linalg.Vector
  * @tparam Learner      Concrete Estimator type
  * @tparam M            Concrete Model type
  */
abstract class Ranker[FeaturesType, Learner <: Ranker[FeaturesType, Learner, M], M <: RankerModel[FeaturesType, M]]
  extends Predictor[FeaturesType, Learner, M] with PredictorParams with HasGroupCol

/**
  * Model produced by a `Ranker`.
  *
  * @tparam FeaturesType Type of input features.  E.g., org.apache.spark.mllib.linalg.Vector
  * @tparam M            Concrete Model type.
  */
abstract class RankerModel[FeaturesType, M <: RankerModel[FeaturesType, M]]
  extends PredictionModel[FeaturesType, M] with PredictorParams
