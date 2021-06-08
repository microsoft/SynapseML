// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm.params

import com.microsoft.ml.spark.lightgbm.dataset.LightGBMDataset

trait FObjTrait extends Serializable {
  /**
    * User defined objective function, returns gradient and second order gradient
    *
    * @param predictions untransformed margin predicts
    * @param trainingData training data
    * @return List with two float array, correspond to grad and hess
    */
  def getGradient(predictions: Array[Array[Double]], trainingData: LightGBMDataset): (Array[Float], Array[Float])
}
