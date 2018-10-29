// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.DataFrame

class RankingCrossValidatorModel(crossValidatorModel: CrossValidatorModel) {
  def recommendForAllUsers(k: Int): DataFrame = crossValidatorModel.bestModel.asInstanceOf[RankingAdapterModel]
    .recommendForAllUsers(k)
}
