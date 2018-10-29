// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, Dataset}

class RankingDataFrame(override val uid: String, dataframe: Dataset[_]) extends RankingFunctions {

  def randomSplit(weights: Array[Double], seed: Long): Array[DataFrame] = {

    val (trainingDataset, validationDataset) = split(dataframe, weights(0), this.getUserCol, this.getItemCol, this
      .getRatingCol)
    List(trainingDataset, validationDataset).toArray
  }

  override def copy(extra: ParamMap): RankingDataFrame = {
    defaultCopy(extra)
  }
}
