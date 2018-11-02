// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.tuning.{RankingTrainValidationSplit, RankingTrainValidationSplitModel}
import org.apache.spark.ml.util.MLReadable

class RankingTrainValidationSplitSpec extends RankingTestBase with EstimatorFuzzing[RankingTrainValidationSplit]{

  override def testObjects(): Seq[TestObject[RankingTrainValidationSplit]] = {
    List(new TestObject(rankingTrainValidationSplit, transformedDf))
  }

  override def reader: MLReadable[_] = RankingTrainValidationSplit

  override def modelReader: MLReadable[_] = RankingTrainValidationSplitModel
}

class RankingTrainValidationSplitModelSpec extends RankingTestBase with TransformerFuzzing[RankingTrainValidationSplitModel] {
  override def testObjects(): Seq[TestObject[RankingTrainValidationSplitModel]] = {
    val df = transformedDf
    List(new TestObject(rankingTrainValidationSplit.fit(df), df))
  }

  override def reader: MLReadable[_] = RankingTrainValidationSplitModel
}

