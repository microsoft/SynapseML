// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.tuning.{RankingCrossValidator, RankingCrossValidatorModel}
import org.apache.spark.ml.util.MLReadable

class RankingCrossValidatorSpec extends RankingTestBase with EstimatorFuzzing[RankingCrossValidator]{

  override def testObjects(): Seq[TestObject[RankingCrossValidator]] = {
    List(new TestObject(rankingCrossValidator , transformedDf))
  }

  override def reader: MLReadable[_] = RankingCrossValidator

  override def modelReader: MLReadable[_] = RankingCrossValidatorModel
}

class RankingCrossValidatorModelSpec extends RankingTestBase with TransformerFuzzing[RankingCrossValidatorModel] {
  override def testObjects(): Seq[TestObject[RankingCrossValidatorModel]] = {
    val df = transformedDf
    List(new TestObject(rankingCrossValidator.fit(df), df))
  }

  override def reader: MLReadable[_] = RankingCrossValidatorModel
}

