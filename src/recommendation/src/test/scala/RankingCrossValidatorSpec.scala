// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.tuning.RankingCrossValidator

class RankingCrossValidatorSpec extends RankingTestBase {

  test("testALSSparkTVS") {

    import scala.language.implicitConversions

    val df = pipeline.fit(ratings).transform(ratings)

    val rankingTrainValidationSplit = new RankingCrossValidator()
      .setEstimator(adapter)
      .setEstimatorParamMaps(paramGrid)
      .setEvaluator(evaluator)

    val model = rankingTrainValidationSplit.fit(df)

    val items = model.recommendForAllUsers(3)
    print(items)
    print(model.avgMetrics)
  }

}
