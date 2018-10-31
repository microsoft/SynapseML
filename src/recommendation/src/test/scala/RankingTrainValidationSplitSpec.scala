// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.tuning.RankingTrainValidationSplit

class RankingTrainValidationSplitSpec extends RankingTestBase {

  test("testALSSparkTVS") {

    import scala.language.implicitConversions

    val df = pipeline.fit(ratings).transform(ratings)

    val rankingTrainValidationSplit = new RankingTrainValidationSplit()
      .setEvaluator(evaluator)
      .setEstimator(als)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)

    val model = rankingTrainValidationSplit.fit(df)

    val items = model.recommendForAllUsers(3)
    print(items)
    print(model.validationMetrics)
  }

}
