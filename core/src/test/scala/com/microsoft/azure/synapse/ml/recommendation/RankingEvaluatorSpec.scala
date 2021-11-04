// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.recommendation

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

class RankingEvaluatorSpec extends TestBase {

  import spark.implicits._

  test("testAllTrue") {
    val df = Seq((Array(1, 2, 3), Array(1, 2, 3)))
      .toDF("prediction", "label")

    val evaluator = new RankingEvaluator()
      .setK(3)
      .setNItems(3)

    val map = evaluator.getMetricsMap(df)
    assert(map("map") == 1.0)
    assert(map("maxDiversity") == 1.0)
    assert(map("diversityAtK") == 1.0)
    assert(map("ndcgAt") == 1.0)
    assert(map("precisionAtk") == 1.0)
    assert(map("mrr") == 1.0)
    assert(map("fcp") == 1.0)
  }

  test("testAllMiss") {
    val df = Seq((Array(4, 5, 6), Array(1, 2, 3)))
      .toDF("prediction", "label")

    val evaluator = new RankingEvaluator()
      .setK(3)
      .setNItems(6)

    val map = evaluator.getMetricsMap(df)
    assert(map("map") == 0.0)
    assert(map("maxDiversity") == 1.0)
    assert(map("diversityAtK") == 0.5)
    assert(map("ndcgAt") == 0.0)
    assert(map("precisionAtk") == 0.0)
    assert(map("mrr") == 0.0)
    assert(map("fcp") == 0.0)
  }

  test("testOrder") {
    val df = Seq((Array(3, 2, 1), Array(1, 2, 3)))
      .toDF("prediction", "label")

    val evaluator = new RankingEvaluator()
      .setK(3)
      .setNItems(3)

    val map = evaluator.getMetricsMap(df)
    assert(map("map") == 1.0)
    assert(map("maxDiversity") == 1.0)
    assert(map("diversityAtK") == 1.0)
    assert(map("ndcgAt") == 1.0)
    assert(map("precisionAtk") == 1.0)
    assert(map("mrr") == 1.0)
    assert(map("fcp") == 0.3333333333333333)
  }

  test("testExtra") {
    val df = Seq((Array(1, 2, 3, 4, 5, 6), Array(1, 2, 3)))
      .toDF("prediction", "label")

    val evaluator = new RankingEvaluator()
      .setK(6)
      .setNItems(6)

    val map = evaluator.getMetricsMap(df)
    assert(map("map") == 1.0)
    assert(map("maxDiversity") == 1.0)
    assert(map("diversityAtK") == 1.0)
    assert(map("recallAtK") == 0.5)
    assert(map("ndcgAt") == 1.0)
    assert(map("precisionAtk") == 0.5)
    assert(map("mrr") == 1.0)
    assert(map("fcp") == 1.0)
  }
}
