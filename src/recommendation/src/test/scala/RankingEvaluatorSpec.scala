// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

class RankingEvaluatorSpec extends TestBase {

  import session.implicits._

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
    assert(map("ba") == 1.0)
    assert(map("slg") == 3.0)
    assert(map("ops") == 4.0)
    assert(map("runs") == 1.0)
    assert(map("rbi") == 3.0)
    assert(map("hr") == 1.0)
    assert(map("so") == 0.0)
  }

  test("testAllTrueMisorder") {
    val df = Seq((Array(1, 2, 3), Array(3, 1, 2)))
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
    assert(map("fcp") == 0.0)
    assert(map("ba") == 1.0)
    assert(map("slg") == 1.5)
    assert(map("ops") == 2.5)
    assert(map("runs") == 0.0)
    assert(map("rbi") == 0.0)
    assert(map("hr") == 1.0)
    assert(map("so") == 0.0)
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
    assert(map("ba") == 0.0)
    assert(map("slg") == 0.0)
    assert(map("ops") == 0.0)
    assert(map("runs") == 0.0)
    assert(map("rbi") == 0.0)
    assert(map("hr") == 0.0)
    assert(map("so") == 1.0)
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
    assert(map("ba") == 1.0)
    assert(map("slg") == 1.0)
    assert(map("ops") == 2.0)
    assert(map("runs") == 1.0)
    assert(map("rbi") == 1.0)
    assert(map("hr") == 1.0)
    assert(map("so") == 0.0)
  }

  test("test_more_predictions") {
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
    assert(map("ba") == 1.0)
    assert(map("slg") == 6.0)
    assert(map("ops") == 7.0)
    assert(map("runs") == 1.0)
    assert(map("rbi") == 3.0)
    assert(map("hr") == 1.0)
    assert(map("so") == 0.0)
  }

  test("test_more_labels") {
    val df = Seq((Array(1, 2, 3), Array(1, 2, 3, 4, 5, 6)))
      .toDF("prediction", "label")

    val evaluator = new RankingEvaluator()
      .setK(6)
      .setNItems(6)

    val map = evaluator.getMetricsMap(df)
    assert(map("map") == 0.5)
    assert(map("maxDiversity") == 1.0)
    assert(map("diversityAtK") == 0.5)
    assert(map("recallAtK") == 1.0)
    assert(map("ndcgAt") == 0.6448244864271548)
    assert(map("precisionAtk") == 0.5)
    assert(map("mrr") == 1.0)
    assert(map("fcp") == 1.0)
    assert(map("ba") == 1.0)
    assert(map("slg") == 3.0)
    assert(map("ops") == 4.0)
    assert(map("runs") == 1.0)
    assert(map("rbi") == 3.0)
    assert(map("hr") == 1.0)
    assert(map("so") == 0.0)
  }
}
