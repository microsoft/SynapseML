// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

class RecommendationEvaluatorTest extends TestBase {

  test("testAllTrue") {
    val seq = Seq((Array(1, 2, 3), Array(1, 2, 3)))
    import session.implicits._
    val df = session.createDataset(seq).toDF("prediction", "label")

    val evaluator = new RecommendationEvaluator()
      .setK(3)
      .setSaveAll(true)

    evaluator.setNItems(3)
    evaluator.evaluate(df)

    evaluator.printMetrics()
    val map = evaluator.getMetricsList.head
    assert(map("map") == 1.0)
    assert(map("maxDiversity") == 1.0)
    assert(map("diversityAtK") == 1.0)
    assert(map("ndcgAt") == 1.0)
    assert(map("precisionAtk") == 1.0)
  }

  test("testAllMiss") {
    val seq = Seq((Array(4, 5, 6), Array(1, 2, 3)))
    import session.implicits._
    val df = session.createDataset(seq).toDF("prediction", "label")

    val evaluator = new RecommendationEvaluator()
      .setK(3)
      .setSaveAll(true)

    evaluator.setNItems(6)
    evaluator.evaluate(df)

    evaluator.printMetrics()
    val map = evaluator.getMetricsList.head
    assert(map("map") == 0.0)
    assert(map("maxDiversity") == 1.0)
    assert(map("diversityAtK") == 0.5)
    assert(map("ndcgAt") == 0.0)
    assert(map("precisionAtk") == 0.0)
  }

  test("testOrder") {
    val seq = Seq((Array(3, 2, 1), Array(1, 2, 3)))
    import session.implicits._
    val df = session.createDataset(seq).toDF("prediction", "label")

    val evaluator = new RecommendationEvaluator()
      .setK(3)
      .setSaveAll(true)

    evaluator.setNItems(3)
    evaluator.evaluate(df)

    evaluator.printMetrics()
    val map = evaluator.getMetricsList.head
    assert(map("map") == 1.0)
    assert(map("maxDiversity") == 1.0)
    assert(map("diversityAtK") == 1.0)
    assert(map("ndcgAt") == 1.0)
    assert(map("precisionAtk") == 1.0)
  }

  test("testExtra") {
    val seq = Seq((Array(1, 2, 3, 4, 5, 6), Array(1, 2, 3)))
    import session.implicits._
    val df = session.createDataset(seq).toDF("prediction", "label")

    val evaluator = new RecommendationEvaluator()
      .setK(6)
      .setSaveAll(true)

    evaluator.setNItems(6)
    evaluator.evaluate(df)

    evaluator.printMetrics()
    val map = evaluator.getMetricsList.head
    assert(map("map") == 1.0)
    assert(map("maxDiversity") == 1.0)
    assert(map("diversityAtK") == 1.0)
    assert(map("recallAtK") == 0.5)
    assert(map("ndcgAt") == 1.0)
    assert(map("precisionAtk") == 0.5)
  }

//  override def serializationTestObjects(): Seq[TestObject[RecommendationEvaluator]] = {
//    val seq = Seq((Array(1, 2, 3, 4, 5, 6), Array(1, 2, 3)))
//    import session.implicits._
//    val df = session.createDataset(seq).toDF("prediction", "label")
//
//    val evaluator = new RecommendationEvaluator()
//      .setK(6)
//      .setSaveAll(true)
//
//    evaluator.setNItems(6)
//
//    List(new TestObject(evaluator, df))
//  }

//  override def reader: MLReadable[_] = RecommendationEvaluator

//  override def modelReader: MLReadable[_] = RecommendationEvaluator
}
