package com.microsoft.ml.spark

class MsftRecommendationEvaluatorTest extends TestBase {

  test("testAllTrue") {
    val seq = Seq((Array(1, 2, 3), Array(1, 2, 3)))
    import session.implicits._
    val df = session.createDataset(seq).toDF("prediction", "label")

    val evaluator = new MsftRecommendationEvaluator()
      .setK(3)
      .setSaveAll(true)

    evaluator.setNItems(3)
    evaluator.evaluate(df)

    evaluator.printMetrics()
    val map = evaluator.getMetricsList(0)
    assert(map.get("map").get == 1.0)
    assert(map.get("maxDiversity").get == 1.0)
    assert(map.get("diversityAtK").get == 1.0)
    assert(map.get("ndcgAt").get == 1.0)
    assert(map.get("precisionAtk").get == 1.0)
  }

  test("testAllMiss") {
    val seq = Seq((Array(4, 5, 6), Array(1, 2, 3)))
    import session.implicits._
    val df = session.createDataset(seq).toDF("prediction", "label")

    val evaluator = new MsftRecommendationEvaluator()
      .setK(3)
      .setSaveAll(true)

    evaluator.setNItems(6)
    evaluator.evaluate(df)

    evaluator.printMetrics()
    val map = evaluator.getMetricsList(0)
    assert(map.get("map").get == 0.0)
    assert(map.get("maxDiversity").get == 0.5)
    assert(map.get("diversityAtK").get == 0.5)
    assert(map.get("ndcgAt").get == 0.0)
    assert(map.get("precisionAtk").get == 0.0)
  }

  test("testOrder") {
    val seq = Seq((Array(3, 2, 1), Array(1, 2, 3)))
    import session.implicits._
    val df = session.createDataset(seq).toDF("prediction", "label")

    val evaluator = new MsftRecommendationEvaluator()
      .setK(3)
      .setSaveAll(true)

    evaluator.setNItems(3)
    evaluator.evaluate(df)

    evaluator.printMetrics()
    val map = evaluator.getMetricsList(0)
    assert(map.get("map").get == 1.0)
    assert(map.get("maxDiversity").get == 1.0)
    assert(map.get("diversityAtK").get == 1.0)
    assert(map.get("ndcgAt").get == 1.0)
    assert(map.get("precisionAtk").get == 1.0)
  }

  test("testExtra") {
    val seq = Seq((Array(1, 2, 3, 4, 5, 6), Array(1, 2, 3)))
    import session.implicits._
    val df = session.createDataset(seq).toDF("prediction", "label")

    val evaluator = new MsftRecommendationEvaluator()
      .setK(6)
      .setSaveAll(true)

    evaluator.setNItems(6)
    evaluator.evaluate(df)

    evaluator.printMetrics()
    val map = evaluator.getMetricsList(0)
    assert(map.get("map").get == 1.0)
    assert(map.get("maxDiversity").get == 0.5)
    assert(map.get("diversityAtK").get == 1.0)
    assert(map.get("recallAtK").get == 0.5)
    assert(map.get("ndcgAt").get == 1.0)
    assert(map.get("precisionAtk").get == 0.5)
  }

}
