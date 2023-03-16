// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.recommendation

import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject, TransformerFuzzing}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.util.MLReadable

class RecommendationIndexerSpec extends RankingTestBase with EstimatorFuzzing[RecommendationIndexer] {
  override def testObjects(): Seq[TestObject[RecommendationIndexer]] = {
    List(new TestObject(recommendationIndexer, ratings))
  }

  override def reader: MLReadable[_] = RecommendationIndexer

  override def modelReader: MLReadable[_] = RankingAdapterModel

  test("ALS") {

    val algo = als

    val adapter: RankingAdapter = new RankingAdapter()
      .setK(5)
      .setRecommender(algo)

    val recopipeline = new Pipeline()
      .setStages(Array(recommendationIndexer, adapter))
      .fit(ratings)

    val output = recopipeline.transform(ratings)

    val evaluator: RankingEvaluator = new RankingEvaluator()
      .setK(5)
      .setNItems(10)

    assert(evaluator.setMetricName("fcp").evaluate(output) == 0.2)
  }

}

class RecommendationIndexerModelSpec extends RankingTestBase with TransformerFuzzing[RecommendationIndexerModel] {
  override def testObjects(): Seq[TestObject[RecommendationIndexerModel]] = {
    val df = ratings
    List(new TestObject(recommendationIndexer.fit(df), df))
  }

  override def reader: MLReadable[_] = RankingAdapterModel
}
