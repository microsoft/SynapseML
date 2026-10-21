// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.automl.{GridSpace, TuneHyperparameters}
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMRanker, LightGBMRankerModel, LightGBMUtils}
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.ParamMap

class VerifyLightGBMRankingTuning extends TestBase {
  test("TuneHyperparameters fits and refits a real LightGBM ranker") {
    val rows = (0 until 8).flatMap(q => (0 until 3).map(l =>
      (s"q$q", l.toDouble, Vectors.dense(l.toDouble, q.toDouble))))
    val data = spark.createDataFrame(rows).toDF("query", "label", "features").coalesce(1)
    val ranker = new LightGBMRanker().setGroupCol("query").setNumTasks(1).setNumThreads(1)
      .setNumIterations(2).setMinDataInLeaf(1).setVerbosity(-1)
    val params = new GridSpace(Array(ParamMap(ranker.numLeaves -> 2), ParamMap(ranker.numLeaves -> 3)))
    val tuner = new TuneHyperparameters().setModels(Array[Estimator[_]](ranker)).setNumFolds(2).setNumRuns(2)
      .setParallelism(1).setParamSpace(params).setEvaluationMetric("ndcg@5")
    val best = tuner.fit(data)
    assert(best.getBestMetric >= 0.0 && best.getBestMetric <= 1.0)
    assert(best.getBestModel.isInstanceOf[LightGBMRankerModel])
    assert(best.transform(data).count() == rows.length)
  }

  test("TuneHyperparameters accepts a ranker previously fitted on other data") {
    val rows = (0 until 8).flatMap(q => (0 until 3).map(l =>
      (s"q$q", l.toDouble, Vectors.dense(l.toDouble, q.toDouble))))
    val oldRows = rows.map { case (query, label, _) =>
      (query, label, Vectors.dense(label + 100.0, label - 100.0))
    }
    val data = spark.createDataFrame(rows).toDF("query", "label", "features").coalesce(1)
    val previousData = spark.createDataFrame(oldRows).toDF("query", "label", "features").coalesce(1)
    val ranker = new LightGBMRanker().setGroupCol("query").setNumTasks(1).setNumThreads(1)
      .setNumIterations(2).setMinDataInLeaf(1).setVerbosity(-1)
    val previous = ranker.fit(previousData)
    try {
      val reference = ranker.getReferenceDataset.clone()
      assert(reference.nonEmpty)
      val best = new TuneHyperparameters().setModels(Array[Estimator[_]](ranker)).setNumFolds(2).setNumRuns(1)
        .setParallelism(1).setParamSpace(new GridSpace(Array(ParamMap.empty))).setEvaluationMetric("ndcg").fit(data)
      try {
        assert(best.getBestMetric >= 0.0 && best.getBestMetric <= 1.0)
        assert(best.transform(data).count() == rows.length)
        assert(ranker.getReferenceDataset.sameElements(reference))
      } finally {
        best.getBestModel.asInstanceOf[LightGBMRankerModel].getLightGBMBooster.freeNativeMemory()
      }
    } finally {
      previous.getLightGBMBooster.freeNativeMemory()
    }
  }

  test("TuneHyperparameters rejects custom native label gains before training") {
    val data = spark.createDataFrame(Seq(("q", 1.0, Vectors.dense(1.0))))
      .toDF("query", "label", "features")
    val ranker = new LightGBMRanker().setGroupCol("query").setLabelGain(Array(0.0, 4.0))
    val tuner = new TuneHyperparameters().setModels(Array[Estimator[_]](ranker)).setNumFolds(2).setNumRuns(1)
      .setParallelism(1).setParamSpace(new GridSpace(Array(ParamMap.empty))).setEvaluationMetric("ndcg")
    assert(intercept[IllegalArgumentException] { tuner.fit(data) }.getMessage.contains("custom labelGain"))
  }
  test("TuneHyperparameters rejects pass-through label gains before training") {
    val data = spark.createDataFrame(Seq(("q", 1.0, Vectors.dense(1.0))))
      .toDF("query", "label", "features")
    val configurations = Seq("label_gain=0,4", "\"label_gain\"=0,4", "'label_gain'=0,4",
      "label_gain=\"0,4\"", "=label_gain=0,4", "label_gain==0,4",
      "max_bin=63\t'label_gain'=0,4", "max_bin=63\n\"label_gain\"=0,4", "max_bin=63 \"label_gain\"=0,4")
    configurations.foreach { args =>
      assert(LightGBMUtils.parseLightGBMParams(args).contains("label_gain"))
      Seq(false, true).foreach { sampled =>
        val ranker = new LightGBMRanker().setGroupCol("query")
        val params = if (sampled) ParamMap(ranker.passThroughArgs -> args) else {
          ranker.setPassThroughArgs(args)
          ParamMap.empty
        }
        val tuner = new TuneHyperparameters().setModels(Array[Estimator[_]](ranker)).setNumFolds(2).setNumRuns(1)
          .setParallelism(1).setParamSpace(new GridSpace(Array(params))).setEvaluationMetric("ndcg")
        val error = intercept[IllegalArgumentException] { tuner.fit(data) }
        assert(error.getMessage.contains("custom label_gain"), s"args=$args, sampled=$sampled")
      }
    }
  }

}
