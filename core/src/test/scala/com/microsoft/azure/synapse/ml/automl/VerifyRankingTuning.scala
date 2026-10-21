// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.automl

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.param.ByteArrayParam
import org.apache.spark.ml.{Estimator, Ranker, RankerModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.{DoubleArrayParam, DoubleParam, LongParam, Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._

import java.util.concurrent.atomic.AtomicBoolean

class TestableTuningRanker(override val uid: String)
  extends Ranker[Vector, TestableTuningRanker, TestableTuningRankerModel] with DefaultParamsWritable {
  def this() = this(Identifiable.randomUID("tuningTestRanker"))
  val labelGain = new DoubleArrayParam(this, "labelGain", "Test custom relevance gains")
  val passThroughArgs = new Param[String](this, "passThroughArgs", "Test pass-through gains")
  val referenceDataset = new ByteArrayParam(this, "referenceDataset", "Test retained reference data")
  setDefault(referenceDataset -> Array.empty[Byte])
  val direction = new DoubleParam(this, "direction", "Prediction multiplier for deterministic tuning fixtures")
  setDefault(direction -> 1.0)
  val trainDelayMs = new LongParam(this, "trainDelayMs", "Test training delay")
  setDefault(trainDelayMs -> 0L)
  override protected def train(dataset: Dataset[_]): TestableTuningRankerModel = {
    require($(referenceDataset).isEmpty, "stale reference dataset reached fit")
    try {
      Thread.sleep($(trainDelayMs))
    } catch {
      case error: InterruptedException =>
        TestableTuningRanker.TrainingInterrupted.set(true)
        throw error
    }
    copyValues(new TestableTuningRankerModel(uid)).setParent(this)
  }
  override def copy(extra: ParamMap): TestableTuningRanker = defaultCopy(extra)
}
object TestableTuningRanker extends DefaultParamsReadable[TestableTuningRanker] {
  val TrainingInterrupted = new AtomicBoolean(false)
}

class TestableTuningRankerModel(override val uid: String)
  extends RankerModel[Vector, TestableTuningRankerModel] with DefaultParamsWritable {
  def this() = this(Identifiable.randomUID("tuningTestRankerModel"))
  val direction = new DoubleParam(this, "direction", "Prediction multiplier for deterministic tuning fixtures")
  setDefault(direction -> 1.0)
  override def predict(features: Vector): Double = $(direction) * features(0)
  override def copy(extra: ParamMap): TestableTuningRankerModel = copyValues(new TestableTuningRankerModel(uid), extra)
}
object TestableTuningRankerModel extends DefaultParamsReadable[TestableTuningRankerModel]

class VerifyRankingTuning extends TestBase {
  def data: DataFrame = {
    val session = spark
    import session.implicits._
    (0 until 6).flatMap(g => (0 until 3).map(l => (s"q$g", l.toDouble, Vectors.dense(l.toDouble))))
      .toDF("query", "label", "features")
  }
  def learner: TestableTuningRanker = new TestableTuningRanker().setGroupCol("query")
  def tuner(model: TestableTuningRanker = learner): TuneHyperparameters = {
    new TuneHyperparameters().setModels(Array[Estimator[_]](model)).setNumRuns(2).setNumFolds(3)
      .setParallelism(1).setEvaluationMetric("ndcg@2")
      .setParamSpace(new GridSpace(Array(ParamMap(model.direction -> -1.0), ParamMap(model.direction -> 1.0))))
  }

  test("ranker tuning selects the graded-NDCG winner and refits a ranker") {
    val fitted = tuner().setParallelism(2).fit(data)
    assert(math.abs(fitted.getBestMetric - 1.0) < 1e-12)
    assert(fitted.getBestModel.isInstanceOf[TestableTuningRankerModel])
    assert(fitted.transform(data).count() == data.count())
    val oldError = intercept[Exception] { EvaluationUtils.getModelType(learner) }
    assert(oldError.getMessage == EvaluationUtils.ModelTypeUnsupportedErr)
    val selector = new FindBestModel().setEvaluationMetric("accuracy")
      .setModels(Array[org.apache.spark.ml.Transformer](fitted.getBestModel))
    val selectorError = intercept[Exception] { selector.fit(data) }
    assert(selectorError.getMessage == EvaluationUtils.ModelTypeUnsupportedErr)
  }

  test("ranker tuning clears copied reference data without mutating the input estimator") {
    val model = learner
    model.set(model.referenceDataset, Array[Byte](1, 2, 3))
    val fitted = tuner(model).fit(data)
    assert(math.abs(fitted.getBestMetric - 1.0) < 1e-12)
    assert(model.getOrDefault(model.referenceDataset).sameElements(Array[Byte](1, 2, 3)))
  }

  test("group folds are deterministic, balanced, nonempty and never split a query") {
    val assigned = RankingTuning.folds(data, "query", 3, 7L, "fold")
    assert(assigned.groupBy("query").agg(countDistinct("fold").as("folds")).filter(col("folds") =!= 1).count() == 0)
    val sizes = assigned.select("query", "fold").distinct().groupBy("fold").count().collect().map(_.getLong(1))
    assert(sizes.length == 3 && sizes.max - sizes.min <= 1)
    val repeated = RankingTuning.folds(data, "query", 3, 7L, "fold")
    assert(assigned.except(repeated).count() == 0)
    intercept[IllegalArgumentException] { RankingTuning.folds(data, "query", 7, 7L, "fold") }
    intercept[IllegalArgumentException] {
      val nullGroups = data.withColumn("query", lit(Option.empty[String].orNull).cast("string"))
      RankingTuning.folds(nullGroups, "query", 3, 7L, "fold")
    }
  }

  test("graded NDCG handles cutoff, pessimistic ties and zero relevance") {
    val session = spark
    import session.implicits._
    val fixture = Seq(("q", 3.0, 0.1), ("q", 1.0, 0.9), ("zero", 0.0, 0.5))
      .toDF("query", "label", "prediction")
    assert(math.abs(RankingTuning.ndcg(fixture, "query", "label", "prediction", Some(1)) - 4.0 / 7.0) < 1e-12)
    val tie = Seq(("q", 1.0, 0.5), ("q", 3.0, 0.5)).toDF("query", "label", "prediction")
    assert(math.abs(RankingTuning.ndcg(tie, "query", "label", "prediction", Some(1)) - 1.0 / 7.0) < 1e-12)
    val maxLabel = Seq(("q", 30.0, 1.0)).toDF("query", "label", "prediction")
    assert(RankingTuning.ndcg(maxLabel, "query", "label", "prediction", None) == 1.0)
    Seq(-1.0, 0.5, 31.0, Double.NaN).foreach { label =>
      intercept[IllegalArgumentException] {
        RankingTuning.ndcg(fixture.withColumn("label", lit(label)), "query", "label", "prediction", None)
      }
    }
  }

  test("ranker-only validation rejects ambiguous models, columns, metrics and search spaces") {
    intercept[IllegalArgumentException] {
      tuner().setModels(Array[Estimator[_]](learner, new LogisticRegression())).fit(data)
    }
    val first = learner
    val second = learner.setGroupCol("otherQuery")
    intercept[IllegalArgumentException] { tuner(first).setModels(Array[Estimator[_]](first, second)).fit(data) }
    val differentLabel = learner.setLabelCol("otherLabel")
    intercept[IllegalArgumentException] { tuner(first).setModels(Array[Estimator[_]](first, differentLabel)).fit(data) }
    intercept[IllegalArgumentException] {
      RankingTuning.folds(data.withColumn("query", lit(1.0)), "query", 3, 7L, "fold")
    }
    val gainSearch = new GridSpace(Array(ParamMap(first.labelGain -> Array(0.0, 4.0))))
    intercept[IllegalArgumentException] { tuner(first).setNumRuns(1).setParamSpace(gainSearch).fit(data) }
    val customGain = learner
    customGain.set(customGain.labelGain, Array(0.0, 4.0))
    intercept[IllegalArgumentException] { tuner(customGain).fit(data) }
    val passThrough = learner
    passThrough.set(passThrough.passThroughArgs, "label_gain=0,4")
    intercept[IllegalArgumentException] { tuner(passThrough).fit(data) }
    val passSearch = new GridSpace(Array(ParamMap(first.passThroughArgs -> "label_gain=0,4")))
    intercept[IllegalArgumentException] { tuner(first).setNumRuns(1).setParamSpace(passSearch).fit(data) }
    val badSpace = new GridSpace(Array(ParamMap(first.groupCol -> "query")))
    intercept[IllegalArgumentException] { tuner(first).setNumRuns(1).setParamSpace(badSpace).fit(data) }
    Seq("rmse", "ndcg@0", "ndcg@-1", "ndcg@2147483648").foreach { metric =>
      intercept[IllegalArgumentException] { RankingTuning.cutoff(metric) }
    }
    assert(RankingTuning.cutoff("ndcg").isEmpty)
    assert(RankingTuning.cutoff("ndcg@5").contains(5))
  }
  for (args <- Seq("\"label_gain\"=0,4", "'label_gain'=0,4", "=label_gain=0,4", "label_gain=");
       sampled <- Seq(false, true)) {
    test(s"ranker tuning rejects pass-through gains '$args' with sampled=$sampled") {
      val model = learner
      val tuning = tuner(model).setNumRuns(1)
      if (sampled) {
        tuning.setParamSpace(new GridSpace(Array(ParamMap(model.passThroughArgs -> args))))
      } else {
        model.set(model.passThroughArgs, args)
      }
      val error = intercept[IllegalArgumentException] { tuning.fit(data) }
      assert(error.getMessage.contains("custom label_gain"))
    }
  }

  test("ranker tuning accepts unrelated pass-through keys and quoted values") {
    val model = learner
    model.set(model.passThroughArgs,
      "max_bin=63 label_gains=1 xlabel_gain=1 metric=\"label_gain\" objective=lambdarank")
    val fitted = tuner(model).setNumRuns(1).fit(data)
    assert(fitted.getBestMetric >= 0.0 && fitted.getBestMetric <= 1.0)
  }

  test("parallel failure waits for sibling training cleanup without driver interruption") {
    val model = learner
    TestableTuningRanker.TrainingInterrupted.set(false)
    val before = spark.sparkContext.getPersistentRDDs.keySet
    val broken = tuner(model).setNumRuns(2).setParallelism(2)
      .setParamSpace(new GridSpace(Array(ParamMap(model.direction -> 1.0, model.trainDelayMs -> 30000L),
        ParamMap(model.direction -> Double.NaN))))
    val error = intercept[IllegalArgumentException] { broken.fit(data) }
    assert(error.getMessage.contains("finite numbers"))
    assert(!TestableTuningRanker.TrainingInterrupted.get())
    assert(spark.sparkContext.getPersistentRDDs.keySet == before)
  }

  test("ranker evaluation failures expose the cause and release cached data") {
    val model = learner
    val before = spark.sparkContext.getPersistentRDDs.keySet
    val broken = tuner(model).setNumRuns(2).setParallelism(2)
      .setParamSpace(new GridSpace(Array(ParamMap(model.direction -> 1.0),
        ParamMap(model.direction -> Double.NaN))))
    val error = intercept[IllegalArgumentException] { broken.fit(data) }
    assert(error.getMessage.contains("finite numbers"))
    assert(spark.sparkContext.getPersistentRDDs.keySet == before)
  }

}
