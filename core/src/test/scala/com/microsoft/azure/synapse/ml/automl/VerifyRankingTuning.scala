// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.automl

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.param.ByteArrayParam
import org.apache.spark.ml.{Estimator, Ranker, RankerModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.{BooleanParam, DoubleArrayParam, DoubleParam, LongParam, Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._

import java.util.concurrent.{CountDownLatch, ExecutionException, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

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
  val interruptTraining = new BooleanParam(this, "interruptTraining", "Test worker-only interruption")
  val blockTraining = new BooleanParam(this, "blockTraining", "Test interrupted worker cleanup")
  setDefault(interruptTraining -> false, blockTraining -> false)
  override protected def train(dataset: Dataset[_]): TestableTuningRankerModel = {
    TestableTuningRanker.TrainingCalls.incrementAndGet()
    require($(referenceDataset).isEmpty, "stale reference dataset reached fit")
    if ($(interruptTraining)) throw TestableTuningRanker.WorkerInterruption.get().get
    if ($(blockTraining)) {
      val block = TestableTuningRanker.Block.get().get
      block.started.countDown()
      try {
        new CountDownLatch(1).await()
      } finally {
        block.finishing.countDown()
        block.release.await()
      }
    }
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
  val TrainingCalls = new AtomicInteger()
  val TrainingInterrupted = new AtomicBoolean(false)
  val WorkerInterruption = new AtomicReference[Option[InterruptedException]](None)
  case class TrainingBlock(started: CountDownLatch, finishing: CountDownLatch, release: CountDownLatch)
  val Block = new AtomicReference[Option[TrainingBlock]](None)
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

  for (runs <- Seq(1, 2)) {
    test(s"ranker tuning rejects inconsistent prediction columns before training with numRuns=$runs") {
      val first = learner.setPredictionCol("firstPrediction")
      first.set(first.direction, -1.0)
      val second = learner.setPredictionCol("secondPrediction")
      val tuning = tuner(first).setModels(Array[Estimator[_]](first, second)).setNumRuns(runs)
        .setParamSpace(new GridSpace(Array.fill(runs)(ParamMap.empty)))
      val before = TestableTuningRanker.TrainingCalls.get()
      val error = intercept[IllegalArgumentException] { tuning.fit(data) }
      assert(error.getMessage.contains("same effective predictionCol"))
      assert(TestableTuningRanker.TrainingCalls.get() == before)
    }
  }

  test("ranker tuning preserves its advertised prediction column when the second candidate wins") {
    val first = learner.setPredictionCol("rankingScore")
    first.set(first.direction, -1.0)
    val second = learner.setPredictionCol("rankingScore")
    val tuning = tuner(first).setModels(Array[Estimator[_]](first, second))
      .setParamSpace(new GridSpace(Array(ParamMap.empty, ParamMap.empty)))
    val frame = data
    val fitted = tuning.fit(frame)
    assert(fitted.getBestModel.uid == second.uid)
    assert(math.abs(fitted.getBestMetric - 1.0) < 1e-12)
    assert(fitted.transformSchema(frame.schema).map(f => (f.name, f.dataType, f.nullable)) ==
      tuning.transformSchema(frame.schema).map(f => (f.name, f.dataType, f.nullable)))
    assert(fitted.transform(frame).select("rankingScore", "label").collect()
      .forall(row => row.getDouble(0) == row.getDouble(1)))
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

  test("parallel failure interrupts sibling training before releasing cached data") {
    val model = learner
    TestableTuningRanker.TrainingInterrupted.set(false)
    val before = spark.sparkContext.getPersistentRDDs.keySet
    val broken = tuner(model).setNumRuns(2).setParallelism(2)
      .setParamSpace(new GridSpace(Array(ParamMap(model.direction -> 1.0, model.trainDelayMs -> 30000L),
        ParamMap(model.direction -> Double.NaN))))
    val error = intercept[IllegalArgumentException] { broken.fit(data) }
    assert(error.getMessage.contains("finite numbers"))
    assert(TestableTuningRanker.TrainingInterrupted.get())
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

  test("worker interruption preserves the cause without interrupting the tuner caller") {
    assert(!Thread.currentThread().isInterrupted)
    val original = new InterruptedException("worker-only ranking interruption")
    TestableTuningRanker.WorkerInterruption.set(Some(original))
    val model = learner
    val before = spark.sparkContext.getPersistentRDDs.keySet
    val broken = tuner(model).setNumRuns(1)
      .setParamSpace(new GridSpace(Array(ParamMap(model.interruptTraining -> true))))
    try {
      val observed = intercept[InterruptedException] { broken.fit(data) }
      assert(observed eq original)
      assert(!Thread.currentThread().isInterrupted)
      assert(new CountDownLatch(0).await(1, TimeUnit.SECONDS))
      assert(spark.sparkContext.getPersistentRDDs.keySet == before)
    } finally {
      Thread.interrupted()
      TestableTuningRanker.WorkerInterruption.set(None)
    }
  }

  test("tuner caller interruption waits for worker cleanup and restores only the caller flag") {
    val block = TestableTuningRanker.TrainingBlock(new CountDownLatch(1), new CountDownLatch(1),
      new CountDownLatch(1))
    TestableTuningRanker.Block.set(Some(block))
    val frame = data
    val before = spark.sparkContext.getPersistentRDDs.keySet
    val model = learner
    val broken = tuner(model).setNumRuns(1)
      .setParamSpace(new GridSpace(Array(ParamMap(model.blockTraining -> true))))
    val observed = new AtomicReference[Throwable]()
    val restored = new AtomicBoolean(false)
    val returned = new CountDownLatch(1)
    val caller = new Thread(new Runnable {
      override def run(): Unit = {
        try {
          broken.fit(frame)
        } catch {
          case error: Throwable => observed.set(error)
        } finally {
          restored.set(Thread.currentThread().isInterrupted)
          returned.countDown()
        }
      }
    })
    try {
      caller.start()
      assert(block.started.await(30, TimeUnit.SECONDS))
      caller.interrupt()
      assert(block.finishing.await(10, TimeUnit.SECONDS))
      assert(!returned.await(200, TimeUnit.MILLISECONDS))
    } finally {
      caller.interrupt()
      block.release.countDown()
      caller.join(30000L)
      TestableTuningRanker.Block.set(None)
    }
    assert(!caller.isAlive)
    assert(observed.get().isInstanceOf[InterruptedException])
    assert(restored.get())
    assert(spark.sparkContext.getPersistentRDDs.keySet == before)
  }

  for (kind <- Seq("worker", "caller", "execution")) {
    test(s"worker cleanup preserves interruption provenance for $kind failure") {
      assert(!Thread.currentThread().isInterrupted)
      val pool = Executors.newSingleThreadExecutor()
      val cause = new InterruptedException("original interruption")
      val original = if (kind == "execution") new ExecutionException(cause) else cause
      val thrown = if (kind == "worker") new RankingTuning.WorkerFailure(cause) else original
      val cleaned = new AtomicBoolean(false)
      try {
        val observed = intercept[Throwable] {
          RankingTuning.withWorkerCleanup(pool) {
            throw thrown
          } {
            assert(!Thread.currentThread().isInterrupted)
            cleaned.set(true)
          }
        }
        assert(observed eq original)
        assert(Thread.currentThread().isInterrupted == (kind == "caller"))
        assert(cleaned.get())
        assert(pool.isTerminated)
      } finally {
        Thread.interrupted()
        pool.shutdownNow()
      }
    }
  }

  for (interruptCaller <- Seq(false, true)) {
    test(s"worker cleanup waits beyond the timeout with caller interruption=$interruptCaller") {
      val pool = Executors.newSingleThreadExecutor()
      val started = new CountDownLatch(1)
      val finishing = new CountDownLatch(1)
      val release = new CountDownLatch(1)
      val returned = new CountDownLatch(1)
      val cleaned = new AtomicBoolean(false)
      val restored = new AtomicBoolean(false)
      val observed = new AtomicReference[Throwable]()
      val original = new IllegalArgumentException("original ranking failure")
      val worker = pool.submit(new Runnable {
        override def run(): Unit = {
          try {
            started.countDown()
            new CountDownLatch(1).await()
          } catch {
            case _: InterruptedException => ()
          } finally {
            finishing.countDown()
            release.await()
          }
        }
      })
      val caller = new Thread(new Runnable {
        override def run(): Unit = {
          try {
            RankingTuning.withWorkerCleanup(pool, 10L) {
              worker.cancel(true)
              throw original
            } {
              assert(!Thread.currentThread().isInterrupted)
              cleaned.set(true)
            }
          } catch {
            case error: Throwable => observed.set(error)
          } finally {
            restored.set(Thread.currentThread().isInterrupted)
            returned.countDown()
          }
        }
      })
      try {
        assert(started.await(10, TimeUnit.SECONDS))
        caller.start()
        assert(finishing.await(10, TimeUnit.SECONDS))
        if (interruptCaller) caller.interrupt()
        // Many 10ms termination intervals expire while the worker's finally remains blocked.
        assert(!returned.await(200, TimeUnit.MILLISECONDS))
        assert(!cleaned.get())
      } finally {
        release.countDown()
        pool.shutdownNow()
        caller.join(10000L)
      }
      assert(!caller.isAlive)
      assert(cleaned.get())
      assert(observed.get() eq original)
      assert(restored.get() == interruptCaller)
      assert(pool.isTerminated)
    }
  }

}
