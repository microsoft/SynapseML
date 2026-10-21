// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.automl

import org.apache.spark.ml.{Estimator, Ranker, RankerModel}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType}

import scala.annotation.tailrec

import java.util.UUID
import java.util.concurrent.{Callable, ConcurrentLinkedQueue, ExecutionException, ExecutorCompletionService,
  ExecutorService, Executors, TimeUnit}

/** The ranking-only route; shared classification/regression evaluation remains unchanged. */
private[automl] object RankingTuning {
  private val StructuralParams = Set("groupCol", "labelCol", "featuresCol", "predictionCol", "labelGain")
  private val MaxRelevance = 30

  private[automl] final class WorkerFailure(cause: Throwable) extends RuntimeException(cause)

  private def field(name: String): Column = col("`" + name.replace("`", "``") + "`")

  def cutoff(metric: String): Option[Int] = {
    val message = "Ranking evaluationMetric must be ndcg or ndcg@k with a positive integer k"
    if (metric == "ndcg") None else {
      require(metric.matches("ndcg@[1-9][0-9]*"), message)
      val parsed = scala.util.Try(metric.substring("ndcg@".length).toInt).toOption
      require(parsed.isDefined, message)
      parsed
    }
  }

  def folds(data: DataFrame, group: String, count: Int, seed: Long, foldCol: String): DataFrame = {
    require(count >= 2, "Ranking tuning requires numFolds >= 2")
    require(Set[DataType](StringType, IntegerType, LongType).contains(data.schema(group).dataType),
      "Ranking groupCol must have string, int or long type")
    require(data.filter(field(group).isNull).limit(1).count() == 0, "Ranking groupCol cannot contain null")
    val groups = data.select(field(group)).distinct()
    require(groups.limit(count).count() >= count, "Ranking tuning requires at least numFolds distinct groups")
    // Only distinct query groups enter this global window; document its single-partition scalability bound.
    val order = Window.orderBy(xxhash64(field(group), lit(seed)), field(group))
    val assignments = groups.withColumn(foldCol, pmod(row_number().over(order) - 1, lit(count)))
    data.join(assignments, Seq(group))
  }

  private def validateLabels(data: DataFrame, label: String): Unit = {
    val value = field(label).cast("double")
    val invalid = value.isNull || isnan(value) || (value < 0) || (value > MaxRelevance) || (value =!= floor(value))
    require(data.filter(invalid).limit(1).count() == 0, "Ranking labels must be integers in [0, 30]")
  }

  def ndcg(data: DataFrame, group: String, label: String, prediction: String,
           k: Option[Int], checkLabels: Boolean = true): Double = {
    ndcg(data, group, label, prediction, k, checkLabels, frame => { frame.unpersist(); () })
  }

  private def ndcg(data: DataFrame, group: String, label: String, prediction: String,
                   k: Option[Int], checkLabels: Boolean, release: DataFrame => Unit): Double = {
    val scored = data.select(field(group).as("query"), field(label).cast("double").as("relevance"),
      field(prediction).cast("double").as("score"))
    try {
      scored.cache()
      if (checkLabels) validateLabels(scored, "relevance")
      val badScore = col("score").isNull || isnan(col("score")) ||
        (abs(col("score")) === lit(Double.PositiveInfinity))
      require(scored.filter(badScore).limit(1).count() == 0, "Ranking predictions must be finite numbers")
      val actual = Window.partitionBy("query").orderBy(col("score").desc, col("relevance").asc)
      val ideal = Window.partitionBy("query").orderBy(col("relevance").desc)
      val ranked = scored.withColumn("actualRank", row_number().over(actual))
        .withColumn("idealRank", row_number().over(ideal))
      def gain(rank: String): Column = {
        val value = (pow(lit(2.0), col("relevance")) - 1.0) / log2(col(rank) + 1.0)
        k.map(limit => when(col(rank) <= limit, value).otherwise(0.0)).getOrElse(value)
      }
      val queryScores = ranked.groupBy("query").agg(sum(gain("actualRank")).as("dcg"),
        sum(gain("idealRank")).as("idcg"))
      val result = queryScores.select(avg(when(col("idcg") === 0.0, 1.0)
        .otherwise(col("dcg") / col("idcg"))).as("metric")).first()
      require(!result.isNullAt(0), "Ranking validation fold cannot be empty")
      result.getDouble(0)
    } finally {
      release(scored)
    }
  }

  private def isLabelGainArgument(token: String): Boolean = {
    def isQuote(char: Char): Boolean = char == '\'' || char == '"'
    // Match LightGBM's key normalization, conservatively rejecting malformed gain assignments too.
    token.split("=", -1).filter(_.nonEmpty).headOption.exists { key =>
      key.dropWhile(isQuote).reverse.dropWhile(isQuote).reverse == "label_gain"
    }
  }

  private def rejectCustomGains(ranker: Ranker[_, _, _]): Unit = {
    if (ranker.hasParam("labelGain")) {
      val param = ranker.getParam("labelGain").asInstanceOf[Param[Array[Double]]]
      require(ranker.get(param).orElse(ranker.getDefault(param)).forall(_.isEmpty),
        "custom labelGain is not mirrored by the tuning metric")
    }
    if (ranker.hasParam("passThroughArgs")) {
      val param = ranker.getParam("passThroughArgs").asInstanceOf[Param[String]]
      require(!ranker.get(param).exists(_.split("\\s+").exists(isLabelGainArgument)),
        "custom label_gain is not mirrored by the tuning metric")
    }
  }

  private case class Plan(runs: Seq[Ranker[_, _, _]], group: String, label: String, cutoff: Option[Int])

  private def prepare(tuner: TuneHyperparameters): Plan = {
    val models = tuner.getModels
    require(models.forall(_.isInstanceOf[Ranker[_, _, _]]), "Rankers cannot be mixed with non-ranking estimators")
    require(tuner.getNumRuns > 0, "Ranking tuning requires numRuns > 0")
    require(tuner.getParallelism > 0, "Ranking tuning requires parallelism > 0")
    val k = cutoff(tuner.getEvaluationMetric)
    val iterator = tuner.getParamSpace.paramMaps
    val runs = (0 until tuner.getNumRuns).map { index =>
      require(iterator.hasNext, "paramSpace has fewer entries than numRuns")
      val params = iterator.next()
      require(!params.toSeq.exists(p => StructuralParams.contains(p.param.name)),
        "Ranking paramSpace cannot vary groupCol, labelCol, featuresCol, predictionCol or labelGain")
      val ranker = models(index % models.length).copy(params).asInstanceOf[Ranker[_, _, _]]
      rejectCustomGains(ranker)
      ranker
    }
    val group = runs.head.getGroupCol
    val label = runs.head.getLabelCol
    val candidates = models.map(_.asInstanceOf[Ranker[_, _, _]]).toSeq ++ runs
    require(candidates.forall(ranker => ranker.getGroupCol == group && ranker.getLabelCol == label),
      "All ranking runs must use the same effective groupCol and labelCol")
    Plan(runs, group, label, k)
  }

  private def fitModel(ranker: Ranker[_, _, _], data: Dataset[_]): RankerModel[_, _] = {
    val fresh = ranker.copy(ParamMap.empty).asInstanceOf[Estimator[_]]
    // A previously fitted estimator may carry bin boundaries from unrelated training rows.
    if (fresh.hasParam("referenceDataset")) fresh.clear(fresh.getParam("referenceDataset"))
    fresh.fit(data).asInstanceOf[RankerModel[_, _]]
  }

  private def evaluateFold(plan: Plan, training: DataFrame, validation: DataFrame,
                           pool: ExecutorService, jobPrefix: String,
                           pending: ConcurrentLinkedQueue[DataFrame]): Seq[Double] = {
    val jobGroups = plan.runs.indices.map(index => s"$jobPrefix-$index")
    val context = training.sparkSession.sparkContext
    val completed = new ExecutorCompletionService[(Int, Double)](pool)
    val futures = plan.runs.zipWithIndex.map { case (ranker, index) =>
      completed.submit(new Callable[(Int, Double)] {
        override def call(): (Int, Double) = {
          context.setJobGroup(jobGroups(index), "Ranking hyperparameter evaluation", interruptOnCancel = true)
          try {
            val model = fitModel(ranker, training)
            val score = ndcg(model.transform(validation), plan.group, plan.label, model.getPredictionCol,
              plan.cutoff, checkLabels = false, frame => { pending.add(frame); () })
            (index, score)
          } finally {
            context.clearJobGroup()
          }
        }
      })
    }
    try {
      val scores = Array.fill(plan.runs.length)(0.0)
      plan.runs.indices.foreach { _ =>
        val (index, score) = try {
          completed.take().get()
        } catch {
          case wrapped: ExecutionException if wrapped.getCause != null => throw new WorkerFailure(wrapped.getCause)
        }
        scores(index) = score
        releaseCompleted(pending)
      }
      scores.toSeq
    } catch {
      case error: Exception =>
        jobGroups.foreach(context.cancelJobGroup)
        // Estimators must handle interruption in their own cleanup; tuner caches belong to the caller.
        futures.foreach(_.cancel(true))
        throw error
    }
  }

  private def releaseCompleted(pending: ConcurrentLinkedQueue[DataFrame]): Unit = {
    Iterator.continually(Option(pending.poll())).takeWhile(_.nonEmpty).flatten.foreach(_.unpersist())
  }

  @tailrec
  private def awaitWorkers(pool: ExecutorService, waitMillis: Long,
                           interrupted: Option[InterruptedException] = None): Option[InterruptedException] = {
    val result = try {
      Right(pool.awaitTermination(waitMillis, TimeUnit.MILLISECONDS))
    } catch {
      case error: InterruptedException => Left(error)
    }
    result match {
      case Right(true) => interrupted
      case Right(false) => awaitWorkers(pool, waitMillis, interrupted)
      case Left(error) => awaitWorkers(pool, waitMillis, interrupted.orElse(Some(error)))
    }
  }

  private def preserveFailure(failure: Option[Throwable])(cleanup: => Unit): Unit = {
    try {
      cleanup
    } catch {
      case error: Throwable =>
        if (error.isInstanceOf[InterruptedException]) Thread.currentThread().interrupt()
        failure match {
          case Some(original) => if (error ne original) original.addSuppressed(error)
          case None => throw error
        }
    }
  }

  // A cancelled Future can be done before its worker's finally block has finished.
  def withWorkerCleanup[T](pool: ExecutorService, waitMillis: Long = TimeUnit.MINUTES.toMillis(1))
                          (body: => T)(cleanup: => Unit): T = {
    var failure = Option.empty[Throwable]
    try {
      body
    } catch {
      case worker: WorkerFailure =>
        failure = Some(worker.getCause)
        throw worker.getCause
      case error: Throwable =>
        if (error.isInstanceOf[InterruptedException]) Thread.currentThread().interrupt()
        failure = Some(error)
        throw error
    } finally {
      var interrupted = Thread.interrupted()
      pool.shutdown()
      val joinInterrupt = awaitWorkers(pool, waitMillis)
      interrupted = Thread.interrupted() || interrupted || joinInterrupt.nonEmpty
      try {
        preserveFailure(failure) {
          cleanup
          if (failure.isEmpty) joinInterrupt.foreach(error => throw error)
        }
      } finally {
        interrupted = Thread.interrupted() || interrupted
        if (interrupted) Thread.currentThread().interrupt()
      }
    }
  }

  def fit(tuner: TuneHyperparameters, dataset: Dataset[_]): TuneHyperparametersModel = {
    val plan = prepare(tuner)
    validateLabels(dataset.toDF(), plan.label)
    val foldCol = "rankingFold_" + UUID.randomUUID().toString.replace("-", "")
    val assigned = folds(dataset.toDF(), plan.group, tuner.getNumFolds, tuner.getSeed, foldCol).cache()
    val pool = Executors.newFixedThreadPool(math.min(tuner.getParallelism, plan.runs.length))
    val pending = new ConcurrentLinkedQueue[DataFrame]()
    withWorkerCleanup(pool) {
      val totals = Array.fill(plan.runs.length)(0.0)
      for (fold <- 0 until tuner.getNumFolds) {
        val training = assigned.filter(field(foldCol) =!= fold).drop(foldCol)
        val validation = assigned.filter(field(foldCol) === fold).drop(foldCol)
        val scores = evaluateFold(plan, training, validation, pool, s"${tuner.uid}-$foldCol-$fold", pending)
        scores.zipWithIndex.foreach { case (score, index) => totals(index) += score }
      }
      val best = totals.indices.maxBy(totals(_))
      val model = fitModel(plan.runs(best), dataset)
      new TuneHyperparametersModel(tuner.uid).setBestModel(model).setBestMetric(totals(best) / tuner.getNumFolds)
    } {
      try {
        releaseCompleted(pending)
      } finally {
        assigned.unpersist()
      }
    }
  }
}
