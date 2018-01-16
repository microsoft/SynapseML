// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import scala.reflect.ClassTag

import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.{IntParam, Param, ParamMap, ParamValidators}
import org.apache.spark.ml.recommendation.MsftRecEvaluatorParams
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.evaluation.RankingMetrics
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row}

@InternalWrapper
final class MsftRecommendationEvaluator[T: ClassTag](override val uid: String)
  extends Evaluator with MsftRecEvaluatorParams {

  def this() = this(Identifiable.randomUID("recEval"))

  val metricName: Param[String] = {
    val allowedParams = ParamValidators.inArray(Array("ndcgAt", "precisionAt"))
    new Param(this, "metricName", "metric name in evaluation (ndcgAt|precisionAt)", allowedParams)
  }

  val k: IntParam = new IntParam(this, "k",
    "number of items", ParamValidators.inRange(1, Integer.MAX_VALUE))

  /** @group getParam */
  def getK: Int = $(k)

  /** @group setParam */
  def setK(value: Int): this.type = set(k, value)

  setDefault(k -> 1)

  /** @group getParam */
  def getMetricName: String = $(metricName)

  /** @group setParam */
  def setMetricName(value: String): this.type = set(metricName, value)

  /** @group setParam */
  def setLabelCol(value: String): this.type = set(labelCol, value)

  setDefault(metricName -> "ndcgAt")

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  override def evaluate(dataset: Dataset[_]): Double = {
    val schema = dataset.schema
    val predictionColName = $(predictionCol)
    val predictionType = schema($(predictionCol)).dataType
    val labelColName = $(labelCol)
    val labelType = schema($(labelCol)).dataType

    val predictionAndLabels = dataset
      .select(col($(predictionCol)).cast(predictionType), col($(labelCol)).cast(labelType))
      .rdd.
      map { case Row(prediction: Seq[T], label: Seq[T]) => (prediction.toArray, label.toArray) }

    val metrics = new RankingMetrics[T](predictionAndLabels)
    val metric = $(metricName) match {
      case "map" => metrics.meanAveragePrecision
      case "ndcgAt" => metrics.ndcgAt($(k))
      case "mapk" => metrics.precisionAt($(k))
    }
    metric
  }
//  override def evaluate(dataset: Dataset[_]): Double = {
//    dataset.cache()
//
//    //    val nItems = test_ratings.map(lambda r: r[1]).distinct().count()
//
//    //    val recallAtk = dataset.rdd.map(
//    //      lambda x: len(set(x[0]).intersection(set(x[1]))) / len(x[1])).mean()
//
//    //    val uniqueItemsRecommended = dataset.rdd.map(lambda row: row[0]) \
//    //      .reduce(lambda x, y: set(x).union(set(y)))
//
//    //    val diversityAtk = len(uniqueItemsRecommended) / nItems
//
//    val metrics = new RankingMetrics(dataset.rdd.asInstanceOf[RDD[(Array[AnyRef], Array[AnyRef])]])
//    dataset.unpersist()
//
//    //    metric = {'mean_avg_precision': metrics.meanAveragePrecision,
//    //      'precision_at_k': metrics.precisionAt(k),
//    //      'recall_at_k': recallAtk,
//    //      'ndcg_at_k': metrics.ndcgAt(k),
//    //      'diversity_at_k': diversityAtk,
//    //      'max_diverstiy_at_k': maxDiversityAtk,
//    //      "uniqueItemsRecommended": len(uniqueItemsRecommended),
//    //      "nItems": nItems}
//
//    val metric = $(metricName) match {
//      case "ndcgAt" => metrics.ndcgAt($(k))
//      case "precisionAt" => metrics.precisionAt($(k))
//    }
//
//    metric
//  }

  override def copy(extra: ParamMap): MsftRecommendationEvaluator[T] = defaultCopy(extra)

}
