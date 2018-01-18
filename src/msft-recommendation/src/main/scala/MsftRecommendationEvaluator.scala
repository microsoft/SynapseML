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

import scala.collection.mutable.ListBuffer

@InternalWrapper
final class MsftRecommendationEvaluator(override val uid: String)
  extends Evaluator with MsftRecEvaluatorParams {

  def this() = this(Identifiable.randomUID("recEval"))

  val metricName: Param[String] = {
    val allowedParams = ParamValidators.inArray(Array("ndcgAt", "map", "mapk", "recallAtK", "diversityAtK",
      "maxDiversity"))
    new Param(this, "metricName", "metric name in evaluation " +
      "(ndcgAt|map|mapk|recallAtK|diversityAtK|maxDiversity)", allowedParams)
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
    val predictionType = schema($(predictionCol)).dataType
    val labelType = schema($(labelCol)).dataType

    val predictionAndLabels = dataset
      .select(col($(predictionCol)).cast(predictionType), col($(labelCol)).cast(labelType)).rdd
      .map { case Row(prediction: Seq[Any], label: Seq[Any]) => (prediction.toArray, label.toArray) }

    val metrics = new RankingMetrics[Any](predictionAndLabels)
    val metric = $(metricName) match {
      case "map" => metrics.meanAveragePrecision
      case "ndcgAt" => metrics.ndcgAt($(k))
      case "mapk" => metrics.precisionAt($(k))
      case "recallAtK" => predictionAndLabels.map(r => r._1.toSet.intersect(r._2.toSet).size / r._1.size).mean()
      case "diversityAtK" => {
        val uniqueItemsRecommended = predictionAndLabels.map(row => row._1)
          .reduce((x, y) => x.toSet.union(y.toSet).toArray)
        val uniqueItemsSeen = predictionAndLabels.map(row => row._2)
          .reduce((x, y) => x.toSet.union(y.toSet).toArray)
        val nItems = uniqueItemsSeen.union(uniqueItemsRecommended).length
        uniqueItemsRecommended.length.toDouble / nItems.toDouble
      }
      case "maxDiversity" => {
        val uniqueItemsRecommended = predictionAndLabels.map(row => row._1)
          .reduce((x, y) => x.toSet.union(y.toSet).toArray)
        val uniqueItemsSeen = predictionAndLabels.map(row => row._2)
          .reduce((x, y) => x.toSet.union(y.toSet).toArray)
        val nItems = uniqueItemsSeen.union(uniqueItemsRecommended).length
        uniqueItemsSeen.length.toDouble / nItems.toDouble
      }
    }
    metric
  }

  override def copy(extra: ParamMap): MsftRecommendationEvaluator = defaultCopy(extra)

}
